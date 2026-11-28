"""T07: đối chiếu chỉ số với kết quả tính tay và chia thời gian không rò rỉ dữ liệu."""

from __future__ import annotations

import math
from datetime import UTC, datetime

import numpy as np
import pytest

from recommender.evaluation.metrics import (
    catalog_coverage,
    evaluate_lists,
    intra_list_diversity,
    ndcg_at_k,
    recall_at_k,
)
from recommender.evaluation.split import SplitConfig, split_events

TOL = 1e-9


def test_hand_computed_fixture_from_experiment_plan() -> None:
    relevant = {"p2", "p4"}
    ranked = ["p2", "p3", "p4"]
    assert recall_at_k(ranked, relevant) == pytest.approx(1.0, abs=TOL)
    assert ndcg_at_k(ranked, relevant) == pytest.approx(0.919720789, abs=TOL)
    expected = 1.5 / (1 + 1 / math.log2(3))
    assert ndcg_at_k(ranked, relevant) == pytest.approx(expected, abs=TOL)
    assert catalog_coverage([ranked], 5) == pytest.approx(0.6, abs=TOL)
    vectors = {
        "p2": np.array([1.0, 0.0]),
        "p3": np.array([0.0, 1.0]),
        "p4": np.array([1.0, 0.0]),
    }
    ild, pairs = intra_list_diversity(ranked, vectors)
    assert pairs == 3 and ild == pytest.approx(2 / 3, abs=TOL)


def test_edge_cases() -> None:
    assert recall_at_k([], {"p1"}) == 0.0
    assert ndcg_at_k([], {"p1"}) == 0.0
    assert recall_at_k(["p9"], {"p1"}) == 0.0
    # Hơn 10 nhãn: mẫu số vẫn tính tất cả; IDCG giới hạn ở 10.
    many = {f"p{i}" for i in range(20)}
    top = [f"p{i}" for i in range(10)]
    assert recall_at_k(top, many) == pytest.approx(0.5, abs=TOL)
    assert ndcg_at_k(top, many) == pytest.approx(1.0, abs=TOL)
    # Bỏ qua vị trí lớn hơn k.
    assert recall_at_k(["x"] * 10 + ["p1"], {"p1"}) == 0.0
    # ILD không xác định khi có <2 vector hợp lệ; vector thiếu không được coi là đa dạng.
    assert intra_list_diversity(["p1"], {"p1": np.array([1.0])}) == (None, 0)
    assert intra_list_diversity(["p1", "p2"], {"p1": np.array([1.0]), "p2": None}) == (
        None,
        0,
    )
    assert catalog_coverage([], 0) is None
    with pytest.raises(ValueError):
        recall_at_k(["p1"], set())


def test_evaluate_lists_macro_average_and_user_set() -> None:
    relevance = {"u1": {"p2", "p4"}, "u2": {"p1"}, "u3": set()}  # loại u3
    recs = {"u1": ["p2", "p3", "p4"]}  # u2 không có danh sách -> điểm 0
    vectors = {p: np.eye(4)[i] for i, p in enumerate(["p1", "p2", "p3", "p4"])}
    out = evaluate_lists(recs, relevance, vectors, catalog_size=5)
    assert out["users"] == 2 and out["labels"] == 3
    assert out["recall_at_10"] == pytest.approx(0.5, abs=TOL)
    assert out["ndcg_at_10"] == pytest.approx(0.919720789 / 2, abs=TOL)
    assert out["ild"] == pytest.approx(1.0, abs=TOL) and out["ild_users"] == 1
    assert out["coverage"] == pytest.approx(3 / 5, abs=TOL)
    empty = evaluate_lists({}, {"u": set()}, vectors, catalog_size=5)
    assert empty["recall_at_10"] is None and empty["coverage"] is None


def _ev(n: int, at: str, **kw: object) -> tuple:
    base = {
        "event_id": f"e{n:03d}",
        "user_id": "u1",
        "session_id": "s1",
        "product_id": "p:1",
        "event_type": "view",
        "event_at": datetime.fromisoformat(at),
        "source": "synthetic",
        "cart_id": None,
    }
    base.update(kw)
    return tuple(base.values())


@pytest.mark.spark
def test_split_boundaries_and_cart_purge(spark) -> None:
    from recommender.pipeline.etl import silver_event_schema

    rows = [
        _ev(1, "2026-10-01T00:00:00Z"),  # bao gồm thời điểm bắt đầu -> tập huấn luyện
        _ev(2, "2026-10-24T23:59:59Z"),  # tập huấn luyện
        _ev(3, "2026-10-25T00:00:00Z"),  # đúng mốc chia -> tập kiểm định
        _ev(4, "2026-11-01T00:00:00Z"),  # đúng mốc test -> tập kiểm tra
        _ev(5, "2026-11-08T00:00:00Z"),  # đúng cận cuối -> ngoài cửa sổ
        _ev(6, "2026-09-30T23:59:59Z"),  # trước thời điểm bắt đầu -> ngoài cửa sổ
        # giỏ trải qua mốc validation -> loại toàn bộ
        _ev(7, "2026-10-24T23:50:00Z", event_type="add_to_cart", cart_id="c_cross"),
        _ev(8, "2026-10-25T00:10:00Z", event_type="add_to_cart", cart_id="c_cross"),
        # giỏ kết thúc đúng mốc test -> vượt mốc (first < cutoff <= last)
        _ev(9, "2026-10-31T23:00:00Z", event_type="add_to_cart", cart_id="c_edge"),
        _ev(10, "2026-11-01T00:00:00Z", event_type="add_to_cart", cart_id="c_edge"),
        # giỏ bắt đầu đúng mốc chia nằm trọn trong validation
        _ev(11, "2026-10-25T00:00:00Z", event_type="add_to_cart", cart_id="c_in"),
        _ev(12, "2026-10-25T00:05:00Z", event_type="add_to_cart", cart_id="c_in"),
    ]
    events = spark.createDataFrame(rows, silver_event_schema())
    result = split_events(events, SplitConfig())
    parts = {r["event_id"]: r["split"] for r in result.events.collect()}
    assert parts == {
        "e001": "train",
        "e002": "train",
        "e003": "validation",
        "e004": "test",
        "e011": "validation",
        "e012": "validation",
    }
    assert result.purged_carts == ["c_cross", "c_edge"]
    summary = result.summary
    assert summary["events_outside_window"] == 2
    assert summary["cart_purge"]["events"] == 4
    assert summary["cart_purge"]["label_events"] == 4
    assert all(summary["invariants"].values())
    assert summary["parts"]["validation"]["events"] == 3


def test_split_config_requires_ordered_cutoffs() -> None:
    with pytest.raises(ValueError):
        SplitConfig(split_val_time=datetime(2026, 11, 2, tzinfo=UTC))
