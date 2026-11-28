"""Đo thử T07: Popularity, TF-IDF, Implicit ALS và ALS + MMR.

Quy trình (EXPERIMENT_PLAN.md mục 3-7):

1. Chia sự kiện Silver theo hai mốc UTC và loại giỏ hàng trải qua các mốc đó.
2. Tập validation: chỉ học từ tập train (danh mục, từ vựng/IDF, popularity,
   các nhân tố ALS, luật và lịch sử đã xem tính đến ``split_val_time``);
   chọn cấu hình ALS, hệ số lambda của MMR và các ngưỡng FP-Growth.
3. Chốt cấu hình, huấn luyện lại trên train + validation (mốc
   ``split_test_time``), rồi đánh giá tập test đúng một lần.

Mọi phương pháp được đánh giá trên cùng tập người dùng ``U`` (có ít nhất
một nhãn liên quan), cùng chính sách ứng viên ``C_t minus H_u`` và cùng
cơ chế bổ sung bằng popularity. Kết quả lưu ở ``gold/<run_id>/evaluation/``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from recommender.config import get_settings
from recommender.evaluation.metrics import evaluate_lists, mean_or_none
from recommender.evaluation.split import SplitConfig, split_events
from recommender.pipeline.gold import (
    build_baskets,
    build_interactions,
    build_seen_products,
    catalog_as_of,
)
from recommender.pipeline.lake import (
    LakeStore,
    create_spark_session,
    lake_store,
    spark_environment,
    utc_now,
)
from recommender.ranking.als import (
    ALS_GRID,
    AlsArtifacts,
    AlsParams,
    als_candidates,
    fit_als,
)
from recommender.ranking.common import CANDIDATE_LIMIT, DEFAULT_K, Scored, with_fallback
from recommender.ranking.fpgrowth import (
    FPGROWTH_GRID,
    FpGrowthParams,
    Rule,
    collect_rules,
    fit_fpgrowth,
    recommend_complements,
)
from recommender.ranking.mmr import MMR_LAMBDAS, MmrStats, mmr_rerank
from recommender.ranking.popularity import rank_popular, recommend_popular
from recommender.ranking.tfidf import TfidfFeatures, fit_tfidf, recommend_tfidf
from recommender.schemas import EVENT_WEIGHTS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

METHODS = ("popularity", "tfidf", "als", "als_mmr")
COHORTS = ("all", "with_history", "new_user")
TIE_TOLERANCE = 1e-6
QUALITY_COLUMNS = (
    "split",
    "model",
    "config",
    "cohort",
    "status",
    "recall_at_10",
    "ndcg_at_10",
    "ild",
    "coverage",
    "users",
    "labels",
    "ild_users",
    "ild_pairs",
    "avg_list_length",
    "fallback_rate",
)


@dataclass(slots=True)
class Timer:
    """Đo thời gian từng giai đoạn bằng đồng hồ đơn điệu (EXPERIMENT_PLAN.md mục 9)."""

    rows: list[dict[str, Any]] = field(default_factory=list)

    def run(self, stage: str, fn: Callable[[], Any], **extra: Any) -> Any:
        start = time.perf_counter()
        status, error = "ok", ""
        try:
            return fn()
        except Exception as exc:  # ghi nhận lỗi rồi ném lại ngoại lệ
            status, error = "error", repr(exc)[:500]
            raise
        finally:
            self.rows.append(
                {
                    "stage": stage,
                    "seconds": round(time.perf_counter() - start, 3),
                    "status": status,
                    "error": error,
                    **extra,
                }
            )


@dataclass(slots=True)
class WindowData:
    """Toàn bộ thông tin mô hình được phép biết tại một mốc thời gian."""

    cutoff: datetime
    catalog: DataFrame
    catalog_meta: dict[str, dict[str, Any]]
    interactions_df: DataFrame
    interactions: list[tuple[str, str, float]]
    seen_df: DataFrame
    seen: dict[str, set[str]]
    baskets_df: DataFrame
    features: TfidfFeatures
    popular: list[tuple[str, float]]


def prepare_window(
    products: DataFrame,
    history: DataFrame,
    start: datetime,
    cutoff: datetime,
    run_id: str,
    timer: Timer,
    label: str,
) -> WindowData:
    """Danh mục tại mốc thời gian, dữ liệu tổng hợp và TF-IDF chỉ học từ dữ liệu được phép."""
    catalog = catalog_as_of(products, cutoff, run_id).cache()
    meta = {
        r["product_id"]: {
            "category_id": r["category_id"],
            "attributes": r["attributes"],
        }
        for r in catalog.select("product_id", "category_id", "attributes").collect()
    }
    interactions_df = timer.run(
        f"aggregate_{label}",
        lambda: build_interactions(history, catalog, run_id, start, cutoff).cache(),
    )
    interactions = [
        (r["user_id"], r["product_id"], float(r["interaction_weight"]))
        for r in interactions_df.collect()
    ]
    seen_df = build_seen_products(history, catalog, run_id, start, cutoff).cache()
    seen: dict[str, set[str]] = defaultdict(set)
    for r in seen_df.collect():
        seen[r["user_id"]].add(r["product_id"])
    baskets_df = build_baskets(history, catalog, run_id, start, cutoff).cache()
    _, features = timer.run(f"fit_tfidf_{label}", lambda: fit_tfidf(catalog))
    popular = timer.run(
        f"fit_popularity_{label}", lambda: rank_popular(interactions_df, catalog)
    )
    return WindowData(
        cutoff,
        catalog,
        meta,
        interactions_df,
        interactions,
        seen_df,
        dict(seen),
        baskets_df,
        features,
        popular,
    )


def relevance_labels(
    eval_events: DataFrame, window: WindowData
) -> tuple[dict[str, set[str]], dict[str, int]]:
    """``R_u``: sản phẩm click/add_to_cart thuộc ``C_t`` và chưa có trong ``H_u`` (mục 4)."""
    from pyspark.sql import functions as F

    rows = (
        eval_events.where(F.col("event_type").isin("click", "add_to_cart"))
        .select("user_id", "product_id")
        .distinct()
        .collect()
    )
    labels: dict[str, set[str]] = defaultdict(set)
    dropped = {"not_in_catalog": 0, "already_seen": 0}
    for r in rows:
        user, pid = r["user_id"], r["product_id"]
        if pid not in window.catalog_meta:
            dropped["not_in_catalog"] += 1
        elif pid in window.seen.get(user, ()):
            dropped["already_seen"] += 1
        else:
            labels[user].add(pid)
    users_any = {r["user_id"] for r in rows}
    dropped["users_without_labels"] = len(users_any - set(labels))
    return dict(labels), dropped


def _final_lists(
    users: Sequence[str],
    primary: Mapping[str, Sequence[Scored]],
    window: WindowData,
    k: int,
) -> tuple[dict[str, list[str]], dict[str, bool]]:
    lists, fallback = {}, {}
    for user in users:
        items = with_fallback(
            primary.get(user, ()),
            window.popular,
            seen=window.seen.get(user, set()),
            limit=k,
        )
        lists[user] = [i.product_id for i in items]
        fallback[user] = any(i.method == "popularity" for i in items)
    return lists, fallback


def method_lists(
    method: str,
    users: Sequence[str],
    window: WindowData,
    *,
    als: AlsArtifacts | None = None,
    als_cands: Mapping[str, Sequence[Scored]] | None = None,
    lam: float | None = None,
    k: int = DEFAULT_K,
    mmr_stats: MmrStats | None = None,
) -> tuple[dict[str, list[str]], dict[str, bool]]:
    """Danh sách top-k cho ``users`` theo một phương pháp, dùng chung cơ chế dự phòng."""
    if method == "popularity":
        lists = {
            u: [
                i.product_id
                for i in recommend_popular(window.popular, window.seen.get(u, set()), k)
            ]
            for u in users
        }
        return lists, {u: True for u in users}
    if method == "tfidf":
        primary = recommend_tfidf(
            window.features, window.interactions, window.seen, limit=k, users=users
        )
        return _final_lists(users, primary, window, k)
    if als_cands is None:
        raise ValueError("ALS methods need candidates")
    if method == "als":
        return _final_lists(
            users, {u: list(c)[:k] for u, c in als_cands.items()}, window, k
        )
    if method == "als_mmr":
        vectors = {
            pid: window.features.vector(pid) for pid in window.features.product_ids
        }
        wanted = set(users)
        primary = {
            u: mmr_rerank(c, vectors, float(lam), limit=k, stats=mmr_stats)
            for u, c in als_cands.items()
            if u in wanted
        }
        return _final_lists(users, primary, window, k)
    raise ValueError(f"Unknown method {method}")


def score_lists(
    split: str,
    model: str,
    config: str,
    lists: Mapping[str, list[str]],
    fallback: Mapping[str, bool],
    labels: Mapping[str, set[str]],
    window: WindowData,
) -> list[dict[str, Any]]:
    """Các dòng kết quả chất lượng cho ba nhóm người dùng."""
    vectors = {pid: window.features.vector(pid) for pid in window.features.product_ids}
    rows = []
    for cohort in COHORTS:
        if cohort == "all":
            subset = labels
        elif cohort == "with_history":
            subset = {u: r for u, r in labels.items() if u in window.seen}
        else:
            subset = {u: r for u, r in labels.items() if u not in window.seen}
        metrics = evaluate_lists(lists, subset, vectors, len(window.catalog_meta))
        users = [u for u, r in subset.items() if r]
        rows.append(
            {
                "split": split,
                "model": model,
                "config": config,
                "cohort": cohort,
                "status": "ok" if users else "not_evaluable",
                **metrics,
                "fallback_rate": mean_or_none(
                    [1.0 if fallback.get(u) else 0.0 for u in users]
                ),
            }
        )
    return rows


def holdout_carts(
    baskets: DataFrame, window: WindowData, seed: int
) -> list[tuple[str, list[str], str]]:
    """``(cart_id, sản phẩm truy vấn, sản phẩm giữ lại)`` cho giỏ có >= 2 sản phẩm trong danh mục."""
    out = []
    for r in baskets.select("cart_id", "product_ids").collect():
        items = sorted(p for p in r["product_ids"] if p in window.catalog_meta)
        if len(items) < 2:
            continue
        held = min(
            items,
            key=lambda p: hashlib.sha256(
                f"{seed}:{r['cart_id']}:{p}".encode()
            ).hexdigest(),
        )
        out.append((r["cart_id"], [p for p in items if p != held], held))
    return sorted(out)


def score_complements(
    split: str,
    params: FpGrowthParams,
    rules: list[Rule],
    carts: list[tuple[str, list[str], str]],
    window: WindowData,
    stats: dict[str, int],
) -> dict[str, Any]:
    """Hit/Recall@10 cho sản phẩm giữ lại của giỏ hàng (mỗi giỏ một mục tiêu)."""
    hits = predicted = 0
    confidences = []
    for _, query, held in carts:
        preds = recommend_complements(
            rules, query, limit=DEFAULT_K, catalog=window.catalog_meta
        )
        if preds:
            predicted += 1
        if any(pid == held for pid, _, _ in preds):
            hits += 1
            confidences.append(next(c for pid, c, _ in preds if pid == held))
    n = len(carts)
    return {
        "split": split,
        "config": json.dumps(params.as_dict(), sort_keys=True),
        "status": "ok" if n else "not_evaluable",
        "carts": n,
        "hit_at_10": hits / n if n else None,
        "carts_with_prediction": predicted / n if n else None,
        "mean_confidence_of_hits": mean_or_none(confidences),
        "rules": len(rules),
        "baskets_fit": stats["baskets_fit"],
        "baskets_single_item": stats["baskets_single_item"],
    }


def _better(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Thứ tự chọn: NDCG, rồi Recall (sai số 1e-6), rồi thời gian huấn luyện."""
    if abs(a["ndcg_at_10"] - b["ndcg_at_10"]) >= TIE_TOLERANCE:
        return a["ndcg_at_10"] > b["ndcg_at_10"]
    if abs(a["recall_at_10"] - b["recall_at_10"]) >= TIE_TOLERANCE:
        return a["recall_at_10"] > b["recall_at_10"]
    return a.get("train_seconds", 0.0) < b.get("train_seconds", 0.0)


def _pick(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = rows[0]
    for row in rows[1:]:  # khi vẫn bằng nhau, ưu tiên thứ tự lưới
        if _better(row, best):
            best = row
    return best


@dataclass(slots=True)
class BenchmarkResult:
    """Cấu hình đã chọn và toàn bộ bảng báo cáo."""

    als: AlsParams
    mmr_lambda: float
    fpgrowth: FpGrowthParams
    quality: list[dict[str, Any]]
    complements: list[dict[str, Any]]
    split_summary: dict[str, Any]
    timings: list[dict[str, Any]]
    config: dict[str, Any]

    def selection(self) -> dict[str, Any]:
        """Cấu hình đã chốt để đánh giá tập test và phục vụ gợi ý."""
        return {
            "als": self.als.as_dict(),
            "mmr_lambda": self.mmr_lambda,
            "fpgrowth": self.fpgrowth.as_dict(),
            "personalized_method": "als_mmr",
            "criterion": "validation NDCG@10, then Recall@10 (tol 1e-6), then train time",
        }


def run_benchmark(
    spark: SparkSession,
    products: DataFrame,
    events: DataFrame,
    *,
    dataset_version: str,
    seed: int = 42,
    split_config: SplitConfig | None = None,
    als_grid: Sequence[AlsParams] = ALS_GRID,
    lambdas: Sequence[float] = MMR_LAMBDAS,
    fp_grid: Sequence[FpGrowthParams] = FPGROWTH_GRID,
    log: Callable[[str], None] = print,
) -> BenchmarkResult:
    """Chọn cấu hình trên validation, rồi đánh giá tập test một lần với cấu hình đã chốt."""
    from pyspark.sql import functions as F

    split_config = split_config or SplitConfig()
    timer = Timer()
    als_grid = [
        AlsParams(p.rank, p.max_iter, p.reg_param, p.alpha, seed) for p in als_grid
    ]
    split = timer.run("split", lambda: split_events(events, split_config))
    labeled = split.events
    quality: list[dict[str, Any]] = []
    complements: list[dict[str, Any]] = []

    # ---------------- validation: huấn luyện trên train ---------------- #
    train = labeled.where(F.col("split") == "train")
    val_events = labeled.where(F.col("split") == "validation")
    w_val = prepare_window(
        products,
        train,
        split_config.start,
        split_config.split_val_time,
        "eval",
        timer,
        "validation",
    )
    labels_val, dropped_val = relevance_labels(val_events, w_val)
    users_val = sorted(labels_val)
    log(f"validation: {len(users_val)} users with labels")

    for method in ("popularity", "tfidf"):
        lists, fb = method_lists(method, users_val, w_val)
        quality += score_lists("validation", method, "{}", lists, fb, labels_val, w_val)

    best_row: dict[str, Any] | None = None
    cands_val: dict[str, list[Scored]] = {}
    for params in als_grid:
        config = json.dumps(params.as_dict(), sort_keys=True)
        start = time.perf_counter()
        artifacts = timer.run(
            "fit_als_validation",
            lambda p=params: fit_als(w_val.interactions_df, p),
            config=config,
        )
        cands = als_candidates(artifacts, w_val.seen_df, w_val.catalog)
        seconds = time.perf_counter() - start
        lists, fb = method_lists("als", users_val, w_val, als_cands=cands)
        rows = score_lists("validation", "als", config, lists, fb, labels_val, w_val)
        quality += rows
        row = {**rows[0], "train_seconds": seconds, "params": params}
        # Chỉ giữ ứng viên tốt nhất hiện tại trong bộ nhớ; dùng thứ tự lưới khi bằng nhau.
        if best_row is None or _better(row, best_row):
            best_row, cands_val = row, cands
        log(f"  ALS {config}: NDCG@10={rows[0]['ndcg_at_10']:.4f}")
    assert best_row is not None
    best_als: AlsParams = best_row["params"]

    mmr_rows = []
    for lam in lambdas:
        stats = MmrStats()
        lists, fb = method_lists(
            "als_mmr", users_val, w_val, als_cands=cands_val, lam=lam, mmr_stats=stats
        )
        config = json.dumps({**best_als.as_dict(), "lambda": lam}, sort_keys=True)
        rows = score_lists(
            "validation", "als_mmr", config, lists, fb, labels_val, w_val
        )
        quality += rows
        mmr_rows.append({**rows[0], "lambda": lam})
        log(
            f"  MMR lambda={lam}: NDCG@10={rows[0]['ndcg_at_10']:.4f} ILD={rows[0]['ild']}"
        )
    best_lambda = float(_pick(mmr_rows)["lambda"])

    val_carts = holdout_carts(
        build_baskets(
            val_events, w_val.catalog, "eval", None, split_config.split_test_time
        ),
        w_val,
        seed,
    )
    fp_rows = []
    for params in fp_grid:
        result = timer.run(
            "fit_fpgrowth_validation",
            lambda p=params: fit_fpgrowth(w_val.baskets_df, p, "eval"),
            config=json.dumps(params.as_dict(), sort_keys=True),
        )
        rules = collect_rules(result.rules)
        row = score_complements(
            "validation",
            params,
            rules,
            val_carts,
            w_val,
            {
                "baskets_fit": result.baskets_fit,
                "baskets_single_item": result.baskets_single_item,
            },
        )
        complements.append(row)
        fp_rows.append((row, params))
        log(
            f"  FP-Growth {params.as_dict()}: hit@10={row['hit_at_10']} rules={len(rules)}"
        )
    best_fp = min(
        enumerate(fp_rows),
        key=lambda item: (
            -(item[1][0]["hit_at_10"] or 0.0),
            item[1][0]["rules"],
            item[0],
        ),
    )[1][1]

    # ---------------- test: huấn luyện lại trên train + validation ---------------- #
    history = labeled.where(F.col("split").isin("train", "validation"))
    test_events = labeled.where(F.col("split") == "test")
    w_test = prepare_window(
        products,
        history,
        split_config.start,
        split_config.split_test_time,
        "eval",
        timer,
        "test",
    )
    labels_test, dropped_test = relevance_labels(test_events, w_test)
    users_test = sorted(labels_test)
    log(f"test: {len(users_test)} users with labels")
    for method in ("popularity", "tfidf"):
        lists, fb = method_lists(method, users_test, w_test)
        quality += score_lists("test", method, "{}", lists, fb, labels_test, w_test)
    artifacts = timer.run(
        "fit_als_test", lambda: fit_als(w_test.interactions_df, best_als)
    )
    cands_test = als_candidates(artifacts, w_test.seen_df, w_test.catalog)
    config_als = json.dumps(best_als.as_dict(), sort_keys=True)
    lists, fb = method_lists("als", users_test, w_test, als_cands=cands_test)
    quality += score_lists("test", "als", config_als, lists, fb, labels_test, w_test)
    lists, fb = method_lists(
        "als_mmr", users_test, w_test, als_cands=cands_test, lam=best_lambda
    )
    config_mmr = json.dumps(
        {**best_als.as_dict(), "lambda": best_lambda}, sort_keys=True
    )
    quality += score_lists(
        "test", "als_mmr", config_mmr, lists, fb, labels_test, w_test
    )
    fp_test = timer.run(
        "fit_fpgrowth_test", lambda: fit_fpgrowth(w_test.baskets_df, best_fp, "eval")
    )
    test_carts = holdout_carts(
        build_baskets(test_events, w_test.catalog, "eval", None, split_config.end),
        w_test,
        seed,
    )
    complements.append(
        score_complements(
            "test",
            best_fp,
            collect_rules(fp_test.rules),
            test_carts,
            w_test,
            {
                "baskets_fit": fp_test.baskets_fit,
                "baskets_single_item": fp_test.baskets_single_item,
            },
        )
    )

    summary = dict(split.summary)
    summary["labels"] = {
        "validation": {
            "users": len(users_val),
            "labels": sum(len(v) for v in labels_val.values()),
            "dropped": dropped_val,
        },
        "test": {
            "users": len(users_test),
            "labels": sum(len(v) for v in labels_test.values()),
            "dropped": dropped_test,
        },
    }
    summary["catalog"] = {
        "validation_cutoff_products": len(w_val.catalog_meta),
        "test_cutoff_products": len(w_test.catalog_meta),
        "validation_cold_start_items": len(w_val.catalog_meta)
        - len({p for _, p, _ in w_val.interactions}),
        "test_cold_start_items": len(w_test.catalog_meta)
        - len({p for _, p, _ in w_test.interactions}),
        "zero_tfidf_vectors_test": w_test.features.zero_vectors,
    }
    config = {
        "dataset_version": dataset_version,
        "seed": seed,
        "split": split_config.as_dict(),
        "weights": dict(EVENT_WEIGHTS),
        "k": DEFAULT_K,
        "candidate_limit": CANDIDATE_LIMIT,
        "candidate_policy": "C_t minus H_u, filtered before top-k; popularity fallback after primary",
        "relevance": "click or add_to_cart in window, in C_t, not in H_u",
        "grids": {
            "als": [p.as_dict() for p in als_grid],
            "mmr_lambda": list(lambdas),
            "fpgrowth": [p.as_dict() for p in fp_grid],
        },
        "environment": spark_environment(spark),
        "created_at": utc_now(),
    }
    labeled.unpersist()
    return BenchmarkResult(
        best_als,
        best_lambda,
        best_fp,
        quality,
        complements,
        summary,
        timer.rows,
        config,
    )


def _csv(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=list(columns), extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {k: ("" if row.get(k) is None else row.get(k)) for k in columns}
        )
    return buffer.getvalue().encode("utf-8")


def write_evaluation(
    store: LakeStore, run_id: str, result: BenchmarkResult
) -> list[str]:
    """Ghi các file đánh giá theo EXPERIMENT_PLAN.md mục 11."""
    base = (run_id, "evaluation")
    config = {**result.config, "selection": result.selection()}
    store.write_json("gold", *base, "config.json", value=config)
    store.write_json("gold", *base, "split_summary.json", value=result.split_summary)
    store.write_json("gold", *base, "selection.json", value=result.selection())
    store.write_bytes(
        "gold", *base, "quality.csv", data=_csv(result.quality, QUALITY_COLUMNS)
    )
    complement_cols = (
        "split",
        "config",
        "status",
        "carts",
        "hit_at_10",
        "carts_with_prediction",
        "mean_confidence_of_hits",
        "rules",
        "baskets_fit",
        "baskets_single_item",
    )
    store.write_bytes(
        "gold", *base, "complements.csv", data=_csv(result.complements, complement_cols)
    )
    timing_cols = ("stage", "config", "seconds", "status", "error")
    store.write_bytes(
        "gold", *base, "batch_timings.csv", data=_csv(result.timings, timing_cols)
    )
    return [
        "evaluation/config.json",
        "evaluation/split_summary.json",
        "evaluation/selection.json",
        "evaluation/quality.csv",
        "evaluation/complements.csv",
        "evaluation/batch_timings.csv",
    ]


def format_test_table(result: BenchmarkResult) -> str:
    """Định dạng kết quả tập test dễ đọc để hiển thị trên console."""
    lines = ["model        Recall@10  NDCG@10   ILD      Coverage  fallback"]
    for row in result.quality:
        if row["split"] == "test" and row["cohort"] == "all":

            def fmt(v: Any) -> str:
                return "   n/a  " if v is None else f"{v:8.4f}"

            lines.append(
                f"{row['model']:<12} {fmt(row['recall_at_10'])}  {fmt(row['ndcg_at_10'])}  "
                f"{fmt(row['ild'])} {fmt(row['coverage'])}  {fmt(row['fallback_rate'])}"
            )
    for row in result.complements:
        if row["split"] == "test":
            lines.append(
                f"fpgrowth     hit@10={row['hit_at_10']} carts={row['carts']} rules={row['rules']}"
            )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Chạy ``python -m recommender.evaluation.benchmark`` (chỉ đánh giá)."""
    from recommender.pipeline.etl import load_silver
    from recommender.pipeline.publish_snapshot import make_run_id

    parser = argparse.ArgumentParser(description="Offline benchmark T07")
    parser.add_argument("--dataset-version")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="2g")
    parser.add_argument(
        "--quick", action="store_true", help="1 ALS config, 2 lambdas, 1 FP config"
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    store = lake_store(settings)
    spark = create_spark_session(
        "benchmark", settings, master=args.master, driver_memory=args.driver_memory
    )
    try:
        products, events, manifest = load_silver(spark, store, args.dataset_version)
        kwargs: dict[str, Any] = {}
        if args.quick:
            kwargs = {
                "als_grid": ALS_GRID[-1:],
                "lambdas": (0.7, 1.0),
                "fp_grid": FPGROWTH_GRID[:1],
            }
        result = run_benchmark(
            spark,
            products,
            events,
            dataset_version=manifest["dataset_version"],
            seed=settings.random_seed,
            **kwargs,
        )
        run_id = make_run_id(
            "eval",
            manifest["dataset_version"],
            result.selection(),
            settings.random_seed,
            evaluated=True,
            evaluation_config=result.config,
        )
        write_evaluation(store, run_id, result)
    finally:
        spark.stop()
    print(format_test_table(result))
    print(f"Evaluation written to gold/{run_id}/evaluation/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
