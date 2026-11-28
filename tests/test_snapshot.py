"""Xuất snapshot Gold và đo thử toàn bộ luồng trên dữ liệu mẫu ngoại tuyến."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from recommender.evaluation.benchmark import run_benchmark, write_evaluation
from recommender.pipeline.etl import load_silver, run_etl
from recommender.pipeline.publish_snapshot import (
    SERVING_TABLES,
    build_snapshot,
    make_run_id,
)
from recommender.ranking.als import AlsParams
from recommender.ranking.fpgrowth import FpGrowthParams
from recommender.schemas import (
    AssociationRule,
    RecommendationItem,
    validate_recommendation_items,
)

pytestmark = pytest.mark.spark

SELECTION = {
    "als": AlsParams(rank=4, max_iter=5).as_dict(),
    "mmr_lambda": 0.7,
    "fpgrowth": FpGrowthParams(min_support=0.1, min_confidence=0.2).as_dict(),
    "personalized_method": "als_mmr",
}


def test_snapshot_exports_contract_tables(spark, lake) -> None:
    silver = run_etl(spark, lake)
    products, events, _ = load_silver(spark, lake)
    end = datetime(2026, 11, 8, tzinfo=UTC)
    run_id = make_run_id("run", silver.dataset_version, SELECTION, 42, end)
    manifest = build_snapshot(
        spark,
        lake,
        products,
        events,
        dataset_version=silver.dataset_version,
        selection=SELECTION,
        seed=42,
        training_start=datetime(2026, 10, 1, tzinfo=UTC),
        training_end=end,
        run_id=run_id,
        log=lambda _msg: None,
    )
    assert manifest["run_id"] == run_id and manifest["schema_version"] == 1
    for key in (
        "training_start",
        "training_end",
        "parameters",
        "environment",
        "counts",
    ):
        assert key in manifest
    tables = {f.get("table") for f in manifest["files"]}
    assert set(SERVING_TABLES) <= tables
    assert {
        "models/als",
        "models/tfidf",
        "models/als_user_index",
        "models/fpgrowth",
    } <= tables
    for entry in manifest["files"]:
        data = lake.read_bytes("gold", run_id, entry["path"])
        assert hashlib.sha256(data).hexdigest() == entry["sha256"]
        assert entry["bytes"] == len(data)

    gold = lambda t: spark.read.parquet(lake.uri("gold", run_id, t))
    recs = gold("recommendation_items").collect()
    assert len(recs) == manifest["counts"]["recommendation_items"]
    assert {r["run_id"] for r in recs} == {run_id}
    validate_recommendation_items([RecommendationItem(**r.asDict()) for r in recs])
    catalog = {r["product_id"]: r["category_id"] for r in gold("products").collect()}
    seen = {(r["user_id"], r["product_id"]) for r in gold("seen_products").collect()}
    for r in recs:
        assert r["product_id"] in catalog
        if r["kind"] == "substitute":
            assert catalog[r["context_id"]] == catalog[r["product_id"]]
        if r["kind"] == "personalized":
            assert (r["context_id"], r["product_id"]) not in seen
    for r in gold("association_rules").collect():
        AssociationRule(**r.asDict())

    # Nạp lại mô hình ALS cùng ánh xạ đã lưu.
    from pyspark.ml.recommendation import ALSModel

    model = ALSModel.load(lake.uri("gold", run_id, "models", "als"))
    index = gold("models/als_user_index")
    assert model.userFactors.count() == index.count()
    from pyspark.ml.fpm import FPGrowthModel

    fp_model = FPGrowthModel.load(lake.uri("gold", run_id, "models", "fpgrowth"))
    assert fp_model.freqItemsets.count() > 0

    # Công bố lại cùng lần chạy không thay đổi dữ liệu hay dấu vân tay.
    before = lake.read_bytes("gold", run_id, "manifest.json")
    build_snapshot(
        spark,
        lake,
        products,
        events,
        dataset_version=silver.dataset_version,
        selection=SELECTION,
        seed=42,
        training_start=datetime(2026, 10, 1, tzinfo=UTC),
        training_end=end,
        run_id=run_id,
        log=lambda _msg: None,
    )
    assert lake.read_bytes("gold", run_id, "manifest.json") == before


def test_benchmark_runs_end_to_end_on_fixture(spark, lake) -> None:
    silver = run_etl(spark, lake)
    products, events, _ = load_silver(spark, lake)
    result = run_benchmark(
        spark,
        products,
        events,
        dataset_version=silver.dataset_version,
        als_grid=[AlsParams(rank=4, max_iter=5)],
        lambdas=(0.5, 1.0),
        fp_grid=[FpGrowthParams(min_support=0.1, min_confidence=0.2)],
        log=lambda _msg: None,
    )
    models = {(r["split"], r["model"]) for r in result.quality}
    for split in ("validation", "test"):
        for model in ("popularity", "tfidf", "als", "als_mmr"):
            assert (split, model) in models
    assert all(result.split_summary["invariants"].values())
    files = write_evaluation(lake, "eval_fixture", result)
    for rel in files:
        assert lake.exists("gold", "eval_fixture", rel)
    assert result.selection()["mmr_lambda"] in (0.5, 1.0)
