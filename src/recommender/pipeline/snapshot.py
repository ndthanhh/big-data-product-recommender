"""Gold snapshot export: one immutable ``gold/<run_id>/`` per batch run.

This is the hand-over to the importer (T08, milestone M2). Layout follows
DATA_CONTRACTS.md section 4::

    gold/<run_id>/
      products/ interactions/ baskets/ seen_products/       Parquet tables
      recommendation_items/ association_rules/              Parquet tables
      models/tfidf/  models/tfidf_features/                 vectorizer + vectors
      models/als/ models/als_user_index/ models/als_product_index/
      evaluation/                                           benchmark (optional)
      manifest.json                                         written last

Serving models are fit on all Silver events in ``[training_start,
training_end)`` with the configuration locked by the benchmark (or defaults
with ``--skip-evaluation``). The run fingerprint is the SHA-256 of
``manifest.json``; an existing published run is never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from recommender.config import get_settings
from recommender.pipeline.gold import (
    build_baskets,
    build_interactions,
    build_seen_products,
    catalog_as_of,
)
from recommender.pipeline.lake import (
    LakeStore,
    create_spark_session,
    iso_z,
    lake_store,
    parquet_file_entries,
    provenance,
    spark_environment,
    to_utc,
    utc_now,
)
from recommender.ranking.als import AlsParams, als_candidates, fit_als, save_als
from recommender.ranking.common import (
    CANDIDATE_LIMIT,
    recommendation_rows,
    recommendation_schema,
    with_fallback,
)
from recommender.ranking.fpgrowth import FpGrowthParams, fit_fpgrowth
from recommender.ranking.mmr import MmrStats, mmr_rerank
from recommender.ranking.popularity import popular_list, rank_popular
from recommender.ranking.substitutes import build_substitutes
from recommender.ranking.tfidf import feature_rows, fit_tfidf
from recommender.schemas import (
    EVENT_WEIGHTS,
    SCHEMA_VERSION,
    AssociationRule,
    RecommendationItem,
    validate_recommendation_items,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from recommender.evaluation.benchmark import BenchmarkResult

SERVING_TABLES = (
    "products",
    "recommendation_items",
    "seen_products",
    "association_rules",
)
DEFAULT_SELECTION: dict[str, Any] = {
    "als": AlsParams().as_dict(),
    "mmr_lambda": 0.7,
    "fpgrowth": FpGrowthParams().as_dict(),
    "personalized_method": "als_mmr",
    "criterion": "defaults (benchmark skipped)",
}


def make_run_id(
    prefix: str,
    dataset_version: str,
    selection: dict[str, Any],
    seed: int,
    training_end: datetime | None = None,
    *,
    evaluated: bool = False,
) -> str:
    """Deterministic run ID: same inputs/code/config -> same ID (no-op rerun).

    ``evaluated`` separates a run that carries ``evaluation/`` from one built
    with a reused or default selection, so neither shadows the other.
    """
    commit, dirty = provenance()
    digest = hashlib.sha256(
        json.dumps(
            {
                "dataset_version": dataset_version,
                "selection": selection,
                "seed": seed,
                "training_end": iso_z(training_end) if training_end else None,
                "commit": commit,
                "dirty": dirty,
                "evaluated": evaluated,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()[:8]
    stamp = (training_end or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    method = str(selection.get("personalized_method", "als_mmr")).replace("_", "")
    return f"{prefix}_{stamp}_{method}_{seed}_{digest}"


def _write(
    df: DataFrame, store: LakeStore, run_id: str, table: str, files: int = 1
) -> None:
    df.coalesce(files).write.mode("overwrite").parquet(store.uri("gold", run_id, table))


def _model_entries(
    spark: SparkSession, store: LakeStore, run_id: str
) -> list[dict[str, Any]]:
    entries = []
    for item in store.list_files("gold", run_id, "models"):
        leaf = item.path.rsplit("/", 1)[-1]
        if leaf.startswith(".") or leaf.endswith(".crc") or leaf == "_SUCCESS":
            continue
        path = f"models/{item.path}"
        is_parquet = leaf.endswith(".parquet")
        entries.append(
            {
                "path": path,
                "format": "parquet" if is_parquet else "binary",
                "rows": spark.read.parquet(store.uri("gold", run_id, path)).count()
                if is_parquet
                else None,
                "bytes": item.size,
                "sha256": store.sha256("gold", run_id, path),
                "table": "models/" + item.path.split("/", 1)[0],
            }
        )
    return entries


def build_snapshot(
    spark: SparkSession,
    store: LakeStore,
    products: DataFrame,
    events: DataFrame,
    *,
    dataset_version: str,
    selection: dict[str, Any],
    seed: int,
    training_start: datetime,
    training_end: datetime,
    run_id: str,
    evaluation: BenchmarkResult | None = None,
    log: Any = print,
) -> dict[str, Any]:
    """Fit serving models, export every Gold table, then publish the manifest."""
    from pyspark.sql import types as T

    if store.exists("gold", run_id, "manifest.json"):
        log(f"Run {run_id} already published; nothing to do")
        return store.read_json("gold", run_id, "manifest.json")

    timings: dict[str, float] = {}
    start_all = time.perf_counter()

    def stage(name: str, began: float) -> None:
        timings[name] = round(time.perf_counter() - began, 3)

    t = time.perf_counter()
    catalog = catalog_as_of(products, training_end, run_id).cache()
    interactions = build_interactions(
        events, catalog, run_id, training_start, training_end
    ).cache()
    baskets = build_baskets(
        events, catalog, run_id, training_start, training_end
    ).cache()
    seen = build_seen_products(
        events, catalog, run_id, training_start, training_end
    ).cache()
    n_catalog = catalog.count()
    if n_catalog == 0:
        raise ValueError("Catalog as of training_end is empty")
    stage("aggregate", t)

    # TF-IDF + substitutes
    t = time.perf_counter()
    tfidf_model, features = fit_tfidf(catalog)
    substitutes = build_substitutes(features)
    stage("fit_tfidf", t)

    # Popularity
    t = time.perf_counter()
    popular = rank_popular(interactions, catalog)
    stage("fit_popularity", t)

    # ALS + MMR personalized, popularity fill after the primary list
    t = time.perf_counter()
    als_params = AlsParams(**{**selection["als"], "seed": seed})
    als = fit_als(interactions, als_params)
    candidates = als_candidates(als, seen, catalog)
    stage("fit_als", t)
    t = time.perf_counter()
    seen_map: dict[str, set[str]] = {}
    for r in seen.collect():
        seen_map.setdefault(r["user_id"], set()).add(r["product_id"])
    vectors = {pid: features.vector(pid) for pid in features.product_ids}
    lam = float(selection["mmr_lambda"])
    mmr_stats = MmrStats()
    personalized = {}
    for user, cands in candidates.items():
        primary = (
            mmr_rerank(cands, vectors, lam, stats=mmr_stats)
            if selection.get("personalized_method", "als_mmr") == "als_mmr"
            else cands
        )
        personalized[user] = with_fallback(
            primary, popular, seen=seen_map.get(user, set()), limit=CANDIDATE_LIMIT
        )
    stage("rerank_mmr", t)

    # FP-Growth complements
    t = time.perf_counter()
    fp_params = FpGrowthParams(**selection["fpgrowth"])
    fp = fit_fpgrowth(baskets, fp_params, run_id)
    rules = fp.rules.cache()
    n_rules = rules.count()
    stage("fit_fpgrowth", t)

    # Contract validation before anything is written
    t = time.perf_counter()
    rows = (
        recommendation_rows(run_id, "popular", {"global": popular_list(popular)})
        + recommendation_rows(run_id, "substitute", substitutes)
        + recommendation_rows(run_id, "personalized", personalized)
    )
    validate_recommendation_items(
        [
            RecommendationItem(
                run_id=r[0],
                kind=r[1],
                context_id=r[2],
                product_id=r[3],
                rank=r[4],
                score=r[5],
                method=r[6],
            )
            for r in rows
        ]
    )
    catalog_ids = set(features.product_ids)
    if any(r[3] not in catalog_ids for r in rows):
        raise ValueError(
            "recommendation_items reference a product outside the run catalog"
        )
    rule_rows = rules.collect()
    for r in rule_rows:
        AssociationRule(
            run_id=r["run_id"],
            antecedent_ids=list(r["antecedent_ids"]),
            consequent_id=r["consequent_id"],
            support=float(r["support"]),
            confidence=float(r["confidence"]),
            lift=float(r["lift"]),
        )
        if not set(r["antecedent_ids"]) | {r["consequent_id"]} <= catalog_ids:
            raise ValueError(
                "association_rules reference a product outside the catalog"
            )
    stage("validate", t)

    # Export tables and models
    t = time.perf_counter()
    rec_df = spark.createDataFrame(rows, recommendation_schema())
    _write(catalog, store, run_id, "products")
    _write(interactions, store, run_id, "interactions")
    _write(baskets, store, run_id, "baskets")
    _write(seen, store, run_id, "seen_products")
    _write(
        rec_df.orderBy("kind", "context_id", "rank"),
        store,
        run_id,
        "recommendation_items",
        2,
    )
    _write(
        rules.orderBy("antecedent_ids", "consequent_id"),
        store,
        run_id,
        "association_rules",
    )
    tfidf_model.write().overwrite().save(store.uri("gold", run_id, "models", "tfidf"))
    feature_schema = T.StructType(
        [
            T.StructField("product_id", T.StringType(), False),
            T.StructField("category_id", T.StringType(), False),
            T.StructField("indices", T.ArrayType(T.IntegerType(), False), False),
            T.StructField("values", T.ArrayType(T.DoubleType(), False), False),
            T.StructField("size", T.IntegerType(), False),
        ]
    )
    _write(
        spark.createDataFrame(feature_rows(features), feature_schema),
        store,
        run_id,
        "models/tfidf_features",
    )
    save_als(
        als,
        store.uri("gold", run_id, "models", "als"),
        store.uri("gold", run_id, "models", "als_user_index"),
        store.uri("gold", run_id, "models", "als_product_index"),
    )
    evaluation_files: list[str] = []
    if evaluation is not None:
        from recommender.evaluation.benchmark import write_evaluation

        evaluation_files = write_evaluation(store, run_id, evaluation)
    stage("recommend_export", t)

    tables = (
        "products",
        "interactions",
        "baskets",
        "seen_products",
        "recommendation_items",
        "association_rules",
    )
    files = []
    for table in tables:
        files += parquet_file_entries(spark, store, "gold", run_id, table, table)
    files += _model_entries(spark, store, run_id)
    for rel in evaluation_files:
        data = store.read_bytes("gold", run_id, rel)
        files.append(
            {
                "path": rel,
                "format": "json" if rel.endswith(".json") else "binary",
                "rows": None,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    counts = {
        table: sum(f["rows"] for f in files if f.get("table") == table)
        for table in tables
    }
    for table in SERVING_TABLES:
        if not any(f.get("table") == table for f in files):
            raise RuntimeError(f"Serving table {table} has no data file")
    kinds = rec_df.groupBy("kind", "method").count().collect()
    counts.update(
        {
            "recommendation_items_by_kind_method": {
                f"{r['kind']}/{r['method']}": int(r["count"]) for r in kinds
            },
            "personalized_users": len(personalized),
            "substitute_contexts": sum(1 for v in substitutes.values() if v),
            "baskets_total": fp.baskets_total,
            "baskets_fit": fp.baskets_fit,
            "baskets_single_item_excluded": fp.baskets_single_item,
            "association_rules": n_rules,
            "mmr_candidates": mmr_stats.candidates,
            "mmr_missing_vectors": mmr_stats.missing_vectors,
            "tfidf_zero_vectors": features.zero_vectors,
            "tfidf_vocabulary": len(tfidf_model.stages[1].vocabulary),
        }
    )
    commit, dirty = provenance()
    timings["total"] = round(time.perf_counter() - start_all, 3)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "dataset_version": dataset_version,
        "created_at": utc_now(),
        "code_commit": commit,
        "code_dirty": dirty,
        "seed": seed,
        "training_start": iso_z(training_start),
        "training_end": iso_z(training_end),
        "evaluation_window": evaluation.config["split"]
        if evaluation is not None
        else None,
        "parameters": {
            "weights": dict(EVENT_WEIGHTS),
            "personalized_method": selection.get("personalized_method", "als_mmr"),
            "als": als_params.as_dict()
            | {"implicitPrefs": True, "coldStartStrategy": "drop"},
            "mmr_lambda": lam,
            "fpgrowth": fp_params.as_dict(),
            "tfidf": {
                "text": "NFC lowercase name + category_id + description",
                "token_pattern": r"[^\p{L}\p{N}]+",
                "vocab_size": 20000,
                "min_df": 1,
            },
            "candidate_limit": CANDIDATE_LIMIT,
            "filters": "personalized: unseen + in catalog; substitute: same category, "
            "not self, score > 0; popular: global, unfiltered",
            "tie_break": "score desc, product_id asc (MMR: selection order)",
            "selection": selection,
            "stage_seconds": timings,
        },
        "environment": spark_environment(spark),
        "counts": counts,
        "files": files,
    }
    store.write_json("gold", run_id, "manifest.json", value=manifest)
    for frame in (catalog, interactions, baskets, seen, rules):
        frame.unpersist()
    return manifest


def default_window(events: DataFrame) -> tuple[datetime, datetime]:
    """``[floor_day(min event_at), floor_day(max event_at) + 1 day)``."""
    from pyspark.sql import functions as F

    row = events.agg(F.min("event_at").alias("a"), F.max("event_at").alias("b")).first()
    if row["a"] is None:
        raise ValueError("Silver dataset has no events")
    lo, hi = to_utc(row["a"]), to_utc(row["b"])
    start = lo.replace(hour=0, minute=0, second=0, microsecond=0)
    end = hi.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return start, end


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("timestamps must be UTC (Z or +00:00)")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    """Run ``python -m recommender.pipeline.snapshot``: benchmark + serving run."""
    from recommender.evaluation.benchmark import (
        ALS_GRID,
        FPGROWTH_GRID,
        format_test_table,
        run_benchmark,
    )
    from recommender.pipeline.etl import load_silver

    parser = argparse.ArgumentParser(description="Export a Gold serving snapshot")
    parser.add_argument("--dataset-version")
    parser.add_argument("--training-start", type=_parse_time)
    parser.add_argument("--training-end", type=_parse_time)
    parser.add_argument("--skip-evaluation", action="store_true")
    parser.add_argument(
        "--selection",
        type=Path,
        help="selection.json of an earlier benchmark (implies --skip-evaluation)",
    )
    parser.add_argument("--quick", action="store_true", help="reduced benchmark grid")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="2g")
    args = parser.parse_args(argv)
    settings = get_settings()
    store = lake_store(settings)
    spark = create_spark_session(
        "gold-snapshot", settings, master=args.master, driver_memory=args.driver_memory
    )
    try:
        products, events, manifest = load_silver(spark, store, args.dataset_version)
        start, end = default_window(events)
        start = args.training_start or start
        end = args.training_end or end
        evaluation = None
        selection = DEFAULT_SELECTION
        if args.selection is not None:
            selection = json.loads(args.selection.read_text(encoding="utf-8"))
        elif not args.skip_evaluation:
            kwargs: dict[str, Any] = {}
            if args.quick:
                kwargs = {
                    "als_grid": ALS_GRID[-1:],
                    "lambdas": (0.7, 1.0),
                    "fp_grid": FPGROWTH_GRID[:1],
                }
            evaluation = run_benchmark(
                spark,
                products,
                events,
                dataset_version=manifest["dataset_version"],
                seed=settings.random_seed,
                **kwargs,
            )
            selection = evaluation.selection()
            print(format_test_table(evaluation))
        run_id = make_run_id(
            "run",
            manifest["dataset_version"],
            selection,
            settings.random_seed,
            end,
            evaluated=evaluation is not None,
        )
        result = build_snapshot(
            spark,
            store,
            products,
            events,
            dataset_version=manifest["dataset_version"],
            selection=selection,
            seed=settings.random_seed,
            training_start=start,
            training_end=end,
            run_id=run_id,
            evaluation=evaluation,
        )
    finally:
        spark.stop()
    fingerprint = hashlib.sha256(
        store.read_bytes("gold", run_id, "manifest.json")
    ).hexdigest()
    print(f"Gold run {run_id} published (fingerprint {fingerprint})")
    print(json.dumps({k: v for k, v in result["counts"].items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
