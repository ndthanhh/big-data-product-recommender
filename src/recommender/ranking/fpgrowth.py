"""Spark FP-Growth on deduplicated multi-item carts; one run per fit."""

import argparse
import json
from pathlib import Path

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RULE_SCHEMA = "run_id string, antecedent_ids array<string>, consequent_id string, support double, confidence double, lift double"


def fit_rules(
    baskets: DataFrame,
    *,
    min_support: float,
    min_confidence: float,
    model_path: str | None = None,
) -> DataFrame:
    if not 0 < min_support <= 1 or not 0 < min_confidence <= 1:
        raise ValueError("Support and confidence must lie in (0,1]")
    runs = baskets.select("run_id").distinct().limit(2).collect()
    if len(runs) > 1 or (runs and runs[0].run_id is None):
        raise ValueError("Expected one run per FP-Growth fit")
    items = baskets.select(F.sort_array(F.array_distinct("product_ids")).alias("items"))
    items = items.where(F.size("items") >= 2)
    if not items.take(1):
        return baskets.sparkSession.createDataFrame([], RULE_SCHEMA)
    model = FPGrowth(
        itemsCol="items", minSupport=min_support, minConfidence=min_confidence
    ).fit(items)
    if model_path:
        model.write().save(model_path)
    # Spark 3.5 generates a single consequent. Keep that invariant explicit;
    # never copy metrics from a multi-consequent rule.
    return model.associationRules.where(F.size("consequent") == 1).select(
        F.lit(runs[0].run_id).alias("run_id"),
        F.sort_array("antecedent").alias("antecedent_ids"),
        F.col("consequent")[0].alias("consequent_id"),
        "support",
        "confidence",
        "lift",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baskets", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--min-support", type=float, default=0.01)
    parser.add_argument("--min-confidence", type=float, default=0.1)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    spark = SparkSession.builder.appName("recommender-fpgrowth").getOrCreate()
    try:
        baskets = spark.read.parquet(args.baskets)
        rules = fit_rules(
            baskets,
            min_support=args.min_support,
            min_confidence=args.min_confidence,
            model_path=str(args.output / "model"),
        )
        rules.write.mode("error").parquet(str(args.output / "association_rules"))
        (args.output / "parameters.json").write_text(
            json.dumps(
                {
                    "min_support": args.min_support,
                    "min_confidence": args.min_confidence,
                    "basket_count": baskets.count(),
                    "excluded_single_item": baskets.where(
                        F.size(F.array_distinct("product_ids")) < 2
                    ).count(),
                }
            ),
            encoding="utf-8",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
