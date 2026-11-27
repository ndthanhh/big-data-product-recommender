"""T06 FP-Growth complements ("Thường thêm cùng").

Fit: Spark ML ``FPGrowth`` on train baskets with at least two items; the
support denominator ``|B|`` is the number of baskets actually fitted.
Metrics are recomputed from frequent itemsets for every single-consequent
rule (DATA_CONTRACTS.md section 4.3)::

    support(A -> i)    = count(A u {i}) / |B|
    confidence(A -> i) = count(A u {i}) / count(A)
    lift(A -> i)       = confidence / (count({i}) / |B|)

Query (section 6.7): rules whose antecedent is a subset of the cart; per
consequent not in the cart keep the rule with max confidence, then lift, then
lexicographic antecedent; order results by confidence desc, lift desc,
product_id asc. Optional compatibility rules filter technically
incompatible pairs; without a configured rule a pair is only co-occurrence.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Category pairs whose recommendation requires an equal, present attribute.
# Example from DATA_CONTRACTS.md 6.7: frozenset({"cable", "keyboard"}): "connector".
COMPATIBILITY_RULES: dict[frozenset[str], str] = {}


@dataclass(frozen=True, slots=True)
class FpGrowthParams:
    """Thresholds of one FP-Growth configuration."""

    min_support: float = 0.001
    min_confidence: float = 0.2

    def as_dict(self) -> dict[str, float]:
        """JSON-friendly form."""
        return asdict(self)


FPGROWTH_GRID = tuple(FpGrowthParams(s, c) for s in (0.001, 0.01) for c in (0.2, 0.5))


@dataclass(frozen=True, slots=True)
class Rule:
    """One association rule with a sorted antecedent."""

    antecedent_ids: tuple[str, ...]
    consequent_id: str
    support: float
    confidence: float
    lift: float


@dataclass(slots=True)
class FpGrowthResult:
    """Rules plus basket statistics recorded in the run manifest."""

    rules: DataFrame
    baskets_total: int
    baskets_fit: int
    baskets_single_item: int


def rules_schema():
    """Spark schema of Gold ``association_rules``."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField(
                "antecedent_ids", T.ArrayType(T.StringType(), containsNull=False), False
            ),
            T.StructField("consequent_id", T.StringType(), False),
            T.StructField("support", T.DoubleType(), False),
            T.StructField("confidence", T.DoubleType(), False),
            T.StructField("lift", T.DoubleType(), False),
        ]
    )


def fit_fpgrowth(
    baskets: DataFrame,
    params: FpGrowthParams,
    run_id: str,
    *,
    model_path: str | None = None,
) -> FpGrowthResult:
    """Mine rules from Gold baskets of one learning window."""
    from pyspark.ml.fpm import FPGrowth
    from pyspark.sql import functions as F

    spark = baskets.sparkSession
    total = baskets.count()
    fit_baskets = (
        baskets.select(
            F.array_sort(F.array_distinct("product_ids")).alias("product_ids")
        )
        .where(F.size("product_ids") >= 2)
        .select(F.col("product_ids").alias("items"))
        .cache()
    )
    n_fit = fit_baskets.count()
    if n_fit == 0:
        return FpGrowthResult(
            spark.createDataFrame([], rules_schema()), total, 0, total
        )

    model = FPGrowth(
        itemsCol="items",
        minSupport=params.min_support,
        minConfidence=params.min_confidence,
    ).fit(fit_baskets)
    if model_path:
        # The publisher has already checked that no ready manifest exists.
        # Overwrite a partial, unpublished model left by an interrupted run.
        model.write().overwrite().save(model_path)
    freq = model.freqItemsets.select(
        F.array_sort("items").alias("key"), F.col("freq").cast("double").alias("freq")
    ).cache()
    singles = freq.where(F.size("key") == 1).select(
        F.col("key")[0].alias("consequent_id"), F.col("freq").alias("freq_c")
    )
    rules = (
        model.associationRules.where(F.size("consequent") == 1)
        .select(
            F.array_sort("antecedent").alias("antecedent_ids"),
            F.col("consequent")[0].alias("consequent_id"),
        )
        .withColumn(
            "union_key",
            F.array_sort(F.array_union("antecedent_ids", F.array("consequent_id"))),
        )
        .join(
            freq.withColumnRenamed("key", "union_key").withColumnRenamed(
                "freq", "freq_u"
            ),
            "union_key",
        )
        .join(
            freq.withColumnRenamed("key", "antecedent_ids").withColumnRenamed(
                "freq", "freq_a"
            ),
            "antecedent_ids",
        )
        .join(singles, "consequent_id")
        .select(
            F.lit(run_id).alias("run_id"),
            "antecedent_ids",
            "consequent_id",
            (F.col("freq_u") / F.lit(float(n_fit))).alias("support"),
            (F.col("freq_u") / F.col("freq_a")).alias("confidence"),
            (
                (F.col("freq_u") / F.col("freq_a"))
                / (F.col("freq_c") / F.lit(float(n_fit)))
            ).alias("lift"),
        )
        .where(F.col("confidence") >= F.lit(params.min_confidence))
    )
    rules = spark.createDataFrame(rules.rdd, rules_schema())
    return FpGrowthResult(rules, total, n_fit, total - n_fit)


def collect_rules(rules: DataFrame) -> list[Rule]:
    """Driver-side rules for querying/evaluation."""
    return [
        Rule(
            tuple(r["antecedent_ids"]),
            r["consequent_id"],
            float(r["support"]),
            float(r["confidence"]),
            float(r["lift"]),
        )
        for r in rules.collect()
    ]


def is_compatible(
    cart_product: Mapping[str, object],
    candidate: Mapping[str, object],
    compatibility: Mapping[frozenset[str], str] = COMPATIBILITY_RULES,
) -> bool:
    """Apply a configured attribute rule; unconfigured category pairs pass."""
    attribute = compatibility.get(
        frozenset({str(cart_product["category_id"]), str(candidate["category_id"])})
    )
    if attribute is None:
        return True
    left = (cart_product.get("attributes") or {}).get(attribute)  # type: ignore[union-attr]
    right = (candidate.get("attributes") or {}).get(attribute)  # type: ignore[union-attr]
    return left is not None and right is not None and left == right


def recommend_complements(
    rules: Sequence[Rule],
    cart: Iterable[str],
    *,
    limit: int = 10,
    catalog: Mapping[str, Mapping[str, object]] | None = None,
    compatibility: Mapping[frozenset[str], str] = COMPATIBILITY_RULES,
) -> list[tuple[str, float, Rule]]:
    """``(product_id, confidence, winning rule)`` for one cart."""
    basket = set(cart)
    if not basket:
        return []
    best: dict[str, Rule] = {}
    for rule in rules:
        if rule.consequent_id in basket or not set(rule.antecedent_ids) <= basket:
            continue
        if catalog is not None and rule.consequent_id not in catalog:
            continue
        current = best.get(rule.consequent_id)
        key = (-rule.confidence, -rule.lift, rule.antecedent_ids)
        if current is None or key < (
            -current.confidence,
            -current.lift,
            current.antecedent_ids,
        ):
            best[rule.consequent_id] = rule
    results = []
    for pid, rule in best.items():
        if catalog is not None and compatibility:
            candidate = catalog[pid]
            if not all(
                is_compatible(catalog[c], candidate, compatibility)
                for c in basket
                if c in catalog
            ):
                continue
        results.append((pid, rule.confidence, rule))
    results.sort(key=lambda item: (-item[2].confidence, -item[2].lift, item[0]))
    return results[:limit]


def fit_rules(
    baskets: DataFrame,
    *,
    min_support: float,
    min_confidence: float,
    model_path: str | None = None,
) -> DataFrame:
    """Backward-compatible facade using the same pipeline FP-Growth implementation."""
    if not 0 < min_support <= 1 or not 0 < min_confidence <= 1:
        raise ValueError("Support and confidence must lie in (0,1]")
    runs = baskets.select("run_id").distinct().limit(2).collect()
    if len(runs) > 1 or (runs and runs[0].run_id is None):
        raise ValueError("Expected one run per FP-Growth fit")
    if not runs:
        return baskets.sparkSession.createDataFrame([], rules_schema())
    return fit_fpgrowth(
        baskets,
        FpGrowthParams(min_support, min_confidence),
        runs[0].run_id,
        model_path=model_path,
    ).rules


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

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
