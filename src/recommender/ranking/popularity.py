"""T05 Popularity baseline and fallback.

``score(i) = sum of interaction_weight over all users`` in the learning window
(DATA_CONTRACTS.md section 7.1); ties break by ``product_id`` ascending.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from recommender.ranking.common import CANDIDATE_LIMIT, Scored, top_k

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def popularity_scores(interactions: DataFrame, catalog: DataFrame) -> DataFrame:
    """``(product_id, score)`` for catalog products with positive total weight."""
    from pyspark.sql import functions as F

    return (
        interactions.join(catalog.select("product_id"), "product_id", "left_semi")
        .groupBy("product_id")
        .agg(F.sum("interaction_weight").alias("score"))
        .where(F.col("score") > 0)
    )


def rank_popular(
    interactions: DataFrame, catalog: DataFrame
) -> list[tuple[str, float]]:
    """All popular products, ordered by score desc then product ID asc."""
    rows = popularity_scores(interactions, catalog).collect()
    return top_k(((r["product_id"], float(r["score"])) for r in rows), len(rows))


def popular_list(
    ranked: list[tuple[str, float]], limit: int = CANDIDATE_LIMIT
) -> list[Scored]:
    """Gold ``popular`` list for context ``global`` (not filtered per user)."""
    return [Scored(pid, score, "popularity") for pid, score in ranked[:limit]]


def recommend_popular(
    ranked: list[tuple[str, float]], seen: set[str], k: int
) -> list[Scored]:
    """Popularity for one user: unseen items only, filtered before taking k."""
    return [
        Scored(pid, score, "popularity")
        for pid, score in top_k(ranked, k, exclude=seen)
    ]
