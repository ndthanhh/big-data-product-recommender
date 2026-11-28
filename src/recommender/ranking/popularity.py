"""T05 phương pháp cơ sở và dự phòng theo độ phổ biến (Popularity).

Điểm ``score(i)`` là tổng ``interaction_weight`` của mọi người dùng trong
cửa sổ học (DATA_CONTRACTS.md mục 7.1); nếu bằng điểm, xếp theo ``product_id`` tăng dần.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from recommender.ranking.common import CANDIDATE_LIMIT, Scored, top_k

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def popularity_scores(interactions: DataFrame, catalog: DataFrame) -> DataFrame:
    """``(product_id, score)`` của sản phẩm trong danh mục có tổng trọng số dương."""
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
    """Tất cả sản phẩm phổ biến, xếp theo điểm giảm dần rồi ID sản phẩm tăng dần."""
    rows = popularity_scores(interactions, catalog).collect()
    return top_k(((r["product_id"], float(r["score"])) for r in rows), len(rows))


def popular_list(
    ranked: list[tuple[str, float]], limit: int = CANDIDATE_LIMIT
) -> list[Scored]:
    """Danh sách Gold ``popular`` cho ngữ cảnh ``global``, chưa lọc theo từng người dùng."""
    return [Scored(pid, score, "popularity") for pid, score in ranked[:limit]]


def recommend_popular(
    ranked: list[tuple[str, float]], seen: set[str], k: int
) -> list[Scored]:
    """Gợi ý phổ biến cho một người dùng: chỉ lấy sản phẩm chưa xem, lọc trước khi lấy k."""
    return [
        Scored(pid, score, "popularity")
        for pid, score in top_k(ranked, k, exclude=seen)
    ]
