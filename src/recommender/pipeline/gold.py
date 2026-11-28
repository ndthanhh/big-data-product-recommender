"""Dữ liệu nền Gold T04: tổng hợp trong cửa sổ thời gian sau khi chia dữ liệu.

Mỗi hàm nhận cửa sổ học nửa mở ``[start, end)`` tường minh để dùng chung
cho đánh giá ngoại tuyến (``end`` là mốc chia) và snapshot phục vụ gợi ý.
Trọng số v1 cố định là ``view=1, click=2, add_to_cart=4`` từ
``recommender.schemas.EVENT_WEIGHTS``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from recommender.schemas import EVENT_WEIGHTS

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def weight_column(event_type: str = "event_type") -> Column:
    """Trọng số hành vi theo loại sự kiện (``1 : 2 : 4``)."""
    from pyspark.sql import functions as F

    expr = None
    for name, weight in EVENT_WEIGHTS.items():
        expr = (F.when if expr is None else expr.when)(
            F.col(event_type) == name, F.lit(float(weight))
        )
    return expr.otherwise(F.lit(None).cast("double"))  # type: ignore[union-attr]


def in_window(events: DataFrame, start: datetime | None, end: datetime) -> DataFrame:
    """Các sự kiện thỏa ``start <= event_at < end``; ``start=None`` nghĩa là không có cận dưới."""
    from pyspark.sql import functions as F

    condition = F.col("event_at") < F.lit(end)
    if start is not None:
        condition = condition & (F.col("event_at") >= F.lit(start))
    return events.where(condition)


def catalog_as_of(products: DataFrame, cutoff: datetime, run_id: str) -> DataFrame:
    """Mỗi sản phẩm lấy một phiên bản thông tin: bản mới nhất thỏa ``collected_at < cutoff``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    latest = Window.partitionBy("product_id").orderBy(F.col("collected_at").desc())
    columns = [c for c in products.columns]
    return (
        products.where(F.col("collected_at") < F.lit(cutoff))
        .withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .select(F.lit(run_id).alias("run_id"), *columns)
    )


def build_interactions(
    events: DataFrame,
    catalog: DataFrame,
    run_id: str,
    start: datetime | None,
    end: datetime,
) -> DataFrame:
    """Gold ``interactions``: ``sum(weight)`` và ``max(event_at)`` theo (người dùng, sản phẩm).

    Chỉ tổng hợp sản phẩm thuộc danh mục của lần chạy, để mô hình chỉ học
    những sản phẩm mà snapshot có thể phục vụ.
    """
    from pyspark.sql import functions as F

    return (
        in_window(events, start, end)
        .join(catalog.select("product_id"), "product_id", "left_semi")
        .groupBy("user_id", "product_id")
        .agg(
            F.sum(weight_column()).alias("interaction_weight"),
            F.max("event_at").alias("last_event_at"),
        )
        .select(
            F.lit(run_id).alias("run_id"),
            "user_id",
            "product_id",
            "interaction_weight",
            "last_event_at",
        )
    )


def build_baskets(
    events: DataFrame,
    catalog: DataFrame,
    run_id: str,
    start: datetime | None,
    end: datetime,
) -> DataFrame:
    """Gold ``baskets`` chỉ từ ``add_to_cart``: ID sản phẩm duy nhất và được sắp xếp.

    ETL đã bảo đảm mỗi giỏ chỉ thuộc một chủ sở hữu ``(user_id, session_id)``.
    """
    from pyspark.sql import functions as F

    return (
        in_window(events, start, end)
        .where(F.col("event_type") == "add_to_cart")
        .join(catalog.select("product_id"), "product_id", "left_semi")
        .groupBy("cart_id")
        .agg(
            F.first("user_id").alias("user_id"),
            F.first("session_id").alias("session_id"),
            F.array_sort(F.collect_set("product_id")).alias("product_ids"),
            F.min("event_at").alias("first_event_at"),
            F.max("event_at").alias("last_event_at"),
        )
        .select(
            F.lit(run_id).alias("run_id"),
            "cart_id",
            "user_id",
            "session_id",
            "product_ids",
            "first_event_at",
            "last_event_at",
        )
    )


def build_seen_products(
    events: DataFrame,
    catalog: DataFrame,
    run_id: str,
    start: datetime | None,
    end: datetime,
) -> DataFrame:
    """Gold ``seen_products``: mọi loại sự kiện đều đánh dấu sản phẩm đã được xem."""
    from pyspark.sql import functions as F

    return (
        in_window(events, start, end)
        .select("user_id", "product_id")
        .distinct()
        .join(catalog.select("product_id"), "product_id", "left_semi")
        .select(F.lit(run_id).alias("run_id"), "user_id", "product_id")
    )
