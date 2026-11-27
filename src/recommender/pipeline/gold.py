"""T04 Gold base: window-scoped aggregates computed after the time split.

Every function takes an explicit half-open learning window ``[start, end)``
so the same code serves offline evaluation (``end`` = a split cutoff) and the
serving snapshot. Weights are the fixed v1 values ``view=1, click=2,
add_to_cart=4`` from ``recommender.schemas.EVENT_WEIGHTS``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from recommender.schemas import EVENT_WEIGHTS

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def weight_column(event_type: str = "event_type") -> Column:
    """Behavior weight of an event type (``1 : 2 : 4``)."""
    from pyspark.sql import functions as F

    expr = None
    for name, weight in EVENT_WEIGHTS.items():
        expr = (F.when if expr is None else expr.when)(
            F.col(event_type) == name, F.lit(float(weight))
        )
    return expr.otherwise(F.lit(None).cast("double"))  # type: ignore[union-attr]


def in_window(events: DataFrame, start: datetime | None, end: datetime) -> DataFrame:
    """Events with ``start <= event_at < end`` (``start=None`` means unbounded)."""
    from pyspark.sql import functions as F

    condition = F.col("event_at") < F.lit(end)
    if start is not None:
        condition = condition & (F.col("event_at") >= F.lit(start))
    return events.where(condition)


def catalog_as_of(products: DataFrame, cutoff: datetime, run_id: str) -> DataFrame:
    """One metadata version per product: the latest with ``collected_at < cutoff``."""
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
    """Gold ``interactions``: ``sum(weight)`` and ``max(event_at)`` per (user, product).

    Only products of the run catalog are aggregated, so models never learn an
    item the snapshot cannot serve.
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
    """Gold ``baskets`` from ``add_to_cart`` only: sorted unique product IDs.

    ETL already guarantees a cart has one ``(user_id, session_id)`` owner.
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
    """Gold ``seen_products``: every event type marks an item as seen."""
    from pyspark.sql import functions as F

    return (
        in_window(events, start, end)
        .select("user_id", "product_id")
        .distinct()
        .join(catalog.select("product_id"), "product_id", "left_semi")
        .select(F.lit(run_id).alias("run_id"), "user_id", "product_id")
    )
