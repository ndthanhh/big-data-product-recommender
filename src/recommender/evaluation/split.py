"""T07 time split with cart purge (EXPERIMENT_PLAN.md section 3).

Half-open windows on ``event_at`` (UTC)::

    train      = [start, split_val_time)
    validation = [split_val_time, split_test_time)
    test       = [split_test_time, end)

An event exactly at a cutoff belongs to the later part. A cart with
``first_event_at < cutoff <= last_event_at`` for either cutoff is purged
entirely from all three parts (never split, never moved into train).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from recommender.pipeline.lake import iso_z, to_utc

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SPLIT_START = datetime(2026, 10, 1, tzinfo=UTC)
SPLIT_VAL_TIME = datetime(2026, 10, 25, tzinfo=UTC)
SPLIT_TEST_TIME = datetime(2026, 11, 1, tzinfo=UTC)
SPLIT_END = datetime(2026, 11, 8, tzinfo=UTC)
PARTS = ("train", "validation", "test")


@dataclass(frozen=True, slots=True)
class SplitConfig:
    """Observation window and the two cutoffs."""

    start: datetime = SPLIT_START
    split_val_time: datetime = SPLIT_VAL_TIME
    split_test_time: datetime = SPLIT_TEST_TIME
    end: datetime = SPLIT_END

    def __post_init__(self) -> None:
        if not self.start < self.split_val_time < self.split_test_time < self.end:
            raise ValueError("Require start < split_val_time < split_test_time < end")

    def as_dict(self) -> dict[str, str]:
        """JSON form with ``Z`` timestamps."""
        return {
            "start": iso_z(self.start),
            "split_val_time": iso_z(self.split_val_time),
            "split_test_time": iso_z(self.split_test_time),
            "end": iso_z(self.end),
        }


@dataclass(slots=True)
class SplitResult:
    """Events labeled with ``split`` plus the summary/invariant report."""

    events: DataFrame
    purged_carts: list[str]
    summary: dict[str, Any] = field(default_factory=dict)


def split_events(events: DataFrame, config: SplitConfig | None = None) -> SplitResult:
    """Label in-window events ``train``/``validation``/``test`` after cart purge."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    config = config or SplitConfig()
    window = events.where(
        (F.col("event_at") >= F.lit(config.start))
        & (F.col("event_at") < F.lit(config.end))
    )
    outside = events.count() - window.count()
    spans = (
        window.where(F.col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(F.min("event_at").alias("first"), F.max("event_at").alias("last"))
    )
    crossing = spans.where(
        (
            (F.col("first") < F.lit(config.split_val_time))
            & (F.lit(config.split_val_time) <= F.col("last"))
        )
        | (
            (F.col("first") < F.lit(config.split_test_time))
            & (F.lit(config.split_test_time) <= F.col("last"))
        )
    ).select("cart_id")
    purged_carts = sorted(r["cart_id"] for r in crossing.collect())
    purged = window.join(F.broadcast(crossing), "cart_id", "left_semi")
    kept = window.join(F.broadcast(crossing), "cart_id", "left_anti")
    labeled = kept.withColumn(
        "split",
        F.when(F.col("event_at") < F.lit(config.split_val_time), "train")
        .when(F.col("event_at") < F.lit(config.split_test_time), "validation")
        .otherwise("test"),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    by_part = {
        (r["split"], r["event_type"], r["source"]): int(r["count"])
        for r in labeled.groupBy("split", "event_type", "source").count().collect()
    }
    parts: dict[str, Any] = {}
    for part in PARTS:
        part_df = labeled.where(F.col("split") == part)
        stats = part_df.agg(
            F.count(F.lit(1)).alias("events"),
            F.countDistinct("user_id").alias("users"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("cart_id").alias("carts"),
            F.min("event_at").alias("tmin"),
            F.max("event_at").alias("tmax"),
        ).first()
        parts[part] = {
            "events": int(stats["events"]),
            "users": int(stats["users"]),
            "sessions": int(stats["sessions"]),
            "carts": int(stats["carts"]),
            "event_time_min": iso_z(stats["tmin"]) if stats["tmin"] else None,
            "event_time_max": iso_z(stats["tmax"]) if stats["tmax"] else None,
            "by_type": {
                t: sum(v for (p, tt, _), v in by_part.items() if p == part and tt == t)
                for t in ("view", "click", "add_to_cart")
            },
            "by_source": {
                s: sum(v for (p, _, ss), v in by_part.items() if p == part and ss == s)
                for s in ("synthetic", "demo")
            },
        }
    purged_stats = purged.agg(
        F.count(F.lit(1)).alias("events"),
        F.countDistinct("user_id").alias("users"),
        F.sum(
            F.when(F.col("event_type").isin("click", "add_to_cart"), 1).otherwise(0)
        ).alias("label_events"),
    ).first()
    summary = {
        "config": config.as_dict(),
        "events_outside_window": int(outside),
        "parts": parts,
        "cart_purge": {
            "reason": "cart spans a cutoff (first_event_at < cutoff <= last_event_at)",
            "carts": len(purged_carts),
            "events": int(purged_stats["events"]),
            "users": int(purged_stats["users"]),
            "label_events": int(purged_stats["label_events"] or 0),
        },
        "invariants": check_invariants(labeled, config),
    }
    if not all(summary["invariants"].values()):
        raise AssertionError(f"Split invariants violated: {summary['invariants']}")
    return SplitResult(labeled, purged_carts, summary)


def check_invariants(labeled: DataFrame, config: SplitConfig) -> dict[str, bool]:
    """Leakage checks required before training (EXPERIMENT_PLAN.md 3.2)."""
    from pyspark.sql import functions as F

    max_by = {
        r["split"]: r["tmax"]
        for r in labeled.groupBy("split").agg(F.max("event_at").alias("tmax")).collect()
    }
    train_max = max_by.get("train")
    history_max = max(
        (t for p, t in max_by.items() if p in ("train", "validation") and t),
        default=None,
    )
    carts_multi = (
        labeled.where(F.col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(F.countDistinct("split").alias("n"))
        .where(F.col("n") > 1)
        .count()
    )
    dup_ids = labeled.groupBy("event_id").count().where(F.col("count") > 1).count()
    return {
        "unique_event_ids": dup_ids == 0,
        "train_before_validation_cutoff": train_max is None
        or to_utc(train_max) < config.split_val_time,
        "history_before_test_cutoff": history_max is None
        or to_utc(history_max) < config.split_test_time,
        "no_cart_in_several_parts": carts_multi == 0,
    }
