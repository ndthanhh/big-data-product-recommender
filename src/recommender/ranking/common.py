"""Ranking helpers shared by every batch method (DATA_CONTRACTS.md section 7)."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

CANDIDATE_LIMIT = 200  # max Gold rows per (kind, context_id)
DEFAULT_K = 10


@dataclass(frozen=True, slots=True)
class Scored:
    """One ranked candidate."""

    product_id: str
    score: float
    method: str


def rank_key(item: tuple[str, float]) -> tuple[float, str]:
    """Score descending, then product ID ascending (Unicode / collation C)."""
    return (-item[1], item[0])


def top_k(
    scores: Iterable[tuple[str, float]],
    k: int,
    *,
    exclude: set[str] | frozenset[str] = frozenset(),
    allowed: set[str] | frozenset[str] | None = None,
) -> list[tuple[str, float]]:
    """Deterministic top-k after dropping excluded, unknown and non-finite items."""
    kept = [
        (pid, float(score))
        for pid, score in scores
        if pid not in exclude
        and (allowed is None or pid in allowed)
        and math.isfinite(score)
    ]
    kept.sort(key=rank_key)
    return kept[:k]


def with_fallback(
    primary: Sequence[Scored],
    popular: Sequence[tuple[str, float]],
    *,
    seen: set[str] | frozenset[str],
    limit: int,
) -> list[Scored]:
    """Primary candidates in their own order, then unseen popularity fill.

    Scores of different methods are never re-sorted together (DATA_CONTRACTS.md
    section 6.5): fallback rows always come after the primary list.
    """
    result: list[Scored] = []
    used: set[str] = set()
    for item in primary:
        if len(result) >= limit:
            return result
        if item.product_id in seen or item.product_id in used:
            continue
        result.append(item)
        used.add(item.product_id)
    for pid, score in popular:
        if len(result) >= limit:
            break
        if pid in seen or pid in used:
            continue
        result.append(Scored(pid, score, "popularity"))
        used.add(pid)
    return result


def recommendation_rows(
    run_id: str,
    kind: str,
    lists: Mapping[str, Sequence[Scored]],
) -> list[tuple[str, str, str, str, int, float, str]]:
    """Flatten per-context lists into Gold ``recommendation_items`` tuples."""
    rows = []
    for context_id in sorted(lists):
        for rank, item in enumerate(lists[context_id], start=1):
            rows.append(
                (
                    run_id,
                    kind,
                    context_id,
                    item.product_id,
                    rank,
                    item.score,
                    item.method,
                )
            )
    return rows


def recommendation_schema():
    """Spark schema of Gold ``recommendation_items``."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("kind", T.StringType(), False),
            T.StructField("context_id", T.StringType(), False),
            T.StructField("product_id", T.StringType(), False),
            T.StructField("rank", T.IntegerType(), False),
            T.StructField("score", T.DoubleType(), False),
            T.StructField("method", T.StringType(), False),
        ]
    )
