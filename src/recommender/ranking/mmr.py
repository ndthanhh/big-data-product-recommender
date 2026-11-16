"""T06 Maximal Marginal Relevance re-ranking (DATA_CONTRACTS.md section 7.2).

At each step pick ``argmax[lambda * norm(rel(i)) - (1 - lambda) * max_j cos(i, j)]``
over the remaining candidates, ``j`` ranging over the already selected items
(penalty 0 for the first pick). ``norm`` is min-max over the candidate list;
equal non-zero scores normalize to 1, all-zero to 0. Missing or zero vectors
have cosine 0. Rank is the selection order and is never re-sorted by the MMR
score; objective ties break by product ID ascending.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np

from recommender.ranking.common import Scored

MMR_LAMBDAS = (0.3, 0.5, 0.7, 1.0)


def min_max_normalize(scores: Sequence[float]) -> np.ndarray:
    """Contract normalization of a list of finite scores."""
    values = np.asarray(scores, dtype=np.float64)
    if values.size == 0:
        return values
    low, high = values.min(), values.max()
    if high == low:
        return np.full_like(values, 0.0 if high == 0 else 1.0)
    return (values - low) / (high - low)


@dataclass(slots=True)
class MmrStats:
    """Diagnostics for reports: candidates lacking a usable vector."""

    lists: int = 0
    candidates: int = 0
    missing_vectors: int = 0


def mmr_rerank(
    candidates: Sequence[Scored],
    vectors: Mapping[str, np.ndarray | None],
    lam: float,
    *,
    limit: int | None = None,
    stats: MmrStats | None = None,
) -> list[Scored]:
    """Re-rank ``candidates``; returned scores are the objective at selection."""
    if not 0 <= lam <= 1:
        raise ValueError("lambda must lie in [0, 1]")
    n = len(candidates)
    limit = n if limit is None else min(limit, n)
    if n == 0 or limit == 0:
        return []
    ids = [c.product_id for c in candidates]
    relevance = min_max_normalize([c.score for c in candidates])
    width = next((v.shape[0] for v in vectors.values() if v is not None), 1)
    matrix = np.zeros((n, width), dtype=np.float64)
    missing = 0
    for i, pid in enumerate(ids):
        vec = vectors.get(pid)
        if vec is None or not np.any(vec):
            missing += 1
            continue
        norm = np.linalg.norm(vec)
        matrix[i] = vec / norm
    if stats is not None:
        stats.lists += 1
        stats.candidates += n
        stats.missing_vectors += missing

    # Tie-break by product ID: evaluate in ID order so argmax picks the smallest.
    order = np.argsort(np.asarray(ids, dtype=object), kind="stable")
    rel = relevance[order]
    mat = matrix[order]
    sorted_ids = [ids[i] for i in order]
    max_sim = np.zeros(n, dtype=np.float64)
    available = np.ones(n, dtype=bool)
    selected: list[Scored] = []
    for step in range(limit):
        penalty = max_sim if step else 0.0
        objective = lam * rel - (1 - lam) * penalty
        objective = np.where(available, objective, -np.inf)
        best = int(np.argmax(objective))
        selected.append(Scored(sorted_ids[best], float(objective[best]), "als_mmr"))
        available[best] = False
        max_sim = np.maximum(max_sim, mat @ mat[best])
    return selected
