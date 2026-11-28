"""T06 xếp hạng lại bằng Maximal Marginal Relevance (DATA_CONTRACTS.md mục 7.2).

Mỗi bước chọn ``argmax[lambda * norm(rel(i)) - (1 - lambda) * max_j cos(i, j)]``
trên các ứng viên còn lại; ``j`` là sản phẩm đã chọn (mức phạt bằng 0
ở lượt đầu). ``norm`` chuẩn hóa min-max trên danh sách ứng viên; các điểm
khác 0 bằng nhau thành 1, toàn bộ bằng 0 thành 0. Vector thiếu hoặc bằng
0 có cosine bằng 0. Thứ hạng là thứ tự chọn, không xếp lại theo điểm MMR;
nếu giá trị mục tiêu bằng nhau thì ưu tiên ID sản phẩm tăng dần.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np

from recommender.ranking.common import Scored

MMR_LAMBDAS = (0.3, 0.5, 0.7, 1.0)


def min_max_normalize(scores: Sequence[float]) -> np.ndarray:
    """Chuẩn hóa danh sách điểm hữu hạn theo hợp đồng dữ liệu."""
    values = np.asarray(scores, dtype=np.float64)
    if values.size == 0:
        return values
    low, high = values.min(), values.max()
    if high == low:
        return np.full_like(values, 0.0 if high == 0 else 1.0)
    return (values - low) / (high - low)


@dataclass(slots=True)
class MmrStats:
    """Thông tin chẩn đoán cho báo cáo: ứng viên thiếu vector dùng được."""

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
    """Xếp hạng lại ``candidates``; điểm trả về là giá trị mục tiêu tại thời điểm chọn."""
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

    # Xét theo ID tăng dần để argmax chọn ID nhỏ nhất khi bằng điểm.
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
