"""Các chỉ số top-K của T07 theo đúng EXPERIMENT_PLAN.md mục 5.

Lấy trung bình đều giữa các người dùng; giá trị tổng hợp không xác định
trả về ``None`` (ghi thành JSON null hoặc ô CSV trống với ``status=not_evaluable``).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence

import numpy as np


def recall_at_k(recommended: Sequence[str], relevant: set[str], k: int = 10) -> float:
    """``|set(L[:k]) & R| / |R|``; yêu cầu ``R`` không rỗng."""
    if not relevant:
        raise ValueError("Recall is undefined for an empty relevant set")
    return len(set(recommended[:k]) & relevant) / len(relevant)


def ndcg_at_k(recommended: Sequence[str], relevant: set[str], k: int = 10) -> float:
    """NDCG với nhãn liên quan nhị phân, ``IDCG`` tính trên ``min(k, |R|)`` vị trí lý tưởng."""
    if not relevant:
        raise ValueError("NDCG is undefined for an empty relevant set")
    dcg = sum(
        1.0 / math.log2(rank + 1)
        for rank, pid in enumerate(recommended[:k], start=1)
        if pid in relevant
    )
    idcg = sum(
        1.0 / math.log2(rank + 1) for rank in range(1, min(k, len(relevant)) + 1)
    )
    return dcg / idcg


def intra_list_diversity(
    recommended: Sequence[str], vectors: Mapping[str, np.ndarray | None]
) -> tuple[float | None, int]:
    """Trung bình ``1 - cos`` giữa các cặp vector hợp lệ; trả ``(None, 0)`` nếu không có cặp nào."""
    valid = []
    for pid in recommended:
        vec = vectors.get(pid)
        if vec is not None and np.any(vec):
            valid.append(vec / np.linalg.norm(vec))
    pairs = len(valid) * (len(valid) - 1) // 2
    if pairs == 0:
        return None, 0
    matrix = np.vstack(valid)
    sims = matrix @ matrix.T
    upper = sims[np.triu_indices(len(valid), k=1)]
    return float(np.mean(1.0 - upper)), pairs


def catalog_coverage(lists: Iterable[Sequence[str]], catalog_size: int) -> float | None:
    """Số phần tử trong hợp các danh sách chia ``|C_t|``; trả ``None`` nếu danh mục rỗng."""
    if catalog_size <= 0:
        return None
    union: set[str] = set()
    for items in lists:
        union.update(items)
    return len(union) / catalog_size


def mean_or_none(values: Sequence[float]) -> float | None:
    """Trung bình đều giữa các mẫu; trả ``None`` khi không có mẫu."""
    return float(sum(values) / len(values)) if values else None


def evaluate_lists(
    recommendations: Mapping[str, Sequence[str]],
    relevance: Mapping[str, set[str]],
    vectors: Mapping[str, np.ndarray | None],
    catalog_size: int,
    *,
    k: int = 10,
) -> dict[str, float | int | None]:
    """Recall/NDCG/ILD/độ bao phủ trên người dùng có tập sản phẩm liên quan không rỗng.

    Người dùng thuộc ``relevance`` nhưng không có gợi ý được tính là danh sách
    rỗng (độ chính xác 0), để mọi phương pháp được chấm trên cùng tập người dùng.
    """
    users = sorted(u for u, rel in relevance.items() if rel)
    recalls, ndcgs, ilds, lengths = [], [], [], []
    pairs = 0
    lists = []
    for user in users:
        items = list(recommendations.get(user, ()))[:k]
        lists.append(items)
        lengths.append(len(items))
        recalls.append(recall_at_k(items, relevance[user], k))
        ndcgs.append(ndcg_at_k(items, relevance[user], k))
        ild, n_pairs = intra_list_diversity(items, vectors)
        if ild is not None:
            ilds.append(ild)
            pairs += n_pairs
    return {
        "recall_at_10": mean_or_none(recalls),
        "ndcg_at_10": mean_or_none(ndcgs),
        "ild": mean_or_none(ilds),
        "coverage": catalog_coverage(lists, catalog_size) if users else None,
        "users": len(users),
        "labels": sum(len(relevance[u]) for u in users),
        "ild_users": len(ilds),
        "ild_pairs": pairs,
        "avg_list_length": mean_or_none(lengths),
    }
