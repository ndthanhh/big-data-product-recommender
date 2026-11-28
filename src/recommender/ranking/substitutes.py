"""T05 sản phẩm thay thế: hàng xóm cùng nhóm theo độ tương đồng cosine TF-IDF.

Gold có kind ``substitute``, method ``tfidf``, ngữ cảnh là sản phẩm gốc.
Quy tắc (DATA_CONTRACTS.md mục 4.2): cùng ``category_id``, loại chính sản
phẩm gốc, điểm hữu hạn trong ``[0, 1]``, nếu bằng điểm thì xếp
``product_id`` tăng dần, tối đa 200 dòng. Không xuất sản phẩm có độ tương đồng 0.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from recommender.ranking.common import CANDIDATE_LIMIT, Scored
from recommender.ranking.tfidf import TfidfFeatures, _top_rows


def build_substitutes(
    features: TfidfFeatures, *, limit: int = CANDIDATE_LIMIT
) -> dict[str, list[Scored]]:
    """Danh sách sản phẩm thay thế cho từng sản phẩm trong danh mục của lần chạy."""
    by_category: dict[str, list[int]] = defaultdict(list)
    for row, category in enumerate(features.categories):
        by_category[category].append(row)
    result: dict[str, list[Scored]] = {}
    for rows in by_category.values():
        block = features.matrix[rows]
        sims = block @ block.T
        ids = [features.product_ids[r] for r in rows]
        for local, pid in enumerate(ids):
            result[pid] = [
                Scored(other, score, "tfidf")
                for other, score in _top_rows(
                    np.clip(sims[local], 0.0, 1.0), ids, [local], limit
                )
            ]
    return result
