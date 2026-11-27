"""T05 substitutes: same-category TF-IDF cosine neighbours of a product.

Gold kind ``substitute``, method ``tfidf``, context = source product. Rules
(DATA_CONTRACTS.md section 4.2): same ``category_id``, never the product
itself, score finite in ``[0, 1]``, ties by ``product_id`` ascending, at most
200 rows. Items with zero similarity are not emitted.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from recommender.ranking.common import CANDIDATE_LIMIT, Scored
from recommender.ranking.tfidf import TfidfFeatures, _top_rows


def build_substitutes(
    features: TfidfFeatures, *, limit: int = CANDIDATE_LIMIT
) -> dict[str, list[Scored]]:
    """Substitute lists for every product in the run catalog."""
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
