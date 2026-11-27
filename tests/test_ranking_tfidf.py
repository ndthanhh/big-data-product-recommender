"""T05: Popularity baseline, TF-IDF profiles and same-category substitutes."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from recommender.ranking.common import Scored, recommendation_rows, top_k, with_fallback
from recommender.ranking.popularity import popular_list, rank_popular, recommend_popular
from recommender.ranking.substitutes import build_substitutes
from recommender.ranking.tfidf import (
    TfidfFeatures,
    feature_rows,
    features_from_rows,
    fit_tfidf,
    normalize_text,
    recommend_tfidf,
    user_profiles,
)
from recommender.schemas import RecommendationItem, validate_recommendation_items

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def catalog(spark):
    from recommender.pipeline.etl import silver_product_schema, validate_product_line

    rows = []
    for line in (FIXTURES / "products.jsonl").read_text(encoding="utf-8").splitlines():
        data = validate_product_line(line)
        rows.append(tuple(data[f.name] for f in silver_product_schema().fields))
    return spark.createDataFrame(rows, silver_product_schema()).cache()


@pytest.fixture(scope="module")
def features(catalog) -> TfidfFeatures:
    return fit_tfidf(catalog)[1]


def _interactions(spark, rows):
    return spark.createDataFrame(
        rows, "user_id string, product_id string, interaction_weight double"
    )


def test_top_k_is_deterministic_and_filters() -> None:
    scores = [("b", 1.0), ("a", 1.0), ("c", 2.0), ("d", float("nan")), ("e", 0.5)]
    assert top_k(scores, 3) == [("c", 2.0), ("a", 1.0), ("b", 1.0)]
    assert top_k(scores, 2, exclude={"c"}) == [("a", 1.0), ("b", 1.0)]
    assert top_k(scores, 5, allowed={"e", "b"}) == [("b", 1.0), ("e", 0.5)]


def test_popularity_sum_and_tie_break(spark, catalog) -> None:
    inter = _interactions(
        spark,
        [
            ("u1", "fixture:p002", 4.0),
            ("u2", "fixture:p002", 1.0),
            ("u1", "fixture:p001", 3.0),
            ("u2", "fixture:p001", 2.0),
            ("u3", "fixture:p010", 7.0),
            ("u3", "fixture:p999", 50.0),  # not in catalog
        ],
    )
    ranked = rank_popular(inter, catalog)
    assert ranked == [
        ("fixture:p010", 7.0),
        ("fixture:p001", 5.0),
        ("fixture:p002", 5.0),
    ]
    unseen = recommend_popular(ranked, {"fixture:p010"}, 1)
    assert [i.product_id for i in unseen] == ["fixture:p001"]  # filter before taking k
    rows = recommendation_rows("run_t", "popular", {"global": popular_list(ranked)})
    items = [
        RecommendationItem(
            run_id=r[0],
            kind=r[1],
            context_id=r[2],
            product_id=r[3],
            rank=r[4],
            score=r[5],
            method=r[6],
        )
        for r in rows
    ]
    validate_recommendation_items(items)
    assert [i.rank for i in items] == [1, 2, 3]


def test_tfidf_vectors_are_normalized(features) -> None:
    norms = np.linalg.norm(features.matrix, axis=1)
    assert np.allclose(norms, 1.0)
    assert features.zero_vectors == 0
    assert features.product_ids == sorted(features.product_ids)
    assert normalize_text("Bàn PHÍM", "keyboard", None) == "bàn phím keyboard "
    rebuilt = features_from_rows(feature_rows(features))
    assert rebuilt.product_ids == features.product_ids
    assert np.allclose(rebuilt.matrix, features.matrix)


def test_substitutes_same_category_without_self(features) -> None:
    subs = build_substitutes(features)
    category = dict(zip(features.product_ids, features.categories, strict=True))
    assert set(subs) == set(features.product_ids)
    for pid, items in subs.items():
        assert items, pid
        assert all(category[i.product_id] == category[pid] for i in items)
        assert pid not in {i.product_id for i in items}
        assert all(0 < i.score <= 1 and i.method == "tfidf" for i in items)
        keys = [(-i.score, i.product_id) for i in items]
        assert keys == sorted(keys)
    rows = recommendation_rows("run_t", "substitute", subs)
    validate_recommendation_items(
        [
            RecommendationItem(
                run_id=r[0],
                kind=r[1],
                context_id=r[2],
                product_id=r[3],
                rank=r[4],
                score=r[5],
                method=r[6],
            )
            for r in rows
        ]
    )
    # The 75% and 65% USB-C keyboards share more words than the Bluetooth one.
    first = [i.product_id for i in subs["fixture:p001"]]
    assert first.index("fixture:p003") < first.index("fixture:p005")


def test_user_profile_prefers_interacted_category(features) -> None:
    interactions = [("u_kb", "fixture:p001", 7.0), ("u_kb", "fixture:p002", 4.0)]
    users, profiles = user_profiles(
        features, interactions + [("u_none", "x:missing", 1.0)]
    )
    assert users == ["u_kb"]  # empty profiles are omitted -> fallback
    assert np.isclose(np.linalg.norm(profiles[0]), 1.0)
    seen = {"u_kb": {"fixture:p001", "fixture:p002"}}
    recs = recommend_tfidf(features, interactions, seen, limit=3)["u_kb"]
    category = dict(zip(features.product_ids, features.categories, strict=True))
    assert all(category[i.product_id] == "keyboard" for i in recs)
    assert not {i.product_id for i in recs} & seen["u_kb"]


def test_fallback_keeps_primary_order_then_popularity() -> None:
    primary = [Scored("p3", -0.5, "als"), Scored("p1", -0.1, "als")]
    popular = [("p1", 9.0), ("p2", 8.0), ("p4", 7.0)]
    result = with_fallback(primary, popular, seen={"p2"}, limit=3)
    assert [(i.product_id, i.method) for i in result] == [
        ("p3", "als"),
        ("p1", "als"),
        ("p4", "popularity"),
    ]
