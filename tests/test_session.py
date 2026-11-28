from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from recommender.ranking.session import normalize, rerank_session, session_scores
from recommender.schemas import Event, RecommendationItem

NOW = datetime(2026, 11, 25, 12, tzinfo=UTC)


def item(product, score=0.8, kind="personalized", context="u1", method="tfidf", rank=1):
    return RecommendationItem(
        run_id="run1",
        kind=kind,
        context_id=context,
        product_id=f"fixture:{product}",
        score=score,
        method=method,
        rank=rank,
    )


def event(age=0, kind="click", product="a"):
    return Event(
        event_id=str(uuid4()),
        user_id="u1",
        session_id="s1",
        product_id=f"fixture:{product}",
        event_type=kind,
        event_at=NOW - timedelta(minutes=age),
        source="demo",
        cart_id="cart1" if kind == "add_to_cart" else None,
    )


def test_weights_decay_and_window():
    sim = [item("c", 1.0, "substitute", "fixture:a")]
    for kind, weight in [("view", 1), ("click", 2), ("add_to_cart", 4)]:
        assert session_scores([event(10, kind)], sim, NOW)[
            "fixture:c"
        ] == pytest.approx(weight / 2)
    assert session_scores([event(31), event(-1)], sim, NOW) == {}
    assert session_scores([event(0)] * 21, sim, NOW)["fixture:c"] == 40


def test_normalization_equal_zero_and_missing_sources():
    assert normalize({"a": 0.0, "b": 0.0}) == {"a": 0.0, "b": 0.0}
    assert normalize({"a": -2.0, "b": -2.0}) == {"a": 1.0, "b": 1.0}
    assert normalize({"a": 2.0, "b": 4.0}) == {"a": 0.0, "b": 1.0}


def test_session_changes_order_and_excludes_seen():
    result = rerank_session(
        [item("b"), item("c", rank=2)],
        [],
        [event()],
        [item("c", 1.0, "substitute", "fixture:a")],
        set(),
        {f"fixture:{x}" for x in "abc"},
        NOW,
        10,
    )
    assert [(x.product_id, x.method) for x in result] == [
        ("fixture:c", "session"),
        ("fixture:b", "session"),
    ]
    assert [x.score for x in result] == [1.0, 0.7]


def test_popularity_fallback_does_not_change_primary_scale():
    batch = [item("b"), item("d", 1000.0, method="popularity", rank=2)]
    popular = [item("d", 1000.0, "popular", "global", "popularity")]
    catalog = {f"fixture:{x}" for x in "abcd"}
    rows = rerank_session(
        batch,
        popular,
        [event()],
        [item("c", 1.0, "substitute", "fixture:a")],
        set(),
        catalog,
        NOW,
        10,
    )
    assert [x.product_id for x in rows] == ["fixture:b", "fixture:c", "fixture:d"]
    assert [x.method for x in rows] == ["session", "session", "popularity"]
    fallback = rerank_session(batch, popular, [event(31)], [], set(), catalog, NOW, 10)
    assert [x.method for x in fallback] == ["tfidf", "popularity"]
