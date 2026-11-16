"""T06: Implicit ALS (ID mapping, candidates), MMR re-ranking and FP-Growth."""

from __future__ import annotations

import math

import numpy as np
import pytest

from recommender.ranking.als import AlsParams, als_candidates, build_index, fit_als
from recommender.ranking.common import Scored
from recommender.ranking.fpgrowth import (
    FpGrowthParams,
    Rule,
    collect_rules,
    fit_fpgrowth,
    recommend_complements,
)
from recommender.ranking.mmr import MmrStats, min_max_normalize, mmr_rerank
from recommender.schemas import AssociationRule


@pytest.fixture(scope="module")
def interactions(spark):
    rows = []
    # Two taste groups: users a* like items x1..x3, users b* like y1..y3.
    for u in ("a1", "a2", "a3", "a4"):
        rows += [(u, f"t:x{i}", float(i)) for i in (1, 2, 3)]
    for u in ("b1", "b2", "b3", "b4"):
        rows += [(u, f"t:y{i}", float(i)) for i in (1, 2, 3)]
    rows.append(("a1", "t:x4", 4.0))
    rows.append(("a2", "t:x4", 4.0))
    return spark.createDataFrame(
        rows, "user_id string, product_id string, interaction_weight double"
    ).cache()


@pytest.mark.spark
def test_build_index_is_sorted_bijection(spark, interactions) -> None:
    index = build_index(interactions, "product_id", "product_index").collect()
    ids = [r["product_id"] for r in sorted(index, key=lambda r: r["product_index"])]
    assert ids == sorted(ids)
    assert [
        r["product_index"] for r in sorted(index, key=lambda r: r["product_index"])
    ] == list(range(len(ids)))


@pytest.mark.spark
def test_als_candidates_unseen_catalog_and_string_ids(spark, interactions) -> None:
    artifacts = fit_als(
        interactions, AlsParams(rank=4, max_iter=5, reg_param=0.05, alpha=10.0)
    )
    seen = interactions.select("user_id", "product_id")
    catalog = spark.createDataFrame(
        [(f"t:x{i}",) for i in (1, 2, 3, 4)] + [(f"t:y{i}",) for i in (1, 2)],
        "product_id string",
    )
    cands = als_candidates(artifacts, seen, catalog, limit=3)
    seen_map: dict[str, set[str]] = {}
    for r in seen.collect():
        seen_map.setdefault(r["user_id"], set()).add(r["product_id"])
    assert set(cands) <= set(seen_map)
    for user, items in cands.items():
        ids = [i.product_id for i in items]
        assert not set(ids) & seen_map[user]
        assert set(ids) <= {r["product_id"] for r in catalog.collect()}  # y3 excluded
        assert all(math.isfinite(i.score) and i.method == "als" for i in items)
        assert [(-i.score, i.product_id) for i in items] == sorted(
            (-i.score, i.product_id) for i in items
        )
    # a3 never saw x4 but its group did: x4 should be its best candidate.
    assert cands["a3"][0].product_id == "t:x4"


def test_min_max_normalization_rules() -> None:
    assert np.allclose(min_max_normalize([1.0, 3.0, 2.0]), [0.0, 1.0, 0.5])
    assert np.allclose(min_max_normalize([2.0, 2.0]), [1.0, 1.0])
    assert np.allclose(min_max_normalize([0.0, 0.0]), [0.0, 0.0])
    assert min_max_normalize([]).size == 0


def test_mmr_lambda_one_keeps_relevance_order_and_ties_by_id() -> None:
    cands = [
        Scored("p3", 0.9, "als"),
        Scored("p1", 0.5, "als"),
        Scored("p2", 0.5, "als"),
    ]
    vectors = {"p1": np.array([1.0, 0.0]), "p2": np.array([1.0, 0.0]), "p3": None}
    stats = MmrStats()
    ranked = mmr_rerank(cands, vectors, 1.0, stats=stats)
    assert [i.product_id for i in ranked] == ["p3", "p1", "p2"]
    assert all(i.method == "als_mmr" for i in ranked)
    assert stats.missing_vectors == 1 and stats.candidates == 3


def test_mmr_increases_diversity_and_keeps_selection_order() -> None:
    a, b = np.array([1.0, 0.0]), np.array([0.0, 1.0])
    cands = [
        Scored("a1", 1.0, "als"),
        Scored("a2", 0.95, "als"),
        Scored("a3", 0.9, "als"),
        Scored("b1", 0.6, "als"),
    ]
    vectors = {"a1": a, "a2": a, "a3": a, "b1": b}
    raw = [c.product_id for c in cands[:2]]
    reranked = mmr_rerank(cands, vectors, 0.5, limit=2)
    assert raw == ["a1", "a2"]
    assert [i.product_id for i in reranked] == ["a1", "b1"]
    # First pick has no penalty: 0.5 * norm(1.0) = 0.5.
    assert reranked[0].score == pytest.approx(0.5)
    # b1: 0.5 * norm(0.6) - 0.5 * 0 = 0.5 * 0 = 0 ; a2: 0.5*0.875 - 0.5*1 < 0
    assert reranked[1].score == pytest.approx(0.0)
    with pytest.raises(ValueError):
        mmr_rerank(cands, vectors, 1.5)


BASKETS = [
    ["p:a", "p:b"],
    ["p:a", "p:b"],
    ["p:a", "p:b", "p:c"],
    ["p:a", "p:c"],
    ["p:b", "p:d"],
    ["p:e"],  # single-item basket: stored, never fitted
]


@pytest.mark.spark
def test_fpgrowth_metrics_are_recomputed_per_consequent(spark) -> None:
    baskets = spark.createDataFrame(
        [(f"c{i}", items) for i, items in enumerate(BASKETS)],
        "cart_id string, product_ids array<string>",
    )
    result = fit_fpgrowth(
        baskets, FpGrowthParams(min_support=0.2, min_confidence=0.1), "run_t"
    )
    assert (
        result.baskets_total == 6
        and result.baskets_fit == 5
        and result.baskets_single_item == 1
    )
    rules = {
        (r.antecedent_ids, r.consequent_id): r for r in collect_rules(result.rules)
    }
    n = 5  # fitted baskets only
    ab = rules[(("p:a",), "p:b")]
    assert ab.support == pytest.approx(3 / n)
    assert ab.confidence == pytest.approx(3 / 4)  # a in 4 baskets, a&b in 3
    assert ab.lift == pytest.approx((3 / 4) / (4 / n))  # b in 4 baskets
    for r in result.rules.collect():
        AssociationRule(
            run_id=r["run_id"],
            antecedent_ids=list(r["antecedent_ids"]),
            consequent_id=r["consequent_id"],
            support=r["support"],
            confidence=r["confidence"],
            lift=r["lift"],
        )
        assert list(r["antecedent_ids"]) == sorted(set(r["antecedent_ids"]))


def test_complements_subset_match_order_and_never_in_cart() -> None:
    rules = [
        Rule(("p:a",), "p:b", 0.6, 0.75, 0.9),
        Rule(("p:a",), "p:c", 0.4, 0.5, 1.25),
        Rule(("p:a", "p:b"), "p:c", 0.2, 0.5, 1.5),  # same confidence, higher lift
        Rule(("p:b",), "p:a", 0.6, 0.75, 0.9),
        Rule(("p:z",), "p:d", 0.2, 0.9, 2.0),  # antecedent not in cart
    ]
    out = recommend_complements(rules, ["p:a", "p:b"], limit=10)
    assert [pid for pid, _, _ in out] == ["p:c"]  # a, b are in the cart
    assert out[0][2].antecedent_ids == ("p:a", "p:b")  # best lift wins the tie
    only_a = recommend_complements(rules, ["p:a"], limit=10)
    assert [pid for pid, _, _ in only_a] == ["p:b", "p:c"]
    assert recommend_complements(rules, [], limit=10) == []
    assert len(recommend_complements(rules, ["p:a"], limit=1)) == 1


def test_complements_compatibility_rule() -> None:
    catalog = {
        "p:kb": {"category_id": "keyboard", "attributes": {"connector": "usb-c"}},
        "p:c1": {"category_id": "cable", "attributes": {"connector": "usb-c"}},
        "p:c2": {"category_id": "cable", "attributes": {"connector": "micro-usb"}},
        "p:c3": {"category_id": "cable", "attributes": {}},
    }
    rules = [Rule(("p:kb",), c, 0.1, 0.5, 1.0) for c in ("p:c1", "p:c2", "p:c3")]
    compat = {frozenset({"keyboard", "cable"}): "connector"}
    out = recommend_complements(rules, ["p:kb"], catalog=catalog, compatibility=compat)
    assert [pid for pid, _, _ in out] == ["p:c1"]
    unrestricted = recommend_complements(
        rules, ["p:kb"], catalog=catalog, compatibility={}
    )
    assert len(unrestricted) == 3


@pytest.mark.spark
def test_als_reload_reproduces_candidates(spark, interactions, tmp_path) -> None:
    from recommender.ranking.als import load_als, save_als

    artifacts = fit_als(interactions, AlsParams(rank=4, max_iter=5))
    seen = interactions.select("user_id", "product_id")
    catalog = interactions.select("product_id").distinct()
    uris = [str(tmp_path / name) for name in ("als", "users", "products")]
    save_als(artifacts, *uris)
    reloaded = load_als(spark, *uris)

    def as_tuples(cands):
        return {
            u: [(i.product_id, round(i.score, 12)) for i in v] for u, v in cands.items()
        }

    before = as_tuples(als_candidates(artifacts, seen, catalog, limit=5))
    after = as_tuples(als_candidates(reloaded, seen, catalog, limit=5))
    assert before == after and before

    # A mapping that does not match the factors is rejected.
    interactions.where("user_id != 'b4'").select("user_id").distinct().orderBy(
        "user_id"
    ).rdd.zipWithIndex().map(lambda p: (p[0][0], int(p[1]))).toDF(
        ["user_id", "user_index"]
    ).write.mode("overwrite").parquet(uris[1])
    with pytest.raises(ValueError, match="user_index"):
        load_als(spark, *uris)
