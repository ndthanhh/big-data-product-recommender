from datetime import UTC, datetime

from recommender.ranking.complements import recommend_complements
from recommender.schemas import AssociationRule, Product


def products():
    return {
        f"fixture:{x}": Product(
            product_id=f"fixture:{x}",
            name=x,
            category_id="cable" if x == "c" else "keyboard",
            source="fixture",
            collected_at=datetime(2026, 1, 1, tzinfo=UTC),
            attributes={"connector": "usb-c"},
        )
        for x in "abc"
    }


def rule(antecedent, consequent, confidence=0.8, lift=1.2):
    return AssociationRule(
        run_id="run1",
        antecedent_ids=[f"fixture:{x}" for x in antecedent],
        consequent_id=f"fixture:{consequent}",
        support=0.5,
        confidence=confidence,
        lift=lift,
    )


def test_subset_dedup_and_cart_exclusion():
    result = recommend_complements(
        {"fixture:a", "fixture:b"},
        [rule("a", "c", 0.6), rule("ab", "c"), rule("a", "b")],
        products(),
        10,
    )
    assert [(r.product_id, r.score, r.method) for r in result] == [
        ("fixture:c", 0.8, "fpgrowth")
    ]


def test_missing_or_different_compatibility_rejected():
    catalog = products()
    pairs = {frozenset(("keyboard", "cable")): "connector"}
    assert recommend_complements(
        {"fixture:a"}, [rule("a", "c")], catalog, 10, compatibility=pairs
    )
    catalog["fixture:c"].attributes.clear()
    assert (
        recommend_complements(
            {"fixture:a"}, [rule("a", "c")], catalog, 10, compatibility=pairs
        )
        == []
    )
    catalog["fixture:c"].attributes["connector"] = "usb-a"
    assert (
        recommend_complements(
            {"fixture:a"}, [rule("a", "c")], catalog, 10, compatibility=pairs
        )
        == []
    )


def test_empty_cart_unknown_catalog_and_ties():
    assert recommend_complements(set(), [rule("a", "c")], products(), 10) == []
    result = recommend_complements(
        {"fixture:a"}, [rule("a", "c"), rule("a", "b")], products(), 10
    )
    assert [r.product_id for r in result] == ["fixture:b", "fixture:c"]
