"""Gợi ý theo sản phẩm cùng xuất hiện trong giỏ, với quy tắc tương thích được cấu hình rõ."""

from collections.abc import Mapping, Sequence

from recommender.schemas import AssociationRule, Product, Recommendation


def recommend_complements(
    cart_ids: set[str],
    rules: Sequence[AssociationRule],
    products: Mapping[str, Product],
    limit: int,
    *,
    compatibility: Mapping[frozenset[str], str] | None = None,
) -> list[Recommendation]:
    if not cart_ids:
        return []
    compatibility = compatibility or {}
    selected = {}
    for rule in rules:
        if (
            not set(rule.antecedent_ids) <= cart_ids
            or rule.consequent_id in cart_ids
            or rule.consequent_id not in products
        ):
            continue
        candidate = products[rule.consequent_id]
        valid = True
        for item in cart_ids:
            if item not in products:
                valid = False
                break
            base = products[item]
            attribute = compatibility.get(
                frozenset((base.category_id, candidate.category_id))
            )
            if attribute and (
                not base.attributes.get(attribute)
                or base.attributes.get(attribute) != candidate.attributes.get(attribute)
            ):
                valid = False
                break
        if not valid:
            continue
        key = (-rule.confidence, -rule.lift, tuple(rule.antecedent_ids))
        if rule.consequent_id not in selected or key < selected[rule.consequent_id][0]:
            selected[rule.consequent_id] = (key, rule)
    ordered = sorted(
        (value[1] for value in selected.values()),
        key=lambda row: (-row.confidence, -row.lift, row.consequent_id),
    )
    return [
        Recommendation(
            product_id=row.consequent_id,
            rank=n,
            score=row.confidence,
            method="fpgrowth",
        )
        for n, row in enumerate(ordered[:limit], 1)
    ]
