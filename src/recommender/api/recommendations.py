"""Serving queries always receive an already pinned run and connection."""

from psycopg.rows import dict_row

from recommender.api.errors import error
from recommender.api.routes.products import product_rows
from recommender.ranking.complements import recommend_complements
from recommender.schemas import AssociationRule, Recommendation, RecommendationItem


def items(conn, run, kind, context):
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            "SELECT * FROM recommendation_items WHERE run_id=%s AND kind=%s AND context_id=%s ORDER BY rank",
            (run, kind, context),
        ).fetchall()
    return [RecommendationItem.model_validate(row) for row in rows]


def ranked(rows, blocked, catalog, limit):
    result = []
    used = set(blocked)
    for row in rows:
        if row.product_id in used or row.product_id not in catalog:
            continue
        used.add(row.product_id)
        result.append(
            Recommendation(
                product_id=row.product_id,
                rank=len(result) + 1,
                score=row.score,
                method=row.method,
            )
        )
        if len(result) >= limit:
            break
    return result


def personalized(conn, run, query):
    catalog = product_rows(conn, run)
    seen = {
        row[0]
        for row in conn.execute(
            "SELECT product_id FROM seen_products WHERE run_id=%s AND user_id=%s",
            (run, query.user_id),
        )
    }
    if (
        query.session_id
        and conn.execute(
            "SELECT 1 FROM events WHERE session_id=%s AND user_id<>%s LIMIT 1",
            (query.session_id, query.user_id),
        ).fetchone()
    ):
        raise error(409, "IDENTITY_CONFLICT", "Session đã thuộc user khác")
    batch = items(conn, run, "personalized", query.user_id)
    popular = items(conn, run, "popular", "global")
    return ranked(batch + popular, seen, catalog, query.limit)


def substitutes(conn, run, query):
    catalog = product_rows(conn, run)
    if query.product_id not in catalog:
        raise error(404, "PRODUCT_NOT_FOUND", f"Không có sản phẩm {query.product_id}")
    category = catalog[query.product_id].category_id
    eligible = {key: row for key, row in catalog.items() if row.category_id == category}
    return ranked(
        items(conn, run, "substitute", query.product_id),
        {query.product_id},
        eligible,
        query.limit,
    )


def complements(conn, run, query, compatibility=None):
    catalog = product_rows(conn, run)
    for product in query.product_ids:
        if product not in catalog:
            raise error(404, "PRODUCT_NOT_FOUND", f"Không có sản phẩm {product}")
    if not query.product_ids:
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            "SELECT * FROM association_rules WHERE run_id=%s AND antecedent_ids <@ %s::text[]",
            (run, query.product_ids),
        ).fetchall()
    return recommend_complements(
        set(query.product_ids),
        [AssociationRule.model_validate(row) for row in rows],
        catalog,
        query.limit,
        compatibility=compatibility,
    )
