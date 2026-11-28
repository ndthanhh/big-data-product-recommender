"""Truy vấn phục vụ luôn nhận kết nối và mã lần chạy đã được cố định từ trước."""

from datetime import UTC, datetime, timedelta

from psycopg.rows import dict_row

from recommender.api.errors import error
from recommender.api.routes.products import product_rows
from recommender.ranking.complements import recommend_complements
from recommender.ranking.session import rerank_session
from recommender.schemas import (
    AssociationRule,
    Event,
    Recommendation,
    RecommendationItem,
)


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
    if query.session_id:
        now = datetime.now(UTC)
        args = (query.user_id, query.session_id, now - timedelta(minutes=30), now)
        # Loại mọi sản phẩm trong cửa sổ thời gian, dù chỉ dùng 20 sự kiện để tính điểm.
        seen.update(
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT product_id FROM events WHERE user_id=%s AND session_id=%s AND event_at BETWEEN %s AND %s",
                args,
            )
        )
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT event_id,user_id,session_id,product_id,event_type,event_at,source,cart_id FROM events WHERE user_id=%s AND session_id=%s AND event_at BETWEEN %s AND %s ORDER BY event_at DESC,received_at DESC,event_id ASC LIMIT 20",
                args,
            ).fetchall()
        recent = [
            Event.model_validate(dict(row, event_id=str(row["event_id"])))
            for row in rows
        ]
        similarities = []
        for product in sorted({event.product_id for event in recent}):
            similarities.extend(items(conn, run, "substitute", product))
        return rerank_session(
            batch, popular, recent, similarities, seen, set(catalog), now, query.limit
        )
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
