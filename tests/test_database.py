"""Exercise PostgreSQL constraints with actual writes."""

import pytest
from psycopg import IntegrityError

from recommender.api.db import connect
from recommender.api.migrate import migrate


def seed_run(conn, run="run1"):
    conn.execute(
        """INSERT INTO batch_runs(run_id,dataset_version,schema_version,
        manifest_sha256,status,training_start,training_end,seed,code_commit,
        parameters,environment,row_counts) VALUES (%s,'fixture',1,%s,'ready',
        '2026-01-01Z','2026-01-02Z',42,'fixture','{}','{}','{}')""",
        (run, "a" * 64),
    )
    for product in ("fixture:a", "fixture:b"):
        conn.execute(
            """INSERT INTO products(run_id,product_id,name,category_id,source,collected_at)
            VALUES (%s,%s,'Product','accessories','fixture','2026-01-01Z')""",
            (run, product),
        )


def test_migration_is_repeatable_and_utc(db_url):
    migrate(db_url)
    with connect(db_url) as conn:
        assert conn.execute("SHOW timezone").fetchone()[0] == "UTC"
        assert conn.execute("SELECT active_run_id FROM serving_state").fetchall() == [
            (None,)
        ]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname='public'"
            )
        }
        assert {
            "products",
            "batch_runs",
            "serving_state",
            "events",
            "seen_products",
            "recommendation_items",
            "association_rules",
        } <= tables
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 1


@pytest.mark.parametrize("score", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_scores_rejected(db_url, score):
    with connect(db_url) as conn:
        seed_run(conn)
        with pytest.raises(IntegrityError), conn.transaction():
            conn.execute(
                "INSERT INTO recommendation_items VALUES ('run1','popular','global','fixture:a',1,%s,'popularity')",
                (score,),
            )


@pytest.mark.parametrize(
    "antecedent", [["fixture:b", "fixture:a"], ["fixture:a", "fixture:a"], [], [None]]
)
def test_invalid_antecedents_rejected(db_url, antecedent):
    with connect(db_url) as conn:
        seed_run(conn)
        with pytest.raises(IntegrityError), conn.transaction():
            conn.execute(
                "INSERT INTO association_rules VALUES ('run1',%s,'fixture:b',0.5,0.5,1.0)",
                (antecedent,),
            )


def test_rank_unique_and_composite_catalog_fk(db_url):
    with connect(db_url) as conn:
        seed_run(conn)
        conn.execute(
            "INSERT INTO recommendation_items VALUES ('run1','popular','global','fixture:a',1,2.0,'popularity')"
        )
        with pytest.raises(IntegrityError), conn.transaction():
            conn.execute(
                "INSERT INTO recommendation_items VALUES ('run1','popular','global','fixture:b',1,1.0,'popularity')"
            )
        with pytest.raises(IntegrityError), conn.transaction():
            conn.execute(
                "INSERT INTO seen_products VALUES ('run1','u1','fixture:missing')"
            )
