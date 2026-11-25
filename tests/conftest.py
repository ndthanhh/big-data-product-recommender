"""Database integration tests require an explicitly selected, disposable test DB."""

import os

import pytest


@pytest.fixture
def db_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated *_test database")
    from psycopg.conninfo import conninfo_to_dict

    if not conninfo_to_dict(url).get("dbname", "").endswith("_test"):
        raise ValueError("Integration tests require a database ending in _test")
    from recommender.api.db import connect
    from recommender.api.migrate import migrate

    migrate(url)
    with connect(url) as conn:
        conn.execute("UPDATE serving_state SET active_run_id=NULL")
        conn.execute(
            "TRUNCATE batch_runs, products, events, recommendation_items, seen_products, association_rules CASCADE"
        )
        conn.execute("INSERT INTO serving_state(singleton_id) VALUES(1)")
    return url
