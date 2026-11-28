from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from gold_fixture import make_gold_fixture

from recommender.api.db import connect
from recommender.api.main import create_app
from recommender.pipeline.importer import import_snapshot


def event(**changes):
    return (
        dict(
            event_id=str(uuid4()),
            user_id="u1",
            session_id="s1",
            product_id="fixture:a",
            event_type="click",
            event_at="2026-11-25T10:00:00Z",
            source="demo",
            cart_id=None,
            **changes,
        )
        if not changes
        else {**event(), **changes}
    )


@pytest.fixture
def client(db_url, tmp_path):
    import_snapshot(make_gold_fixture(tmp_path), db_url)
    return TestClient(create_app(db_url))


def test_health_independent_of_database():
    assert TestClient(create_app("postgresql://invalid@127.0.0.1:1/missing")).get(
        "/health"
    ).json() == {"status": "ok"}


def test_no_snapshot_and_validation_order(db_url):
    c = TestClient(create_app(db_url))
    assert c.get("/api/v1/products").status_code == 503
    assert c.get("/api/v1/products?limit=0").status_code == 422


def test_catalog_pagination_and_errors(client):
    response = client.get("/api/v1/products?limit=2&offset=1")
    assert response.status_code == 200
    assert [row["product_id"] for row in response.json()["items"]] == [
        "fixture:b",
        "fixture:c",
    ]
    assert client.get("/api/v1/products?offset=100").json() == {"items": []}
    assert client.get("/api/v1/products/fixture:a").json()["name"] == "Product a"
    for url, status, code in [
        ("/api/v1/products/fixture:missing", 404, "PRODUCT_NOT_FOUND"),
        ("/api/v1/products?limit=101", 422, "VALIDATION_ERROR"),
    ]:
        r = client.get(url)
        assert (r.status_code, r.json()["detail"]["code"]) == (status, code)
        assert isinstance(r.json()["detail"]["fields"], list)


def test_event_idempotency_conflict_and_retry_after_switch(client, db_url, tmp_path):
    payload = event()
    first = client.post("/api/v1/events", json=payload)
    assert first.status_code == 201
    assert (
        client.post(
            "/api/v1/events",
            json={**payload, "event_at": payload["event_at"].replace("Z", "+00:00")},
        ).status_code
        == 200
    )
    assert (
        client.post(
            "/api/v1/events", json={**payload, "event_type": "view"}
        ).status_code
        == 409
    )
    import_snapshot(make_gold_fixture(tmp_path, "other_run"), db_url)
    with connect(db_url) as c:
        c.execute("UPDATE serving_state SET active_run_id=NULL")
    assert client.post("/api/v1/events", json=payload).status_code == 200
    with connect(db_url) as c:
        assert c.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_concurrent_same_uuid(client):
    payload = event()
    with ThreadPoolExecutor(2) as pool:
        statuses = list(
            pool.map(
                lambda _: client.post("/api/v1/events", json=payload).status_code,
                range(2),
            )
        )
    assert sorted(statuses) == [200, 201]


@pytest.mark.parametrize("cart", [False, True])
def test_concurrent_identity_ownership(client, cart):
    first = event(cart_id="cart1" if cart else None)
    second = event(
        user_id="u2",
        session_id="s2" if cart else "s1",
        cart_id="cart1" if cart else None,
    )
    with ThreadPoolExecutor(2) as pool:
        statuses = list(
            pool.map(
                lambda p: client.post("/api/v1/events", json=p).status_code,
                [first, second],
            )
        )
    assert sorted(statuses) == [201, 409]


def test_bad_events_and_missing_product(client):
    for changes, status in [
        ({"source": "synthetic"}, 422),
        ({"event_type": "add_to_cart"}, 422),
        ({"extra": 1}, 422),
        ({"product_id": "fixture:missing"}, 404),
    ]:
        assert (
            client.post("/api/v1/events", json=event(**changes)).status_code == status
        )


def test_commit_failure_is_503_and_retriable(client, monkeypatch, db_url):
    from recommender.api.routes import events

    @contextmanager
    def fail_commit(url):
        with connect(url) as conn:
            yield conn
            conn.rollback()
            raise psycopg.OperationalError("simulated commit failure")

    monkeypatch.setattr(events, "connect", fail_commit)
    payload = event()
    r = client.post("/api/v1/events", json=payload)
    assert (r.status_code, r.json()["detail"]["code"]) == (503, "DATABASE_UNAVAILABLE")
    with connect(db_url) as c:
        assert c.execute("SELECT count(*) FROM events").fetchone()[0] == 0


def test_slash_product_id_and_conditional_metadata_run(db_url, tmp_path):
    from gold_fixture import rewrite_table

    root = make_gold_fixture(tmp_path)
    rewrite_table(
        root, "products", lambda rows: rows[4].update(product_id="fixture:e/slash")
    )
    rewrite_table(
        root,
        "recommendation_items",
        lambda rows: [
            row.update(product_id="fixture:e/slash")
            for row in rows
            if row["product_id"] == "fixture:e"
        ],
    )
    import_snapshot(root, db_url)
    c = TestClient(create_app(db_url))
    r = c.get("/api/v1/products/fixture%3Ae%2Fslash")
    assert r.status_code == 200 and r.json()["product_id"] == "fixture:e/slash"


def test_conditional_metadata_rejects_surviving_id_from_new_run(
    client, db_url, tmp_path
):
    from gold_fixture import rewrite_table

    c = client
    second = make_gold_fixture(tmp_path, "next")
    rewrite_table(second, "products", lambda rows: rows[0].update(name="New name"))
    import_snapshot(second, db_url)
    r = c.get(
        "/api/v1/products/fixture:a", headers={"X-Expected-Run-ID": "fixture_run"}
    )
    assert r.status_code == 503 and r.headers["X-Active-Run-ID"] == "next"
