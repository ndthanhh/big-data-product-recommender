from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from gold_fixture import make_gold_fixture
from streamlit.testing.v1 import AppTest

from recommender.api.db import connect
from recommender.api.main import create_app
from recommender.pipeline.importer import import_snapshot
from recommender.ui.client import ApiClient, ApiError
from recommender.ui.state import StoreState


def test_user_session_cart_and_retry_uuid():
    state = StoreState("u1")
    session, cart = state.session_id, state.cart_id
    state.open_product("fixture:a")
    pending = list(state.pending)
    assert len(pending) == 2
    assert state.session_id == session and state.cart_id == cart
    state.change_user("u1")
    assert list(state.pending) == pending
    state.change_user("u2")
    assert state.session_id != session and state.cart_id != cart
    assert state.selected is None and not state.cart
    assert list(state.pending) == pending


def test_client_timeout_preserves_pending(monkeypatch):
    state = StoreState("u1")
    state.open_product("fixture:a")
    expected = list(state.pending)

    def failed(*args, **kwargs):
        raise ApiError("Kết nối thất bại")

    monkeypatch.setattr(ApiClient, "request", failed)
    with pytest.raises(ApiError):
        state.flush(ApiClient("http://127.0.0.1:1"))
    assert list(state.pending) == expected


def test_app_rerun_navigation_and_events(db_url, tmp_path, monkeypatch):
    import_snapshot(make_gold_fixture(tmp_path), db_url)
    tc = TestClient(create_app(db_url))

    def request(self, method, path, **kwargs):
        r = tc.request(method, path, **kwargs)
        if r.status_code >= 400:
            raise ApiError(r.json()["detail"]["message"])
        return r.json()

    monkeypatch.setattr(ApiClient, "request", request)
    app = AppTest.from_file(
        str(Path("src/recommender/ui/app.py").resolve()), default_timeout=20
    ).run()
    assert not app.exception

    def count():
        with connect(db_url) as conn:
            return conn.execute("SELECT count(*) FROM events").fetchone()[0]

    assert count() == 0
    app.button(key="catalog-fixture:a").click().run()
    assert not app.exception
    assert count() == 2
    app.run()
    assert count() == 2
    app.button(key="add-cart").click().run()
    assert count() == 3
    app.toggle[0].set_value(False).run()
    assert count() == 3
    app.text_input(key="user-id").set_value("new-user").run()
    assert not app.exception and count() == 3


def test_app_shows_api_error(monkeypatch):
    def failed(*args, **kwargs):
        raise ApiError("Chưa có snapshot phục vụ")

    monkeypatch.setattr(ApiClient, "request", failed)
    app = AppTest.from_file(str(Path("src/recommender/ui/app.py").resolve())).run()
    assert not app.exception
    assert any("snapshot" in element.value for element in app.error)


def test_permanent_rejection_does_not_block_other_events():
    state = StoreState("u1")
    state.open_product("fixture:removed")
    rejected = set(state.pending)
    state.change_user("u2")
    state.open_product("fixture:valid")
    valid = set(state.pending) - rejected

    class Client:
        def request(self, method, path, json):
            if json["event_id"] in rejected:
                raise ApiError(
                    "Product no longer exists",
                    status_code=404,
                    code="PRODUCT_NOT_FOUND",
                )
            return {"event_id": json["event_id"]}

    state.flush(Client())
    assert not state.pending
    assert set(state.failed_events) == rejected
    assert valid.isdisjoint(state.failed_events)


def test_clear_cart_starts_new_basket_preserving_queued_ids():
    state = StoreState("u1")
    state.open_product("fixture:a")
    state.add_to_cart()
    old_cart, session = state.cart_id, state.session_id
    state.clear_cart()
    state.open_product("fixture:b")
    state.add_to_cart()
    baskets = [
        row["cart_id"]
        for row in state.pending.values()
        if row["event_type"] == "add_to_cart"
    ]
    assert baskets == [old_cart, state.cart_id]
    assert old_cart != state.cart_id and state.session_id == session


def test_recommendation_metadata_reloads_group_after_snapshot_change(monkeypatch):
    from recommender.ui.client import SnapshotChanged

    client = ApiClient("http://example.test")
    loaded = []

    def fetch():
        run = "old" if not loaded else "new"
        loaded.append(run)
        return {"run_id": run, "items": [{"product_id": "fixture:a"}]}

    def product(product_id, *, run_id=None):
        if run_id == "old":
            raise SnapshotChanged("Changed snapshot")
        return {"product_id": product_id, "name": "New metadata"}

    monkeypatch.setattr(client, "product", product)
    response, products = client.recommendation_products(fetch)
    assert loaded == ["old", "new"] and response["run_id"] == "new"
    assert products[0][1]["name"] == "New metadata"
