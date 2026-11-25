from fastapi.testclient import TestClient
from gold_fixture import make_gold_fixture, rewrite_table

from recommender.api.db import connect
from recommender.api.main import create_app
from recommender.pipeline.importer import import_snapshot


def client_for(db_url, tmp_path):
    import_snapshot(make_gold_fixture(tmp_path), db_url)
    return TestClient(create_app(db_url))


def test_personalized_filters_seen_and_appends_popularity(db_url, tmp_path):
    c = client_for(db_url, tmp_path)
    r = c.get("/api/v1/recommendations/personalized?user_id=u1&limit=4")
    assert r.status_code == 200
    rows = r.json()["items"]
    assert [x["product_id"] for x in rows] == [
        "fixture:b",
        "fixture:c",
        "fixture:d",
        "fixture:e",
    ]
    assert [x["method"] for x in rows] == ["tfidf", "tfidf", "popularity", "popularity"]
    assert [x["rank"] for x in rows] == [1, 2, 3, 4]
    assert (
        c.get("/api/v1/recommendations/personalized?user_id=new&limit=1").json()[
            "items"
        ][0]["product_id"]
        == "fixture:a"
    )


def test_substitutes_and_complements(db_url, tmp_path):
    c = client_for(db_url, tmp_path)
    assert (
        c.get("/api/v1/recommendations/substitutes?product_id=fixture:a").json()[
            "items"
        ][0]["product_id"]
        == "fixture:c"
    )
    assert (
        c.get("/api/v1/recommendations/substitutes?product_id=fixture:b").json()[
            "items"
        ]
        == []
    )
    for ids, expected in [
        ([], []),
        (["fixture:a", "fixture:a"], ["fixture:d"]),
        (["fixture:a", "fixture:d"], []),
    ]:
        r = c.post("/api/v1/recommendations/complements", json={"product_ids": ids})
        assert r.status_code == 200
        assert [x["product_id"] for x in r.json()["items"]] == expected
    assert (
        c.post(
            "/api/v1/recommendations/complements",
            json={"product_ids": ["fixture:a", "fixture:missing"]},
        ).status_code
        == 404
    )
    assert (
        c.get(
            "/api/v1/recommendations/substitutes?product_id=fixture:missing"
        ).status_code
        == 404
    )
    assert (
        c.get("/api/v1/recommendations/personalized?user_id=u1&limit=0").status_code
        == 422
    )


def test_request_keeps_same_snapshot_during_activation(db_url, tmp_path, monkeypatch):
    from recommender.api import recommendations

    c = client_for(db_url, tmp_path)
    second = make_gold_fixture(tmp_path, "new_run")
    rewrite_table(second, "products", lambda rows: rows[0].update(name="New catalog"))
    original = recommendations.product_rows

    def switch(conn, run):
        import_snapshot(second, db_url)
        return original(conn, run)

    monkeypatch.setattr(recommendations, "product_rows", switch)
    result = c.get("/api/v1/recommendations/personalized?user_id=new")
    assert result.status_code == 200 and result.json()["run_id"] == "fixture_run"
    with connect(db_url) as conn:
        assert (
            conn.execute("SELECT active_run_id FROM serving_state").fetchone()[0]
            == "new_run"
        )


def test_mmr_order_is_preserved(db_url, tmp_path):
    root = make_gold_fixture(tmp_path)

    def modify(rows):
        for row in rows:
            if row["kind"] == "personalized":
                row["method"] = "als_mmr"
                row["score"] = -2.0 if row["rank"] == 1 else 3.0

    rewrite_table(root, "recommendation_items", modify)
    import_snapshot(root, db_url)
    rows = (
        TestClient(create_app(db_url))
        .get("/api/v1/recommendations/personalized?user_id=u1&limit=2")
        .json()["items"]
    )
    assert [r["score"] for r in rows] == [-2.0, 3.0]


def test_click_affects_only_selected_session(db_url, tmp_path):
    from datetime import UTC, datetime

    from test_api import event

    c = client_for(db_url, tmp_path)
    url = "/api/v1/recommendations/personalized?user_id=u1&limit=2&session_id=s1"
    before = c.get(url).json()
    assert before["items"][0]["product_id"] == "fixture:b"
    assert (
        c.post(
            "/api/v1/events", json=event(event_at=datetime.now(UTC).isoformat())
        ).status_code
        == 201
    )
    after = c.get(url).json()
    assert after["run_id"] == before["run_id"]
    assert after["items"][0]["product_id"] == "fixture:c"
    assert (
        c.get(url.replace("s1", "s2")).json()["items"][0]["product_id"] == "fixture:b"
    )
    assert c.get(url.replace("user_id=u1", "user_id=u2")).status_code == 409
