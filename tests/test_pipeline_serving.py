"""Bộ công bố ML thực tế phải bàn giao snapshot dùng được cho ứng dụng phục vụ."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from recommender.api.main import create_app
from recommender.pipeline.etl import load_silver, run_etl
from recommender.pipeline.export_events import export_demo_events
from recommender.pipeline.importer import import_snapshot
from recommender.pipeline.publish_snapshot import build_snapshot
from recommender.pipeline.snapshot import validate_snapshot
from recommender.schemas import Event


@pytest.mark.spark
def test_trained_gold_import_api_and_event_export(spark, lake, db_url, tmp_path):
    silver = run_etl(spark, lake)
    products, events, _ = load_silver(spark, lake)
    run = "integration_trained_gold"
    selection = {
        "als": {"rank": 4, "max_iter": 3, "reg_param": 0.1, "alpha": 1.0, "seed": 42},
        "mmr_lambda": 0.7,
        "fpgrowth": {"min_support": 0.1, "min_confidence": 0.2},
        "personalized_method": "als_mmr",
    }
    manifest = build_snapshot(
        spark,
        lake,
        products,
        events,
        dataset_version=silver.dataset_version,
        selection=selection,
        seed=42,
        training_start=datetime(2026, 10, 1, tzinfo=UTC),
        training_end=datetime(2026, 11, 8, tzinfo=UTC),
        run_id=run,
        log=lambda _: None,
    )
    from pathlib import Path

    root = Path(lake.uri("gold", run))
    validated = validate_snapshot(root)
    assert validated.manifest == manifest
    assert import_snapshot(root, db_url) == run
    assert import_snapshot(root, db_url) == run
    client = TestClient(create_app(db_url))
    catalog = client.get("/api/v1/products?limit=100").json()["items"]
    assert len(catalog) == manifest["counts"]["products"]
    first = catalog[0]["product_id"]
    response = client.get(
        "/api/v1/recommendations/personalized",
        params={"user_id": "integration-new-user"},
    )
    assert (
        response.status_code == 200
        and response.json()["run_id"] == run
        and response.json()["items"]
    )
    substitute = client.get(
        "/api/v1/recommendations/substitutes", params={"product_id": first}
    )
    assert substitute.status_code == 200 and substitute.json()["run_id"] == run
    rules = validated.tables["association_rules"]
    cart = rules[0]["antecedent_ids"] if rules else []
    complement = client.post(
        "/api/v1/recommendations/complements", json={"product_ids": cart, "limit": 50}
    )
    assert complement.status_code == 200 and complement.json()["run_id"] == run
    if rules:
        assert rules[0]["consequent_id"] in {
            row["product_id"] for row in complement.json()["items"]
        }
    payload = Event(
        event_id=str(uuid4()),
        user_id="integration-new-user",
        session_id="integration-session",
        product_id=first,
        event_type="click",
        event_at=datetime.now(UTC),
        source="demo",
    ).model_dump(mode="json")
    assert client.post("/api/v1/events", json=payload).status_code == 201
    assert client.post("/api/v1/events", json=payload).status_code == 200
    session = client.get(
        "/api/v1/recommendations/personalized",
        params={"user_id": payload["user_id"], "session_id": payload["session_id"]},
    )
    assert session.status_code == 200 and session.json()["run_id"] == run
    exported = export_demo_events(db_url, lake.root / "bronze" / "demo-events")
    saved = Event.model_validate_json(
        (exported.parent / "events.jsonl").read_text("utf-8").strip()
    )
    assert saved.event_id == payload["event_id"]
    export_demo_events(db_url, lake.root / "bronze" / "demo-events")
    replayed = run_etl(spark, lake)
    assert (
        replayed.manifest["counts"]["events"] == silver.manifest["counts"]["events"] + 1
    )
    assert replayed.report["events"]["duplicate"] == 1
