import json

import pytest
from fastapi.testclient import TestClient
from gold_fixture import make_gold_fixture
from test_api import event

from recommender.api.db import connect
from recommender.api.main import create_app
from recommender.ingestion.sync_minio import validate_manifest
from recommender.pipeline.export_events import export_demo_events
from recommender.pipeline.importer import import_snapshot
from recommender.schemas import Event


def test_empty_export_has_valid_manifest(db_url, tmp_path):
    manifest = export_demo_events(db_url, tmp_path / "exports")
    m = validate_manifest(manifest.parent)
    assert m["counts"]["events"] == 0 and m["received_at_max"] is None


def test_full_replay_and_late_commit(db_url, tmp_path):
    import_snapshot(make_gold_fixture(tmp_path), db_url)
    c = TestClient(create_app(db_url))
    early = event(event_at="2020-01-01T00:00:00Z")
    assert c.post("/api/v1/events", json=early).status_code == 201
    late = event(event_at="2030-01-01T00:00:00Z")
    with connect(db_url) as pending:
        pending.execute(
            "INSERT INTO events(event_id,user_id,session_id,product_id,event_type,event_at,source,cart_id,catalog_run_id) VALUES (%s,'u1','s1','fixture:a','click',%s,'demo',NULL,'fixture_run')",
            (late["event_id"], late["event_at"]),
        )
        first = export_demo_events(db_url, tmp_path / "exports")
    second = export_demo_events(db_url, tmp_path / "exports")
    assert first != second
    first_m = validate_manifest(first.parent)
    second_m = validate_manifest(second.parent)
    assert [first_m["counts"]["events"], second_m["counts"]["events"]] == [1, 2]
    rows = [
        json.loads(line)
        for line in (second.parent / "events.jsonl").read_text("utf-8").splitlines()
    ]
    assert {r["event_id"] for r in rows} == {early["event_id"], late["event_id"]}
    assert all(set(r) == set(Event.model_fields) for r in rows)
    assert all(Event.model_validate(r) for r in rows)


def test_write_failure_preserves_database_and_previous_export(
    db_url, tmp_path, monkeypatch
):
    from recommender.pipeline import export_events

    previous = export_demo_events(db_url, tmp_path / "exports")

    def broken(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(export_events, "write_bronze", broken)
    with pytest.raises(OSError):
        export_demo_events(db_url, tmp_path / "exports")
    assert validate_manifest(previous.parent)["counts"]["events"] == 0
    assert list((tmp_path / "exports").glob("*/manifest.json")) == [previous]
