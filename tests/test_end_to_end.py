import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from gold_fixture import make_gold_fixture
from test_api import event

from recommender.api.main import create_app
from recommender.pipeline.export_events import export_demo_events
from recommender.schemas import Event


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, Path("scripts") / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_click_db_session_export_roundtrip(db_url, tmp_path):
    runner = load_script("run_demo")
    run = runner.prepare(make_gold_fixture(tmp_path), db_url)
    c = TestClient(create_app(db_url))
    assert c.get("/health").json() == {"status": "ok"}
    query = (
        "/api/v1/recommendations/personalized?user_id=u1&session_id=e2e-session&limit=2"
    )
    before = c.get(query).json()
    payload = event(session_id="e2e-session", event_at=datetime.now(UTC).isoformat())
    assert c.post("/api/v1/events", json=payload).status_code == 201
    after = c.get(query).json()
    assert before["run_id"] == after["run_id"] == run
    assert before["items"][0]["product_id"] != after["items"][0]["product_id"]
    assert (
        c.post(
            "/api/v1/recommendations/complements", json={"product_ids": ["fixture:a"]}
        ).json()["items"][0]["product_id"]
        == "fixture:d"
    )
    exported = export_demo_events(db_url, tmp_path / "export")
    row = Event.model_validate_json(
        (exported.parent / "events.jsonl").read_text("utf-8").strip()
    )
    assert row.event_id == payload["event_id"] and row.source == "demo"


def test_benchmark_measures_all_routes_and_writes_result(db_url, tmp_path):
    runner = load_script("run_demo")
    runner.prepare(make_gold_fixture(tmp_path), db_url)
    module = load_script("benchmark_api")
    with TestClient(create_app(db_url)) as client:
        result = module.benchmark(client, requests=4, concurrency=2, warmup=1)
    assert set(result["endpoints"]) == {
        "personalized",
        "substitutes",
        "complements",
        "events",
    }
    assert all(
        row["requests"] == 4
        and row["errors"] == 0
        and row["p95_ms"] >= row["p50_ms"] >= 0
        for row in result["endpoints"].values()
    )
    assert result["run_id"] == "fixture_run"
    output = tmp_path / "result.json"
    module.write_result(output, result)
    assert json.loads(output.read_text("utf-8"))["run_id"] == "fixture_run"


def test_runner_does_not_announce_inactive_ready_snapshot(db_url, tmp_path):
    import pytest

    from recommender.pipeline.importer import import_snapshot

    runner = load_script("run_demo")
    old = make_gold_fixture(tmp_path, "old")
    import_snapshot(old, db_url)
    import_snapshot(make_gold_fixture(tmp_path, "new"), db_url)
    with pytest.raises(ValueError, match="activate-run"):
        runner.prepare(old, db_url)
