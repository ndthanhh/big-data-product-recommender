"""Shared Spark fixtures: one local[2] UTC session per test run, offline."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def pytest_configure(config: pytest.Config) -> None:
    """Register the marker used by tests that start a local Spark session."""
    config.addinivalue_line("markers", "spark: needs PySpark and a JDK (local[2])")


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    """Small local Spark session; skips Spark tests when PySpark/Java is absent."""
    pytest.importorskip("pyspark")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from recommender.config import Settings
    from recommender.pipeline.lake import create_spark_session

    session = create_spark_session(
        "tests",
        Settings(data_lake_storage="local"),
        master="local[2]",
        shuffle_partitions=2,
    )
    yield session
    session.stop()


def publish_bronze(folder: Path) -> None:
    """Write the manifest that publishes every JSONL file in ``folder``."""
    files = []
    for path in sorted(folder.glob("*.jsonl")):
        data = path.read_bytes()
        files.append(
            {
                "path": path.name,
                "format": "jsonl",
                "rows": data.count(b"\n"),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    manifest = {"schema_version": 1, "files": files}
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture()
def lake(tmp_path: Path):
    """Empty local lake with fixture catalog/events copied into Bronze."""
    from recommender.pipeline.lake import LocalLakeStore

    store = LocalLakeStore(tmp_path / "data")
    for table, name in (("products", "products.jsonl"), ("events", "events.jsonl")):
        target = tmp_path / "data" / "bronze" / table
        target.mkdir(parents=True)
        (target / name).write_bytes((FIXTURES / name).read_bytes())
        publish_bronze(target)
    return store


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
