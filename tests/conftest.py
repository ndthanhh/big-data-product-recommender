"""Shared Spark fixtures: one local[2] UTC session per test run, offline."""

from __future__ import annotations

import hashlib
import json
import os
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
