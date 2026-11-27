"""Discover the immutable sync prefixes and exported demo batches without Spark."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from test_sync_minio import Store

from recommender.ingestion.bronze import write_bronze
from recommender.ingestion.sync_minio import download_batch, sync_bronze_to_minio
from recommender.pipeline.etl import bronze_inputs
from recommender.pipeline.lake import LocalLakeStore
from recommender.schemas import Event, Product


def records():
    product = Product.model_validate_json(
        Path("tests/fixtures/products.jsonl").read_text("utf-8").splitlines()[0]
    )
    event = Event(
        event_id=str(uuid4()),
        user_id="handoff-user",
        session_id="handoff-session",
        product_id=product.product_id,
        event_type="click",
        event_at=datetime.now(UTC),
        source="demo",
    )
    return product, event


def test_etl_reads_versioned_sync_prefixes(tmp_path):
    product, event = records()
    source = tmp_path / "source"
    for name, rows in [("products", [product]), ("clicks", [event])]:
        write_bronze(
            rows,
            source / name / (name + ".jsonl"),
            source="handoff",
            seed=42,
            parameters={},
            products=[product],
        )
    remote = Store()
    sync_bronze_to_minio(source, client=remote)
    lake = LocalLakeStore(tmp_path / "lake")
    for key, content in remote.objects.items():
        lake.write_bytes("bronze", key, data=content)
    inputs, skipped = bronze_inputs(lake)
    assert len(inputs) == 2 and not skipped
    assert {item["table"] for item in inputs} == {"products", "events"}


def test_etl_discovers_demo_export_and_ignores_hidden_staging(tmp_path):
    product, event = records()
    lake = LocalLakeStore(tmp_path / "lake")
    write_bronze(
        [product],
        lake.root / "bronze/products/products.jsonl",
        source="handoff",
        seed=42,
        parameters={},
        products=[product],
    )
    for folder in ("export-ready", ".export-staging"):
        write_bronze(
            [event],
            lake.root / "bronze/demo-events" / folder / "events.jsonl",
            source="demo-export",
            seed=None,
            parameters={},
            products=[product],
        )
    inputs, _ = bronze_inputs(lake)
    assert len(inputs) == 2
    assert any(item["path"].startswith("demo-events/export-ready/") for item in inputs)
    assert not any(".export-staging" in item["path"] for item in inputs)


def test_discovery_retains_all_accepted_manifest_provenance(tmp_path):
    from recommender.pipeline.etl import bronze_provenance

    product, event = records()
    source = tmp_path / "source"
    write_bronze(
        [product],
        source / "products/products.jsonl",
        source="catalog",
        seed=42,
        parameters={},
        products=[product],
    )
    for name, seed in [("one", 42), ("two", 43)]:
        write_bronze(
            [event],
            source / name / "events.jsonl",
            source="synthetic",
            seed=seed,
            parameters={"batch": name},
            products=[product],
            generator_version="synthetic-test-v1",
        )
    remote = Store()
    sync_bronze_to_minio(source, client=remote)
    lake = LocalLakeStore(tmp_path / "lake")
    for key, content in remote.objects.items():
        lake.write_bytes("bronze", key, data=content)
    inputs, _ = bronze_inputs(lake)
    metadata = bronze_provenance(inputs)
    assert len(metadata["bronze_dataset_versions"]["products"]) == 1
    assert len(metadata["bronze_dataset_versions"]["events"]) == 2
    assert metadata["seeds"] == [42, 43]
    assert metadata["generator_versions"] == ["synthetic-test-v1"]
    expected = __import__("json").loads(
        (source / "products/manifest.json").read_text("utf-8")
    )["catalog_version"]
    assert metadata["catalog_version"] == expected
    assert {p["batch"] for p in metadata["generator_parameters"]} == {"one", "two"}


def test_documented_typed_download_feeds_local_discovery(tmp_path):
    product, _ = records()
    write_bronze(
        [product],
        tmp_path / "source/products.jsonl",
        source="catalog",
        seed=42,
        parameters={},
        products=[product],
    )
    remote = Store()
    prefix = sync_bronze_to_minio(tmp_path / "source", client=remote)[0]
    lake = LocalLakeStore(tmp_path / "lake")
    download_batch(
        prefix, lake.root / "bronze/products/shared/catalog-v1", client=remote
    )
    inputs, skipped = bronze_inputs(lake)
    assert len(inputs) == 1 and not skipped
    assert inputs[0]["table"] == "products"
