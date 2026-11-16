"""T04: PySpark ETL Bronze -> Silver and Gold base aggregates (offline)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from recommender.pipeline.etl import (
    ConflictError,
    load_silver,
    manifest_name,
    run_etl,
    validate_event_line,
    validate_product_line,
)
from recommender.pipeline.gold import (
    build_baskets,
    build_interactions,
    build_seen_products,
    catalog_as_of,
)

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).parent / "fixtures"


def _event(n: int, **overrides: object) -> dict:
    base = {
        "event_id": f"00000000-0000-4000-8000-{n:012d}",
        "user_id": "u_etl",
        "session_id": "s_etl",
        "product_id": "fixture:p001",
        "event_type": "view",
        "event_at": "2026-10-20T00:00:00Z",
        "source": "synthetic",
        "cart_id": None,
    }
    base.update(overrides)
    return base


def _append(lake, table: str, lines: list[str]) -> None:
    path = lake.root / "bronze" / table / f"{table}.jsonl"
    with path.open("a", encoding="utf-8") as stream:
        for line in lines:
            stream.write(line + "\n")


def test_row_validators_classify_reasons() -> None:
    assert validate_event_line("{not json")["reason"] == "invalid_json"
    assert validate_event_line("")["reason"] == "invalid_json"
    assert validate_event_line("[1, 2]")["reason"] == "invalid_schema"
    bad_tz = json.dumps(_event(1, event_at="2026-10-20T00:00:00"))
    assert validate_event_line(bad_tz)["reason"] == "invalid_timestamp"
    extra = json.dumps({**_event(1), "interaction_weight": 3})
    assert validate_event_line(extra)["reason"] == "invalid_schema"
    no_cart = json.dumps(_event(1, event_type="add_to_cart"))
    assert validate_event_line(no_cart)["reason"] == "invalid_schema"
    ok = validate_event_line(
        json.dumps(_event(1, event_id=_event(1)["event_id"].upper()))
    )
    assert ok["reason"] is None
    assert ok["event_id"] == _event(1)["event_id"]  # UUID normalized to lowercase
    product = validate_product_line(
        '{"product_id":"x:1","name":"A","category_id":"Cat","source":"x",'
        '"collected_at":"2026-10-01T00:00:00Z"}'
    )
    assert product["reason"] == "invalid_schema"  # category_id must be lowercase


def test_fixture_etl_publishes_clean_silver(spark, lake) -> None:
    result = run_etl(spark, lake)
    assert not result.reused
    manifest = lake.read_json("silver", manifest_name(result.dataset_version))
    assert manifest["counts"]["events"] == 50
    assert manifest["counts"]["catalog_versions"] == 20
    assert manifest["schema_version"] == 1
    event_rows = sum(f["rows"] for f in manifest["files"] if f.get("table") == "events")
    assert event_rows == 50
    for entry in manifest["files"]:
        assert not entry["path"].startswith("/") and ".." not in entry["path"]
        assert len(entry["sha256"]) == 64
    assert lake.exists("silver", manifest["quality_report"])

    products, events, loaded = load_silver(spark, lake)
    assert loaded["dataset_version"] == result.dataset_version
    assert dict(events.dtypes)["event_at"] == "timestamp"
    assert dict(products.dtypes)["attributes"] == "map<string,string>"
    assert events.count() == 50

    again = run_etl(spark, lake)
    assert again.reused and again.dataset_version == result.dataset_version


def test_etl_rejects_and_reports_each_reason(spark, lake) -> None:
    good_line = (lake.root / "bronze/events/events.jsonl").read_text().splitlines()[0]
    lines = [
        "{broken",
        json.dumps({**_event(101), "extra": 1}),
        json.dumps(_event(102, event_at="2026-10-20T07:00:00+07:00")),
        good_line,  # identical duplicate -> merged
        json.dumps(_event(103, product_id="fixture:p999")),
        # session owned by two users
        json.dumps(_event(104, session_id="s_shared", user_id="u_a")),
        json.dumps(_event(105, session_id="s_shared", user_id="u_b")),
        # cart reused in two sessions of the same user
        json.dumps(
            _event(106, session_id="s_x", event_type="add_to_cart", cart_id="c_dup")
        ),
        json.dumps(
            _event(107, session_id="s_y", event_type="add_to_cart", cart_id="c_dup")
        ),
    ]
    _append(lake, "events", lines)
    result = run_etl(spark, lake)
    events = result.report["events"]
    assert events["accepted"] == 50
    assert events["duplicate"] == 1
    rejected = events["rejected"]
    assert rejected["invalid_json"] == 1
    assert rejected["invalid_schema"] == 1
    assert rejected["invalid_timestamp"] == 1
    assert rejected["unknown_product"] == 1
    assert rejected["invalid_cart_owner"] == 4
    assert events["input_rows"] == 50 + len(lines)
    quarantine = spark.read.parquet(
        lake.uri("silver", "quarantine", result.dataset_version, "events")
    )
    assert quarantine.count() == len(lines)


def test_conflicting_event_id_blocks_publication(spark, lake) -> None:
    first = _event(201)
    _append(
        lake,
        "events",
        [json.dumps(first), json.dumps({**first, "event_type": "click"})],
    )
    with pytest.raises(ConflictError) as info:
        run_etl(spark, lake)
    version = info.value.dataset_version
    assert info.value.report["events"]["rejected"]["conflicting_event_id"] == 2
    assert not lake.exists("silver", manifest_name(version))
    assert lake.exists("silver", "quality", f"{version}.json")


def test_product_versions_duplicates_and_conflicts(spark, lake) -> None:
    lines = (lake.root / "bronze/products/products.jsonl").read_text().splitlines()
    p1 = json.loads(lines[0])
    newer = {**p1, "collected_at": "2026-10-10T00:00:00Z", "name": p1["name"] + " v2"}
    _append(lake, "products", [lines[0], json.dumps(newer)])
    result = run_etl(spark, lake)
    report = result.report["products"]
    assert report["duplicate"] == 1 and report["accepted"] == 21
    products, _, _ = load_silver(spark, lake, result.dataset_version)

    early = catalog_as_of(products, datetime(2026, 10, 5, tzinfo=UTC), "r1")
    late = catalog_as_of(products, datetime(2026, 10, 11, tzinfo=UTC), "r1")
    name_early = early.where(f"product_id = '{p1['product_id']}'").first()["name"]
    name_late = late.where(f"product_id = '{p1['product_id']}'").first()["name"]
    assert name_early == p1["name"] and name_late.endswith(" v2")
    assert early.count() == late.count() == 20
    # A version collected exactly at the cutoff is not yet known.
    at_cutoff = catalog_as_of(products, datetime(2026, 10, 10, tzinfo=UTC), "r1")
    assert (
        at_cutoff.where(f"product_id = '{p1['product_id']}'").first()["name"]
        == p1["name"]
    )

    conflict = {**newer, "name": "different payload"}
    _append(lake, "products", [json.dumps(conflict)])
    with pytest.raises(ConflictError) as info:
        run_etl(spark, lake)
    assert info.value.report["products"]["rejected"]["conflicting_product_version"] == 2


def test_gold_base_weights_baskets_and_seen(spark, lake) -> None:
    lines = [
        json.dumps(
            _event(
                301, user_id="u_w", session_id="s_w", event_at="2026-10-21T00:00:00Z"
            )
        ),
        json.dumps(
            _event(
                302,
                user_id="u_w",
                session_id="s_w",
                event_type="click",
                event_at="2026-10-21T00:01:00Z",
            )
        ),
        json.dumps(
            _event(
                303,
                user_id="u_w",
                session_id="s_w",
                event_type="add_to_cart",
                cart_id="c_w",
                event_at="2026-10-21T00:02:00Z",
            )
        ),
        json.dumps(
            _event(
                304,
                user_id="u_w",
                session_id="s_w",
                product_id="fixture:p002",
                event_type="add_to_cart",
                cart_id="c_w",
                event_at="2026-10-21T00:03:00Z",
            )
        ),
        json.dumps(
            _event(
                305,
                user_id="u_w",
                session_id="s_w",
                product_id="fixture:p003",
                event_at="2026-10-30T00:00:00Z",
            )
        ),
    ]
    _append(lake, "events", lines)
    result = run_etl(spark, lake)
    products, events, _ = load_silver(spark, lake, result.dataset_version)
    start = datetime(2026, 10, 1, tzinfo=UTC)
    end = datetime(2026, 10, 25, tzinfo=UTC)
    catalog = catalog_as_of(products, end, "run_t")

    inter = {
        (r["user_id"], r["product_id"]): r
        for r in build_interactions(events, catalog, "run_t", start, end).collect()
    }
    row = inter[("u_w", "fixture:p001")]
    assert row["interaction_weight"] == 1 + 2 + 4
    assert row["run_id"] == "run_t"
    assert inter[("u_w", "fixture:p002")]["interaction_weight"] == 4
    assert ("u_w", "fixture:p003") not in inter  # after the window end

    basket = (
        build_baskets(events, catalog, "run_t", start, end)
        .where("cart_id = 'c_w'")
        .first()
    )
    assert basket["product_ids"] == ["fixture:p001", "fixture:p002"]
    assert basket["user_id"] == "u_w" and basket["session_id"] == "s_w"
    assert basket["first_event_at"] <= basket["last_event_at"]

    seen = {
        r["product_id"]
        for r in build_seen_products(events, catalog, "run_t", start, end)
        .where("user_id = 'u_w'")
        .collect()
    }
    assert seen == {"fixture:p001", "fixture:p002"}


def test_empty_inputs_and_unfinished_batch(spark, lake) -> None:
    events_file = lake.root / "bronze/events/events.jsonl"
    events_file.write_text("", encoding="utf-8")
    result = run_etl(spark, lake)
    assert result.manifest["counts"]["events"] == 0
    assert result.manifest["event_time_min"] is None
    assert result.report["events"]["input_rows"] == 0

    # A crashed batch leaves data files but no manifest: the rerun overwrites
    # them and publishes, while readers never saw the unfinished version.
    events_file.write_text(
        (FIXTURES / "events.jsonl").read_text(encoding="utf-8"), encoding="utf-8"
    )
    from recommender.pipeline.etl import bronze_inputs, dataset_version_for

    version = dataset_version_for(bronze_inputs(lake))
    stale = lake.root / "silver" / "events" / version
    stale.mkdir(parents=True)
    (stale / "part-stale.parquet").write_bytes(b"not parquet")
    with pytest.raises(FileNotFoundError):
        load_silver(spark, lake, version)
    published = run_etl(spark, lake)
    assert published.dataset_version == version and not published.reused
    assert not (stale / "part-stale.parquet").exists()
    assert load_silver(spark, lake, version)[1].count() == 50

    # No Bronze catalog at all is an error, not an empty release.
    (lake.root / "bronze/products/products.jsonl").rename(
        lake.root / "bronze/products/products.old"
    )
    with pytest.raises(FileNotFoundError):
        run_etl(spark, lake)
