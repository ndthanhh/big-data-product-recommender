"""Kiểm tra hợp đồng, hành vi và CLI ngoại tuyến cho phần thu thập T03."""

import hashlib
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from uuid import UUID

import pytest

from recommender.ingestion import catalog, synthetic
from recommender.schemas import Event, Product

CATEGORY_NAMES = [
    "Computers & Tablets",
    "TV & Home Theater",
    "Audio",
    "Cell Phones",
    "Cameras & Camcorders",
    "Video Games",
    "Appliances",
    "Car Electronics & GPS",
    "Connected Home & Housewares",
    "Wearable Technology",
    "Musical Instruments",
    "Toys, Games & Drones",
]


def raw_product(sku: int, category: str = "Computers & Tablets") -> dict:
    subcategory = {
        "Connected Home & Housewares": "Connected Home",
        "Musical Instruments": "Recording Equipment",
        "Toys, Games & Drones": "Drones & Accessories",
    }.get(category, "Accessories")
    return {
        "sku": sku,
        "name": f"  Example technology {sku}  ",
        "type": "HardGood",
        "price": 129.99,
        "upc": f"000000{sku:06}",
        "category": [
            {"id": "root", "name": category},
            {"id": "leaf", "name": subcategory},
        ],
        "shipping": 0,
        "description": "  A useful electronic device.  ",
        "manufacturer": "Example",
        "model": "M-1",
        "url": f"http://www.bestbuy.com/site/{sku}.p",
        "image": f"http://img.bbystatic.com/{sku}.jpg",
    }


@pytest.fixture
def raw_catalog() -> list[dict]:
    return [
        raw_product(10000 + index, CATEGORY_NAMES[index % 12]) for index in range(1200)
    ]


@pytest.fixture
def catalog_path(tmp_path: Path, raw_catalog: list[dict]) -> Path:
    source = tmp_path / "source.json"
    source.write_text(json.dumps(raw_catalog), encoding="utf-8")
    return catalog.ingest_catalog(
        source=source,
        output=tmp_path / "products" / "products.jsonl",
        limit=120,
        seed=42,
        cache_path=tmp_path / "cache.json",
    )


def assert_manifest(path: Path, source: str, rows: int) -> dict:
    manifest = json.loads((path.parent / "manifest.json").read_text("utf-8"))
    required = {
        "schema_version",
        "dataset_version",
        "created_at",
        "code_commit",
        "code_dirty",
        "generator_version",
        "catalog_version",
        "seed",
        "parameters",
        "counts",
        "event_time_min",
        "event_time_max",
        "files",
        "source",
        "collected_at",
        "record_count",
        "checksum_sha256",
    }
    assert required <= manifest.keys()
    assert manifest["schema_version"] == 1
    assert manifest["source"] == source
    assert manifest["record_count"] == rows
    assert manifest["created_at"].endswith("Z")
    assert manifest["collected_at"].endswith("Z")
    assert len(manifest["code_commit"]) == 40
    assert isinstance(manifest["code_dirty"], bool)
    assert len(manifest["files"]) == 1
    entry = manifest["files"][0]
    assert entry["path"] == path.name
    assert entry["format"] == "jsonl"
    assert entry["rows"] == rows
    data = path.read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    assert entry["bytes"] == len(data)
    assert entry["sha256"] == manifest["checksum_sha256"] == checksum
    assert b"\r" not in data
    assert all(line.strip() for line in data.splitlines())
    return manifest


def test_catalog_normalization_preserves_metadata_in_strict_schema() -> None:
    product = catalog.normalize_product(raw_product(123))
    assert Product.model_validate_json(product.model_dump_json()) == product
    assert product.product_id == "bestbuy:123"
    assert product.name == "Example technology 123"
    assert product.category_id == "computers_tablets"
    assert product.source == "bestbuy"
    assert product.collected_at.utcoffset() == timedelta(0)
    assert product.image_url == "https://img.bbystatic.com/123.jpg"
    assert product.product_url == "https://www.bestbuy.com/site/123.p"
    assert product.description == "A useful electronic device."
    assert product.brand == "Example"
    assert product.attributes["price"] == "129.99"
    assert product.attributes["is_active"] == "true"
    assert product.attributes["category"] == "Computers & Tablets"
    assert json.loads(product.attributes["tags"]) == ["Example", "M-1", "Accessories"]


@pytest.mark.parametrize(
    "change",
    [
        {"sku": None},
        {"sku": True},
        {"sku": "123 456"},
        {"name": " "},
        {"name": "x" * 513},
        {"price": -1},
        {"price": 0},
        {"price": float("nan")},
        {"price": float("inf")},
        {"price": True},
        {"image": ""},
        {"image": "ftp://example.com/a.jpg"},
        {"category": []},
        {"category": [{"name": "Movies & Music"}]},
        {"type": "Service"},
    ],
)
def test_catalog_rejects_unclean_products(change: dict) -> None:
    with pytest.raises(ValueError):
        catalog.normalize_product(raw_product(1) | change)


@pytest.mark.parametrize(
    "change,attributes",
    [
        ({"product_id": "bestbuy:not-a-sku"}, {}),
        (
            {"category_id": "musical_instruments"},
            {"category": "Office Furniture & Storage"},
        ),
        ({}, {"category": "Audio"}),
        ({}, {"tags": "null"}),
        ({}, {"tags": '["Example",12]'}),
        ({}, {"is_active": "false"}),
    ],
)
def test_catalog_rejects_malformed_normalized_seed(
    change: dict, attributes: dict
) -> None:
    normalized = catalog.normalize_product(raw_product(1)).model_dump(mode="json")
    normalized.update(change)
    normalized["attributes"].update(attributes)
    with pytest.raises(ValueError):
        catalog.normalize_product(normalized)


@pytest.mark.parametrize("field", ["is_active", "tags"])
def test_catalog_rejects_seed_missing_metadata(field: str) -> None:
    normalized = catalog.normalize_product(raw_product(1)).model_dump(mode="json")
    normalized["attributes"].pop(field)
    with pytest.raises(ValueError):
        catalog.normalize_product(normalized)


def test_catalog_rejects_price_that_rounds_to_zero() -> None:
    with pytest.raises(ValueError):
        catalog.normalize_product(raw_product(1) | {"price": 0.001})


@pytest.mark.parametrize("limit", [1, 12, 37, 1000])
def test_catalog_exact_limit_balanced_categories_and_unique_ids(
    raw_catalog: list[dict], limit: int
) -> None:
    products = catalog.select_products(raw_catalog + [raw_catalog[0]], limit, 42)
    assert len(products) == len({p.product_id for p in products}) == limit
    if limit >= 12:
        counts = Counter(p.category_id for p in products)
        assert len(counts) == 12
        assert max(counts.values()) - min(counts.values()) <= 1


def test_catalog_prefers_complete_metadata_and_filters_invalid() -> None:
    sparse = raw_product(1) | {"manufacturer": "", "model": "", "description": ""}
    products = catalog.select_products(
        [sparse, raw_product(2), raw_product(3) | {"price": -1}], 1, 42
    )
    assert [p.product_id for p in products] == ["bestbuy:2"]


@pytest.mark.parametrize(
    "department,subcategory",
    [
        ("Connected Home & Housewares", "Office Furniture & Storage"),
        ("Musical Instruments", "Sheet Music & DVDs"),
        ("Toys, Games & Drones", "Games, Puzzles & Cards"),
    ],
)
def test_catalog_excludes_nontech_items_within_mixed_departments(
    department: str, subcategory: str
) -> None:
    raw = raw_product(1, department)
    raw["category"][1]["name"] = subcategory
    with pytest.raises(ValueError):
        catalog.normalize_product(raw)


def test_catalog_rejects_insufficient_stock_and_nonpositive_limits() -> None:
    for limit in (0, -1, 2):
        with pytest.raises(ValueError):
            catalog.select_products([raw_product(1)], limit, 42)


def test_catalog_cache_replay_is_offline_and_byte_identical(
    tmp_path: Path, raw_catalog: list[dict], monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(json.dumps(raw_catalog), "utf-8")
    cache = tmp_path / "seed.json"
    first = catalog.ingest_catalog(
        source=source, output=tmp_path / "a/products.jsonl", cache_path=cache
    )
    assert len(json.loads(cache.read_text("utf-8"))) == 1000

    def no_network(*args, **kwargs):
        pytest.fail("Offline replay attempted a network request")

    monkeypatch.setattr("urllib.request.urlopen", no_network)
    second = catalog.ingest_catalog(
        output=tmp_path / "b/products.jsonl", cache_path=cache
    )
    assert first.read_bytes() == second.read_bytes()
    assert_manifest(first, "bestbuy", 1000)
    assert_manifest(second, "bestbuy", 1000)
    small = catalog.ingest_catalog(
        output=tmp_path / "c/products.jsonl", limit=7, cache_path=cache
    )
    assert len(small.read_text("utf-8").splitlines()) == 7
    assert len(json.loads(cache.read_text("utf-8"))) == 1000


def test_catalog_reads_http_source(tmp_path: Path) -> None:
    (tmp_path / "source.json").write_text(json.dumps([raw_product(7)]), "utf-8")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        path = catalog.ingest_catalog(
            source=f"http://127.0.0.1:{server.server_port}/source.json",
            output=tmp_path / "products.jsonl",
            limit=1,
            cache_path=tmp_path / "cache.json",
        )
        assert (
            Product.model_validate_json(path.read_text("utf-8")).product_id
            == "bestbuy:7"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_bundled_bestbuy_seed_has_1000_clean_unique_products() -> None:
    path = Path(__file__).resolve().parents[1] / "data/raw/products_bestbuy_1000.json"
    rows = json.loads(path.read_text("utf-8"))
    products = [catalog.normalize_product(row) for row in rows]
    assert len(products) == len({p.product_id for p in products}) == 1000
    assert len({p.category_id for p in products}) == 12
    assert all(p.image_url.startswith("https://") for p in products)
    assert all(p.brand and p.description and p.attributes["model"] for p in products)


def test_events_have_catalog_foreign_keys_uuid4_utc_and_cart_rules(
    catalog_path: Path,
) -> None:
    products = synthetic.load_catalog(catalog_path)
    events = list(synthetic.generate_events(products, 5000, 30, 100, 42))
    assert len(events) == len({e.event_id for e in events}) == 5000
    ids = {p.product_id for p in products}
    sessions = defaultdict(set)
    carts = defaultdict(set)
    baskets = defaultdict(set)
    for event in events:
        assert Event.model_validate_json(event.model_dump_json()) == event
        assert event.product_id in ids
        assert UUID(event.event_id).version == 4
        assert event.event_at.utcoffset() == timedelta(0)
        assert event.source == "synthetic"
        assert (
            event.cart_id
            if event.event_type == "add_to_cart"
            else event.cart_id is None
        )
        sessions[event.session_id].add(event.user_id)
        if event.cart_id:
            carts[event.cart_id].add((event.user_id, event.session_id))
            baskets[event.cart_id].add(event.product_id)
    assert len({e.user_id for e in events}) == 30
    assert len(sessions) == 100
    assert all(len(owners) == 1 for owners in sessions.values())
    assert all(len(owners) == 1 for owners in carts.values())
    assert sum(len(items) >= 2 for items in baskets.values()) >= 20
    bundle_counts = Counter(
        tuple(sorted(items)) for items in baskets.values() if len(items) >= 2
    )
    assert bundle_counts.most_common(1)[0][1] >= 2


def test_session_funnels_timestamps_and_category_preferences(
    catalog_path: Path,
) -> None:
    products = synthetic.load_catalog(catalog_path)
    categories = {p.product_id: p.category_id for p in products}
    previous_time = {}
    funnel = defaultdict(set)
    interests = defaultdict(Counter)
    counts = Counter()
    for event in synthetic.generate_events(products, 20000, 20, 300, 42):
        key = (event.session_id, event.product_id)
        if event.event_type == "click":
            assert "view" in funnel[key]
        if event.event_type == "add_to_cart":
            assert {"view", "click"} <= funnel[key]
        funnel[key].add(event.event_type)
        assert event.event_at > previous_time.get(
            event.session_id, event.event_at - timedelta(seconds=1)
        )
        previous_time[event.session_id] = event.event_at
        interests[event.user_id][categories[event.product_id]] += 1
        counts[event.event_type] += 1
    assert counts["view"] > counts["click"] > counts["add_to_cart"] > 0
    assert all(
        counter.most_common(1)[0][1] / counter.total() > 0.45
        for counter in interests.values()
    )
    popularity = Counter(
        e.product_id for e in synthetic.generate_events(products, 20000, 20, 300, 42)
    )
    assert sum(n for _, n in popularity.most_common(12)) / popularity.total() > 0.3
    assert min(previous_time.values()).isoformat() < "2026-10-25"
    assert max(previous_time.values()).isoformat() > "2026-11-01"


@pytest.mark.parametrize(
    "n_events,n_users,n_sessions", [(1, 1, 1), (7, 2, 3), (0, 0, 0)]
)
def test_events_honor_small_and_empty_dimensions(
    catalog_path: Path, n_events: int, n_users: int, n_sessions: int
) -> None:
    events = list(
        synthetic.generate_events(
            synthetic.load_catalog(catalog_path), n_events, n_users, n_sessions, 42
        )
    )
    assert len(events) == n_events
    assert len({e.user_id for e in events}) == n_users
    assert len({e.session_id for e in events}) == n_sessions


@pytest.mark.parametrize("dimensions", [(-1, 1, 1), (4, 0, 1), (4, 3, 2), (2, 1, 3)])
def test_events_reject_impossible_dimensions(
    catalog_path: Path, dimensions: tuple
) -> None:
    with pytest.raises(ValueError):
        list(
            synthetic.generate_events(
                synthetic.load_catalog(catalog_path), *dimensions, 42
            )
        )


def test_load_catalog_rejects_empty_and_duplicate_ids(
    catalog_path: Path, tmp_path: Path
) -> None:
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", "utf-8")
    duplicate = tmp_path / "duplicate.jsonl"
    line = catalog_path.read_text("utf-8").splitlines()[0]
    duplicate.write_text(f"{line}\n{line}\n", "utf-8")
    for path in (empty, duplicate):
        with pytest.raises(ValueError):
            synthetic.load_catalog(path)


def test_synthetic_seed_reproducibility_and_manifest(
    catalog_path: Path, tmp_path: Path
) -> None:
    paths = [
        synthetic.ingest_synthetic(catalog_path, tmp_path / name, 5000, 30, 100, seed)
        for name, seed in [("a", 42), ("b", 42), ("different/events.jsonl", 43)]
    ]
    assert paths[0].read_bytes() == paths[1].read_bytes()
    assert paths[0].read_bytes() != paths[2].read_bytes()
    manifest = assert_manifest(paths[0], "synthetic", 5000)
    assert manifest["generator_version"]
    catalog_manifest = json.loads(
        (catalog_path.parent / "manifest.json").read_text("utf-8")
    )
    assert manifest["catalog_version"] == catalog_manifest["catalog_version"]
    assert manifest["parameters"]["n_events"] == 5000
    assert manifest["counts"]["users"] == 30
    assert manifest["counts"]["sessions"] == 100
    assert manifest["counts"]["events"] == 5000
    events = [json.loads(line) for line in paths[0].read_text("utf-8").splitlines()]
    assert manifest["event_time_min"] == min(e["event_at"] for e in events)
    assert manifest["event_time_max"] == max(e["event_at"] for e in events)
    assert manifest["counts"]["events_by_type"] == dict(
        Counter(e["event_type"] for e in events)
    )
    assert manifest["counts"]["events_by_source"] == {"synthetic": 5000}


def test_clis_generate_valid_files(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text(json.dumps([raw_product(i) for i in range(1, 21)]), "utf-8")
    product_path = tmp_path / "products/products.jsonl"
    event_path = tmp_path / "events/events.jsonl"
    commands = [
        [
            "catalog",
            "--source",
            str(source),
            "--limit",
            "20",
            "--output",
            str(product_path),
            "--seed",
            "17",
        ],
        [
            "synthetic",
            "--catalog",
            str(product_path),
            "--output",
            str(event_path),
            "--events",
            "101",
            "--users",
            "3",
            "--sessions",
            "7",
            "--seed",
            "17",
        ],
    ]
    for module, *args in commands:
        result = subprocess.run(
            [sys.executable, "-m", f"recommender.ingestion.{module}", *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env=os.environ | {"DATA_LAKE_DIR": str(tmp_path / "data")},
        )
        assert result.returncode == 0, result.stderr
    assert_manifest(product_path, "bestbuy", 20)
    assert_manifest(event_path, "synthetic", 101)
