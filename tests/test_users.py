"""Kiểm tra ngoại tuyến hồ sơ nhân khẩu học và thu thập dữ liệu theo sở thích."""

import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest
from pydantic import ValidationError

from recommender.ingestion import synthetic, users
from recommender.ingestion.catalog import CATEGORIES
from recommender.schemas import Product, UserProfile

CATALOG = Path(__file__).resolve().parents[1] / "data/bronze/products/products.jsonl"
SEED_CATALOG = (
    Path(__file__).resolve().parents[1] / "data/raw/products_bestbuy_1000.json"
)


def profile_data() -> dict:
    return {
        "user_id": "u_syn_0001",
        "name": "Nguyễn Văn An",
        "gender": "male",
        "age": 25,
        "city": "Hà Nội",
        "primary_interest": "audio",
        "secondary_interest": None,
        "created_at": "2026-09-01T00:00:00Z",
    }


@pytest.mark.parametrize(
    "change",
    [
        {"user_id": " bad"},
        {"user_id": 1},
        {"name": ""},
        {"name": "x" * 129},
        {"gender": "unknown"},
        {"age": 12},
        {"age": 101},
        {"age": "25"},
        {"age": True},
        {"age": 25.0},
        {"city": 1},
        {"primary_interest": ""},
        {"secondary_interest": 5},
        {"created_at": "2026-09-01T00:00:00"},
        {"created_at": "2026-09-01T07:00:00+07:00"},
        {"unexpected": True},
    ],
)
def test_profile_strict_validation(change: dict) -> None:
    with pytest.raises(ValidationError):
        UserProfile.model_validate(profile_data() | change)


@pytest.mark.parametrize("age", [13, 100])
def test_profile_roundtrip_and_age_boundaries(age: int) -> None:
    profile = UserProfile.model_validate(profile_data() | {"age": age})
    assert UserProfile.model_validate_json(profile.model_dump_json()) == profile
    assert profile.model_dump(mode="json")["created_at"].endswith("Z")


def test_users_reproducible_demographics_and_category_foreign_keys() -> None:
    first = users.generate_users(1000, 42)
    assert first == users.generate_users(1000, 42)
    assert first != users.generate_users(1000, 43)
    assert len(first) == len({u.user_id for u in first}) == 1000
    categories = {p["category_id"] for p in json.loads(SEED_CATALOG.read_text("utf-8"))}
    assert set(CATEGORIES.values()) == categories
    assert {u.primary_interest for u in first} == categories
    assert all(u.secondary_interest in categories | {None} for u in first)
    assert all(u.secondary_interest != u.primary_interest for u in first)
    assert {"Hà Nội", "TP.HCM", "Đà Nẵng", "Cần Thơ", "Hải Phòng"} <= {
        u.city for u in first
    }
    assert any(not u.name.isascii() for u in first)


@pytest.mark.parametrize("limit,seed", [(0, 42), (-1, 42), (1, -1)])
def test_users_reject_invalid_dimensions(limit: int, seed: int) -> None:
    with pytest.raises(ValueError):
        users.generate_users(limit, seed)


def test_users_cli_writes_seed_bronze_manifest(tmp_path: Path) -> None:
    output, cache = tmp_path / "users/users.jsonl", tmp_path / "users.json"
    users.main(
        [
            "--limit",
            "24",
            "--output",
            str(output),
            "--cache-path",
            str(cache),
            "--seed",
            "7",
        ]
    )
    profiles = users.load_users(output)
    assert profiles == users.load_users(cache) == users.generate_users(24, 7)
    manifest = json.loads((output.parent / "manifest.json").read_text("utf-8"))
    assert manifest["counts"]["users"] == 24
    assert (
        manifest["files"][0]["sha256"]
        == hashlib.sha256(output.read_bytes()).hexdigest()
    )
    assert manifest["files"][0]["rows"] == 24
    assert manifest["seed"] == 7
    assert b"\r" not in output.read_bytes() + cache.read_bytes()
    before = output.read_bytes(), cache.read_bytes()
    users.main(
        [
            "--limit",
            "24",
            "--output",
            str(output),
            "--cache-path",
            str(cache),
            "--seed",
            "7",
        ]
    )
    assert before == (output.read_bytes(), cache.read_bytes())


@pytest.mark.parametrize(
    "payload",
    [
        [],
        [profile_data(), profile_data()],
        [profile_data() | {"primary_interest": "unknown"}],
    ],
)
def test_load_users_rejects_invalid_input(tmp_path: Path, payload: list) -> None:
    path = tmp_path / "users.json"
    path.write_text(json.dumps(payload), "utf-8")
    with pytest.raises(ValueError):
        users.load_users(path, category_ids=set(CATEGORIES.values()))


def test_synthetic_cli_uses_profile_ids_and_both_interests(tmp_path: Path) -> None:
    catalog = tmp_path / "products.jsonl"
    catalog.write_text(
        "\n".join(json.dumps(p) for p in json.loads(SEED_CATALOG.read_text("utf-8")))
    )
    user_path = tmp_path / "users.json"
    user_path.write_text(
        json.dumps([profile_data() | {"secondary_interest": "video_games"}]), "utf-8"
    )
    output = tmp_path / "events/events.jsonl"
    synthetic.main(
        [
            "--catalog",
            str(catalog),
            "--users-file",
            str(user_path),
            "--events",
            "3000",
            "--sessions",
            "100",
            "--output",
            str(output),
        ]
    )
    events = [json.loads(line) for line in output.read_text("utf-8").splitlines()]
    categories = {p.product_id: p.category_id for p in synthetic.load_catalog(catalog)}
    interests = Counter(categories[e["product_id"]] for e in events)
    assert {e["user_id"] for e in events} == {"u_syn_0001"}
    assert interests["audio"] > interests["video_games"] > 100
    assert (interests["audio"] + interests["video_games"]) / len(events) > 0.8
    manifest = json.loads((output.parent / "manifest.json").read_text("utf-8"))
    assert (
        manifest["parameters"]["users_checksum_sha256"]
        == hashlib.sha256(user_path.read_bytes()).hexdigest()
    )


def test_synthetic_cli_infers_session_count_from_users_file(tmp_path: Path) -> None:
    catalog_path = tmp_path / "products.jsonl"
    products = json.loads(SEED_CATALOG.read_text("utf-8"))
    catalog_path.write_text(
        "\n".join(json.dumps(p) for p in products), encoding="utf-8"
    )
    user_path = tmp_path / "users.json"
    profiles = users.generate_users(30)
    user_path.write_text(
        json.dumps([u.model_dump(mode="json") for u in profiles]), "utf-8"
    )
    output = tmp_path / "events/events.jsonl"
    synthetic.main(
        [
            "--catalog",
            str(catalog_path),
            "--users-file",
            str(user_path),
            "--events",
            "40",
            "--output",
            str(output),
        ]
    )
    events = [json.loads(line) for line in output.read_text("utf-8").splitlines()]
    assert {e["user_id"] for e in events} == {u.user_id for u in profiles}


def test_synthetic_manifest_records_start_after_profile_creation(
    tmp_path: Path,
) -> None:
    user_path = tmp_path / "users.json"
    user_path.write_text(
        json.dumps([profile_data() | {"created_at": "2027-01-01T00:00:00Z"}]), "utf-8"
    )
    catalog_path = tmp_path / "products.jsonl"
    products = json.loads(SEED_CATALOG.read_text("utf-8"))
    catalog_path.write_text("\n".join(json.dumps(p) for p in products), "utf-8")
    output = synthetic.ingest_synthetic(
        catalog_path, tmp_path / "events", 10, 1, 1, users_file=user_path
    )
    manifest = json.loads((output.parent / "manifest.json").read_text("utf-8"))
    assert manifest["parameters"]["event_start"] == "2027-01-02T00:00:00Z"


def test_profile_aware_synthetic_ids_do_not_conflict_with_other_datasets() -> None:
    products = [
        Product.model_validate(p) for p in json.loads(SEED_CATALOG.read_text("utf-8"))
    ]
    profiles = users.generate_users(2)
    legacy = list(synthetic.generate_events(products, 100, 2, 5, 42))
    first = list(synthetic.generate_events(products, 100, 2, 5, 42, users=profiles))
    changed = [profiles[0].model_copy(update={"user_id": "another_user"}), profiles[1]]
    second = list(synthetic.generate_events(products, 100, 2, 5, 42, users=changed))
    resized = list(synthetic.generate_events(products, 101, 2, 5, 42, users=profiles))
    assert first == list(
        synthetic.generate_events(products, 100, 2, 5, 42, users=profiles)
    )
    first_ids = {e.event_id for e in first}
    for events in (legacy, second, resized):
        assert first_ids.isdisjoint(e.event_id for e in events)
