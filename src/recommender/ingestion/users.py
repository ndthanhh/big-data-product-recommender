"""Sinh hồ sơ nhân khẩu học Việt Nam có thể tái lập và ghi JSONL Bronze."""

import argparse
import json
import random
from collections.abc import Collection, Sequence
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path

from faker import Faker

from recommender.config import get_settings
from recommender.ingestion.bronze import write_bronze, write_json
from recommender.ingestion.catalog import CATEGORIES
from recommender.schemas import UserProfile

GENERATOR_VERSION = "1.0.0"
CREATED_START = datetime(2026, 9, 1, tzinfo=UTC)
VIETNAMESE_CITIES = (
    "Hà Nội",
    "TP.HCM",
    "Đà Nẵng",
    "Cần Thơ",
    "Hải Phòng",
    "Huế",
    "Nha Trang",
    "Đà Lạt",
    "Vũng Tàu",
    "Quy Nhơn",
    "Biên Hòa",
    "Buôn Ma Thuột",
)


def generate_users(limit: int = 1000, seed: int = 42) -> list[UserProfile]:
    """Cân bằng sở thích chính trên 12 mã nhóm sản phẩm Best Buy hiện có.

    Nhân khẩu học và sở thích được lấy mẫu độc lập. Khoảng thời gian tạo UTC
    cố định và bộ sinh ngẫu nhiên riêng giúp tái lập mà không lệ thuộc giờ thực tế.
    """
    if limit <= 0 or seed < 0:
        raise ValueError("limit must be positive and seed must be nonnegative")
    rng = random.Random(seed)
    fake = Faker("vi_VN")
    fake.seed_instance(seed)
    categories = sorted(CATEGORIES.values())
    rng.shuffle(categories)
    profiles = []
    for index in range(limit):
        primary = categories[index % len(categories)]
        secondary = (
            rng.choice([c for c in categories if c != primary])
            if rng.random() < 0.8
            else None
        )
        gender = rng.choice(("male", "female", "other"))
        name = (
            fake.name_male()
            if gender == "male"
            else fake.name_female()
            if gender == "female"
            else fake.name()
        )
        profiles.append(
            UserProfile(
                user_id=f"u_syn_{index + 1:04d}",
                name=name,
                gender=gender,
                age=rng.randint(13, 100),
                city=rng.choice(VIETNAMESE_CITIES),
                primary_interest=primary,
                secondary_interest=secondary,
                created_at=CREATED_START + timedelta(seconds=rng.randrange(30 * 86400)),
            )
        )
    return profiles


def validate_user_interests(
    profiles: Sequence[UserProfile], category_ids: Collection[str]
) -> None:
    """Từ chối ID người dùng thiếu/trùng và sở thích ngoài danh mục sản phẩm."""
    if not profiles or len({u.user_id for u in profiles}) != len(profiles):
        raise ValueError("Users must be nonempty with unique user_id values")
    categories = set(category_ids)
    for profile in profiles:
        for interest in (profile.primary_interest, profile.secondary_interest):
            if interest is not None and interest not in categories:
                raise ValueError(
                    f"Unknown category_id {interest!r} for {profile.user_id}"
                )


def load_users(
    path: str | Path, *, category_ids: Collection[str] | None = None
) -> list[UserProfile]:
    """Đọc mảng JSON hoặc JSONL gốc theo quy tắc nghiêm ngặt, giữ thứ tự người dùng đầu vào."""
    content = Path(path).read_text(encoding="utf-8")
    if content.lstrip().startswith("["):
        profiles = [UserProfile.model_validate(row) for row in json.loads(content)]
    else:
        profiles = [
            UserProfile.model_validate_json(line) for line in content.splitlines()
        ]
    validate_user_interests(
        profiles, category_ids if category_ids is not None else CATEGORIES.values()
    )
    return profiles


def ingest_users(
    output: str | Path | None = None,
    limit: int = 1000,
    seed: int = 42,
    cache_path: str | Path | None = None,
) -> Path:
    """Ghi dữ liệu gốc có thể tái lập theo yêu cầu, rồi công bố hồ sơ kèm manifest."""
    settings = get_settings()
    destination = (
        Path(output)
        if output is not None
        else settings.data_lake_dir / "bronze/users/users.jsonl"
    )
    cache = (
        Path(cache_path)
        if cache_path is not None
        else settings.data_lake_dir / "raw/users_1000.json"
    )
    if destination.resolve() == cache.resolve() or "manifest.json" in (
        destination.name,
        cache.name,
    ):
        raise ValueError("Seed, Bronze output and manifest must use different paths")
    profiles = generate_users(limit, seed)
    write_json(cache, [u.model_dump(mode="json") for u in profiles])
    return write_bronze(
        profiles,
        destination,
        source="synthetic-users",
        seed=seed,
        products=[],
        generator_version=GENERATOR_VERSION,
        parameters={
            "limit": limit,
            "locale": "vi_VN",
            "faker_version": version("Faker"),
            "category_ids": sorted(CATEGORIES.values()),
            "created_start": CREATED_START.isoformat().replace("+00:00", "Z"),
        },
    )


def main(argv: list[str] | None = None) -> None:
    """Chạy python -m recommender.ingestion.users."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seed", type=int, default=settings.random_seed)
    parser.add_argument("--cache-path", type=Path)
    args = parser.parse_args(argv)
    try:
        path = ingest_users(args.output, args.limit, args.seed, args.cache_path)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"users: {exc}\n")
    print(f"Published {args.limit} users: {path}")


if __name__ == "__main__":
    main()
