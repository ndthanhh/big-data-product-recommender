"""Mô phỏng chuỗi hành vi thời gian thực, gửi sự kiện JSON theo hợp đồng v1 lên Kafka."""

import argparse
import hashlib
import math
import random
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from recommender.config import get_settings
from recommender.ingestion.synthetic import load_catalog
from recommender.ingestion.users import load_users, validate_user_interests
from recommender.schemas import Event, Product, UserProfile


def generate_stream(
    products: Sequence[Product],
    users: Sequence[UserProfile],
    seed: int = 42,
    *,
    start_at: datetime | None = None,
) -> Iterator[Event]:
    """Sinh liên tục chuỗi xem → nhấp → thêm giỏ trong phiên người dùng.

    Ưu tiên sở thích chính/phụ ở 85% lượt chọn sản phẩm (chia theo tỷ lệ 75/25).
    Các lượt còn lại khám phá danh mục. Cố định start_at cho phép tái lập chính
    xác; chạy trực tiếp dùng giờ thực tế và không gian định danh lần chạy riêng.
    """
    if (
        seed < 0
        or not products
        or len({p.product_id for p in products}) != len(products)
    ):
        raise ValueError("Nonnegative seed and a nonempty unique catalog are required")
    validate_user_interests(users, {p.category_id for p in products})
    start = start_at if start_at is not None else datetime.now(UTC)
    if start.tzinfo is None or start.utcoffset() != timedelta(0):
        raise ValueError("start_at must be UTC")
    if start < max([p.collected_at for p in products] + [u.created_at for u in users]):
        raise ValueError("Stream must start after catalog collection and user creation")
    namespace = hashlib.sha256(f"{seed}:{start.isoformat()}".encode()).hexdigest()[:20]
    rng = random.Random(seed)
    groups: dict[str, list[str]] = defaultdict(list)
    for product in sorted(products, key=lambda p: p.product_id):
        groups[product.category_id].append(product.product_id)
    categories = sorted(groups)
    sequence = 0
    while True:
        user = rng.choice(users)
        session = f"s_stream_{namespace}_{UUID(int=rng.getrandbits(128), version=4)}"
        cart = f"c_stream_{namespace}_{UUID(int=rng.getrandbits(128), version=4)}"
        for _ in range(rng.randint(2, 5)):
            if rng.random() < 0.85:
                category = (
                    user.secondary_interest
                    if user.secondary_interest is not None and rng.random() < 0.25
                    else user.primary_interest
                )
            else:
                category = rng.choice(categories)
            product_id = rng.choice(groups[category])
            depth = rng.choices((1, 2, 3), weights=(0.55, 0.30, 0.15))[0]
            for event_type in ("view", "click", "add_to_cart")[:depth]:
                yield Event(
                    event_id=str(
                        UUID(int=rng.getrandbits(128) ^ int(namespace, 16), version=4)
                    ),
                    user_id=user.user_id,
                    session_id=session,
                    product_id=product_id,
                    event_type=event_type,
                    event_at=start + timedelta(milliseconds=sequence)
                    if start_at is not None
                    else datetime.now(UTC),
                    source="synthetic",
                    cart_id=cart if event_type == "add_to_cart" else None,
                )
                sequence += 1


def create_kafka_producer(bootstrap_servers: str) -> Any:
    """Chỉ import thư viện Kafka tùy chọn khi gửi dữ liệu đến broker."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        max_block_ms=10000,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )


def run_producer(
    catalog_path: str | Path,
    users_path: str | Path,
    *,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    rate: float = 10,
    max_events: int | None = None,
    seed: int = 42,
    dry_run: bool = False,
    kafka_producer: Any = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    should_stop: Callable[[], bool] | None = None,
) -> int:
    """Gửi tối đa max_events, điều tiết bằng đồng hồ đơn điệu; dry-run in JSONL."""
    if (
        not math.isfinite(rate)
        or rate <= 0
        or seed < 0
        or (max_events is not None and max_events < 0)
    ):
        raise ValueError(
            "rate must be finite and positive; max-events/seed nonnegative"
        )
    settings = get_settings()
    products = load_catalog(catalog_path)
    users = load_users(users_path, category_ids={p.category_id for p in products})
    topic = topic if topic is not None else settings.kafka_topic_events
    if max_events == 0:
        return 0
    client = None if dry_run else kafka_producer
    if not dry_run and client is None:
        client = create_kafka_producer(
            bootstrap_servers or settings.kafka_bootstrap_servers
        )
    sent = 0
    started = monotonic()
    try:
        events = generate_stream(products, users, seed)
        while max_events is None or sent < max_events:
            if should_stop is not None and should_stop():
                break
            delay = started + sent / rate - monotonic()
            if delay > 0:
                if should_stop is None:
                    sleep(delay)
                else:
                    while delay > 0 and not should_stop():
                        interval = min(delay, 0.1)
                        sleep(interval)
                        delay -= interval
                    if should_stop():
                        break
            event = next(events)
            if dry_run:
                print(event.model_dump_json(), flush=True)
            else:
                client.send(
                    topic,
                    key=event.user_id.encode("utf-8"),
                    value=event.model_dump_json().encode("utf-8"),
                ).get(timeout=10)
            sent += 1
    except KeyboardInterrupt:
        pass
    finally:
        if client is not None:
            try:
                client.flush(timeout=10)
            finally:
                client.close(timeout=10)
    return sent


def main(argv: list[str] | None = None) -> None:
    """Chạy python -m recommender.streaming.producer; Ctrl+C dừng có dọn dẹp."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog",
        type=Path,
        default=settings.data_lake_dir / "bronze/products/products.jsonl",
    )
    parser.add_argument(
        "--users",
        type=Path,
        default=settings.data_lake_dir / "bronze/users/users.jsonl",
    )
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic_events)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--seed", type=int, default=settings.random_seed)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        count = run_producer(
            args.catalog,
            args.users,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            rate=args.rate,
            max_events=args.max_events,
            seed=args.seed,
            dry_run=args.dry_run,
        )
    except (ValueError, OSError, ImportError) as exc:
        parser.exit(2, f"producer: {exc}\n")
    print(
        f"Published {count} events" if not args.dry_run else f"Printed {count} events",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
