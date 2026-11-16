"""Stream reproducible category/Zipf/session/basket behavior from a real catalog."""

import argparse
import hashlib
from collections import defaultdict
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path

import numpy as np
from faker import Faker

from recommender.config import get_settings
from recommender.ingestion.bronze import catalog_version, write_bronze
from recommender.ingestion.users import load_users, validate_user_interests
from recommender.schemas import Event, EventType, Product, UserProfile

GENERATOR_VERSION = "1.1.0"
EVENT_START = datetime(2026, 10, 2, tzinfo=UTC)
EVENT_END = datetime(2026, 11, 8, tzinfo=UTC)
ZIPF_EXPONENT = 1.2
PREFERENCE_PROBABILITY = 0.85
FUNNEL_PROBABILITIES = (0.55, 0.30, 0.15)


def load_catalog(path: str | Path) -> list[Product]:
    """Load strict Product JSONL; reject empty catalogs and ambiguous IDs."""
    products = []
    seen = set()
    with Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, start=1):
            try:
                product = Product.model_validate_json(line)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid catalog record at line {number}: {exc}"
                ) from exc
            if product.product_id in seen:
                raise ValueError(f"Duplicate catalog product_id: {product.product_id}")
            seen.add(product.product_id)
            products.append(product)
    if not products:
        raise ValueError("Catalog must contain at least one product")
    return sorted(products, key=lambda product: product.product_id)


def _event_start(
    products: Sequence[Product], users: Sequence[UserProfile] | None
) -> datetime:
    """Share the effective creation/collection cutoff with manifest metadata."""
    start = max(EVENT_START, max(p.collected_at for p in products) + timedelta(days=1))
    if users:
        start = max(start, max(u.created_at for u in users) + timedelta(days=1))
    return start


def generate_events(
    products: Sequence[Product],
    n_events: int = 100000,
    n_users: int = 1000,
    n_sessions: int = 5000,
    seed: int = 42,
    *,
    users: Sequence[UserProfile] | None = None,
) -> Iterator[Event]:
    """Yield exactly n_events without retaining events in memory.

    Every user owns at least one session and every session contains an event.
    Products are ranked with a seeded permutation and sampled with Zipf weights.
    Users favor a persistent category; exploration adds cross-category noise.
    Sessions with >=6 events begin with a recurring two-item basket, giving
    FP-Growth repeated co-occurrences. Other funnels stop after view or click,
    or proceed to add_to_cart. A final partial funnel still starts with view.
    """
    if users is not None:
        validate_user_interests(users, {p.category_id for p in products})
        n_users = len(users)
    if seed < 0 or min(n_events, n_users, n_sessions) < 0:
        raise ValueError("Counts and seed must be nonnegative")
    if not products or len({p.product_id for p in products}) != len(products):
        raise ValueError("A nonempty catalog with unique product IDs is required")
    if n_events == 0:
        if n_users != 0 or n_sessions != 0:
            raise ValueError("Zero events require zero users and sessions")
        return
    if not 1 <= n_users <= n_sessions <= n_events:
        raise ValueError("Require 1 <= users <= sessions <= events")

    rng = np.random.default_rng(seed)
    fake = Faker("en_US")
    fake.seed_instance(seed)
    if users is not None:
        # Profile mode can coexist with legacy Bronze data. Include every input
        # that can change payloads so distinct runs do not reuse event UUIDs.
        identity = hashlib.sha256(
            f"profiles:{GENERATOR_VERSION}:{seed}:{n_events}:{n_sessions}:{catalog_version(products)}".encode()
        )
        for profile in users:
            identity.update(profile.model_dump_json().encode("utf-8"))
            identity.update(b"\n")
        fake.seed_instance(int.from_bytes(identity.digest(), "big"))
    user_ids = (
        [u.user_id for u in users]
        if users is not None
        else [f"u_syn_{fake.uuid4()}" for _ in range(n_users)]
    )
    owners = np.arange(n_sessions) % n_users
    rng.shuffle(owners)
    session_weights = rng.pareto(2.0, n_sessions) + 1
    budgets = 1 + rng.multinomial(
        n_events - n_sessions, session_weights / session_weights.sum()
    )

    groups: dict[str, list[str]] = defaultdict(list)
    for product in sorted(products, key=lambda p: p.product_id):
        groups[product.category_id].append(product.product_id)
    categories = sorted(groups)
    probabilities = {}
    for category in categories:
        rng.shuffle(groups[category])
        weights = np.arange(1, len(groups[category]) + 1, dtype=float) ** -ZIPF_EXPONENT
        probabilities[category] = weights / weights.sum()
    interests = rng.permutation(n_users) % len(categories)
    start = _event_start(products, users)
    end = max(EVENT_END, start + timedelta(days=37))
    span = int((end - start).total_seconds()) - 1800

    for session_index, (owner, budget_value) in enumerate(
        zip(owners, budgets, strict=True)
    ):
        budget = int(budget_value)
        session_id = f"s_syn_{fake.uuid4()}"
        cart_id = f"c_syn_{fake.uuid4()}"
        profile = users[int(owner)] if users is not None else None
        preferred = (
            profile.primary_interest
            if profile is not None
            else categories[int(interests[int(owner)])]
        )
        event_at = start + timedelta(
            seconds=session_index * span // max(1, n_sessions - 1)
        )
        max_step_us = max(1, min(45_000_000, 1_800_000_000 // budget))
        emitted = 0
        # The two popular products form a recurring category bundle, not an
        # assertion of technical compatibility. One-item catalogs remain valid.
        bundle = groups[preferred][:2] if budget >= 6 else []
        for product_id in bundle:
            for event_type in ("view", "click", "add_to_cart"):
                event_at += timedelta(
                    microseconds=int(rng.integers(1, max_step_us + 1))
                )
                yield Event(
                    event_id=fake.uuid4(),
                    user_id=user_ids[int(owner)],
                    session_id=session_id,
                    product_id=product_id,
                    event_type=event_type,
                    event_at=event_at,
                    source="synthetic",
                    cart_id=cart_id if event_type == "add_to_cart" else None,
                )
                emitted += 1
        while emitted < budget:
            favorite = (
                profile.secondary_interest
                if profile is not None
                and profile.secondary_interest is not None
                and rng.random() < 0.25
                else preferred
            )
            category = (
                favorite
                if rng.random() < PREFERENCE_PROBABILITY
                else categories[int(rng.integers(len(categories)))]
            )
            group = groups[category]
            product_id = group[int(rng.choice(len(group), p=probabilities[category]))]
            depth = min(
                int(rng.choice([1, 2, 3], p=FUNNEL_PROBABILITIES)), budget - emitted
            )
            funnel: tuple[EventType, ...] = ("view", "click", "add_to_cart")
            for event_type in funnel[:depth]:
                event_at += timedelta(
                    microseconds=int(rng.integers(1, max_step_us + 1))
                )
                yield Event(
                    event_id=fake.uuid4(),
                    user_id=user_ids[int(owner)],
                    session_id=session_id,
                    product_id=product_id,
                    event_type=event_type,
                    event_at=event_at,
                    source="synthetic",
                    cart_id=cart_id if event_type == "add_to_cart" else None,
                )
                emitted += 1


def ingest_synthetic(
    catalog_path: str | Path,
    output: str | Path | None = None,
    n_events: int = 100000,
    n_users: int = 1000,
    n_sessions: int = 5000,
    seed: int = 42,
    users_file: str | Path | None = None,
) -> Path:
    """Validate a catalog and publish streamed events to a file or directory."""
    catalog_path = Path(catalog_path)
    products = load_catalog(catalog_path)
    profiles = (
        load_users(users_file, category_ids={p.category_id for p in products})
        if users_file is not None
        else None
    )
    if profiles is not None:
        n_users = len(profiles)
    destination = (
        Path(output)
        if output is not None
        else get_settings().data_lake_dir / "bronze/events"
    )
    if destination.suffix.lower() != ".jsonl":
        destination = destination / "events.jsonl"
    if destination.resolve() == catalog_path.resolve():
        raise ValueError("Event output must differ from its catalog input")
    if users_file is not None and destination.resolve() == Path(users_file).resolve():
        raise ValueError("Event output must differ from its users input")
    # Record the exact runtime versions because RNG sequences may change across
    # library releases even when a seed and generator version stay the same.
    return write_bronze(
        generate_events(products, n_events, n_users, n_sessions, seed, users=profiles),
        destination,
        source="synthetic",
        seed=seed,
        products=products,
        generator_version=GENERATOR_VERSION,
        parameters={
            "n_events": n_events,
            "n_users": n_users,
            "n_sessions": n_sessions,
            "users_checksum_sha256": hashlib.sha256(
                Path(users_file).read_bytes()
            ).hexdigest()
            if users_file is not None
            else None,
            "secondary_interest_probability": 0.25 if profiles is not None else None,
            "catalog_checksum_sha256": hashlib.sha256(
                catalog_path.read_bytes()
            ).hexdigest(),
            "event_start": _event_start(products, profiles)
            .isoformat()
            .replace("+00:00", "Z"),
            "window_days": 37,
            "zipf_exponent": ZIPF_EXPONENT,
            "preference_probability": PREFERENCE_PROBABILITY,
            "funnel_probabilities": list(FUNNEL_PROBABILITIES),
            "bundle_size": 2,
            "bundle_min_session_events": 6,
            "numpy_version": version("numpy"),
            "faker_version": version("Faker"),
        },
    )


def main(argv: list[str] | None = None) -> None:
    """Run python -m recommender.ingestion.synthetic."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog",
        type=Path,
        default=settings.data_lake_dir / "bronze/products/products.jsonl",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--events", type=int, default=100000)
    parser.add_argument("--users", type=int)
    parser.add_argument(
        "--users-file",
        type=Path,
        help="JSON/JSONL profiles; use all users and their category preferences",
    )
    parser.add_argument("--sessions", type=int)
    parser.add_argument("--seed", type=int, default=settings.random_seed)
    args = parser.parse_args(argv)
    if args.users_file is not None and args.users is not None:
        parser.error("--users and --users-file are mutually exclusive")
    try:
        profile_count = (
            len(load_users(args.users_file)) if args.users_file is not None else None
        )
    except (ValueError, OSError) as exc:
        parser.exit(2, f"synthetic: {exc}\n")
    n_users = (
        profile_count
        if profile_count is not None
        else args.users
        if args.users is not None
        else min(1000, max(1, args.events // 50))
        if args.events
        else 0
    )
    n_sessions = (
        args.sessions
        if args.sessions is not None
        else min(5000, max(n_users, args.events // 10))
        if args.events
        else 0
    )
    try:
        path = ingest_synthetic(
            args.catalog,
            args.output,
            args.events,
            n_users,
            n_sessions,
            args.seed,
            args.users_file,
        )
    except (ValueError, OSError) as exc:
        parser.exit(2, f"synthetic: {exc}\n")
    print(f"Published {args.events} events: {path}")


if __name__ == "__main__":
    main()
