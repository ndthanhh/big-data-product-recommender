"""Collect a balanced Best Buy catalog, retaining a seed for offline replay.

Product v1 stores price/model/activity and JSON-encoded tags in its flat
string-valued attributes. collected_at is the reproducible experiment snapshot;
the manifest records the actual acquisition/publication time.
"""

import argparse
import hashlib
import json
import math
import time
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from recommender.config import get_settings
from recommender.ingestion.bronze import write_bronze, write_json
from recommender.schemas import Product

BESTBUY_URL = (
    "https://raw.githubusercontent.com/BestBuyAPIs/open-data-set/master/products.json"
)
SNAPSHOT_AT = datetime(2026, 10, 1, tzinfo=UTC)
CATEGORIES = {
    "Computers & Tablets": "computers_tablets",
    "TV & Home Theater": "tv_home_theater",
    "Audio": "audio",
    "Cell Phones": "cell_phones",
    "Cameras & Camcorders": "cameras_camcorders",
    "Video Games": "video_games",
    "Appliances": "appliances",
    "Car Electronics & GPS": "car_electronics_gps",
    "Connected Home & Housewares": "connected_home",
    "Wearable Technology": "wearable_technology",
    "Musical Instruments": "musical_instruments",
    "Toys, Games & Drones": "toys_games_drones",
}
TECHNOLOGY_SUBCATEGORIES = {
    "Connected Home & Housewares": {
        "Connected Home",
        "Office Electronics",
        "Telephones & Communication",
        "Scanners, Faxes & Copiers",
        "Batteries & Power",
    },
    "Musical Instruments": {
        "Microphones & Live Sound",
        "Recording Equipment",
        "DJ & Lighting Equipment",
        "Amps & Effects",
        "Keyboards",
        "Karaoke",
    },
    "Toys, Games & Drones": {
        "Drones & Accessories",
        "Remote Control Toys",
        "App-Enabled Toys",
        "Robots & Robotic Kits",
        "Kids' Electronics",
    },
}


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _https(value: object) -> str | None:
    text = _text(value)
    if not text:
        return None
    parts = urlsplit(text)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("Expected an absolute HTTP/HTTPS product URL")
    return urlunsplit(parts._replace(scheme="https"))


def normalize_product(raw: Mapping[str, Any]) -> Product:
    """Normalize a raw Best Buy record or validate a previously normalized seed.

    Reject unusable SKUs, nonphysical products, unsupported departments,
    nonpositive/nonfinite prices, and missing/invalid images. Optional metadata
    completeness is ranked by select_products rather than fabricated.
    """
    if "product_id" in raw:
        product = Product.model_validate(dict(raw))
        price = product.attributes.get("price", "0")
        category = product.attributes.get("category", "")
        tags = json.loads(product.attributes.get("tags", "null"))
        sku = product.product_id.partition(":")[2]
        if (
            product.source != "bestbuy"
            or not sku.isascii()
            or not sku.isdigit()
            or int(sku) <= 0
            or product.category_id != CATEGORIES.get(category)
            or not isinstance(tags, list)
            or not all(isinstance(tag, str) and tag.strip() for tag in tags)
            or product.attributes.get("is_active") != "true"
            or not product.image_url
            or not product.image_url.startswith("https://")
            or not math.isfinite(float(price))
            or float(price) <= 0
            or (
                category in TECHNOLOGY_SUBCATEGORIES
                and not any(tag in TECHNOLOGY_SUBCATEGORIES[category] for tag in tags)
            )
        ):
            raise ValueError("Invalid normalized Best Buy seed record")
        return product
    sku = raw.get("sku")
    if (
        isinstance(sku, bool)
        or not isinstance(sku, (int, str))
        or not str(sku).isascii()
        or not str(sku).isdigit()
        or int(sku) <= 0
    ):
        raise ValueError("Best Buy SKU must be a positive integer identifier")
    if raw.get("type", "HardGood") != "HardGood":
        raise ValueError("Only physical technology products are supported")
    price = raw.get("price")
    if (
        isinstance(price, bool)
        or not isinstance(price, (int, float))
        or not math.isfinite(price)
        or price <= 0
    ):
        raise ValueError("Product price must be finite and positive")
    categories = raw.get("category")
    if not isinstance(categories, list):
        raise TypeError("Missing category hierarchy")
    names = [_text(item.get("name")) for item in categories if isinstance(item, dict)]
    category = next((name for name in names if name in CATEGORIES), None)
    if category is None:
        raise ValueError("Product is outside the technology departments")
    if category in TECHNOLOGY_SUBCATEGORIES and not any(
        name in TECHNOLOGY_SUBCATEGORIES[category] for name in names
    ):
        raise ValueError("Nontechnology subcategory in a mixed department")
    image = _https(raw.get("image"))
    if image is None:
        raise ValueError("A clean catalog product requires an image")
    manufacturer = _text(raw.get("manufacturer"))
    model = _text(raw.get("model"))
    tags = list(dict.fromkeys(tag for tag in [manufacturer, model, *names[1:]] if tag))
    rounded_price = Decimal(str(price)).quantize(Decimal("0.01"))
    if rounded_price <= 0:
        raise ValueError("Product price must remain positive after rounding to cents")
    attributes = {
        "price": format(rounded_price, "f"),
        "category": category,
        "model": model,
        "tags": json.dumps(tags, ensure_ascii=False, separators=(",", ":")),
        "is_active": "true",
    }
    return Product(
        product_id=f"bestbuy:{int(sku)}",
        name=_text(raw.get("name")),
        category_id=CATEGORIES[category],
        source="bestbuy",
        collected_at=SNAPSHOT_AT,
        description=_text(raw.get("description")) or None,
        brand=manufacturer or None,
        image_url=image,
        product_url=_https(raw.get("url")),
        attributes=attributes,
    )


def _quality(product: Product) -> int:
    return sum(
        bool(value)
        for value in (
            product.description,
            product.brand,
            product.product_url,
            product.attributes.get("model"),
            product.attributes.get("tags") not in {None, "[]"},
        )
    )


def select_products(
    records: Iterable[Mapping[str, Any]], limit: int = 1000, seed: int = 42
) -> list[Product]:
    """Select exactly limit clean unique SKUs, balancing technology categories.

    Completeness wins within each category; a seeded hash breaks ties without
    depending on source ordering. Round-robin selection redistributes unfilled
    quotas from small categories to categories that still have candidates.
    """
    if limit <= 0 or seed < 0:
        raise ValueError("limit must be positive and seed must be nonnegative")
    unique: dict[str, Product] = {}
    for raw in records:
        if not isinstance(raw, Mapping):
            continue
        try:
            product = normalize_product(raw)
        except (ValueError, TypeError, ArithmeticError):
            continue
        old = unique.get(product.product_id)
        if old is None or (-_quality(product), product.model_dump_json()) < (
            -_quality(old),
            old.model_dump_json(),
        ):
            unique[product.product_id] = product
    if len(unique) < limit:
        raise ValueError(
            f"Requested {limit} products, but only {len(unique)} clean unique products are available"
        )
    groups: dict[str, list[Product]] = defaultdict(list)
    for product in unique.values():
        groups[product.category_id].append(product)
    for group in groups.values():
        group.sort(
            key=lambda p: (
                -_quality(p),
                hashlib.sha256(f"{seed}:{p.product_id}".encode()).digest(),
                p.product_id,
            )
        )
    selected = []
    depth = 0
    while len(selected) < limit:
        for category in sorted(groups):
            if depth < len(groups[category]):
                selected.append(groups[category][depth])
                if len(selected) == limit:
                    break
        depth += 1
    return sorted(selected, key=lambda p: p.product_id)


def _read_source(source: str | Path) -> list[dict[str, Any]]:
    if str(source).startswith(("http://", "https://")):
        for attempt in range(3):
            try:
                with urllib.request.urlopen(str(source), timeout=30) as response:
                    payload = json.load(response)
                break
            except (urllib.error.URLError, TimeoutError):
                if attempt == 2:
                    raise
                time.sleep(0.25 * (2**attempt))
    else:
        with Path(source).open(encoding="utf-8") as stream:
            payload = json.load(stream)
    if not isinstance(payload, list):
        raise TypeError("Catalog source must be a JSON list")
    return payload


def ingest_catalog(
    source: str | Path | None = None,
    output: str | Path | None = None,
    limit: int = 1000,
    seed: int = 42,
    cache_path: str | Path | None = None,
) -> Path:
    """Collect from an explicit file/URL, or prefer a sufficient offline seed."""
    settings = get_settings()
    cache = (
        Path(cache_path)
        if cache_path is not None
        else settings.data_lake_dir / "raw/products_bestbuy_1000.json"
    )
    destination = (
        Path(output)
        if output is not None
        else settings.data_lake_dir / "bronze/products/products.jsonl"
    )
    if destination.resolve() == cache.resolve():
        raise ValueError("Bronze output must differ from the seed file")
    actual_source = (
        source if source is not None else (cache if cache.is_file() else BESTBUY_URL)
    )
    if (
        not str(actual_source).startswith(("http://", "https://"))
        and Path(actual_source).resolve() == destination.resolve()
    ):
        raise ValueError("Bronze output must differ from its input")
    raw = _read_source(actual_source)
    if source is None and actual_source == cache:
        try:
            select_products(raw, limit, seed)
        except ValueError:
            if limit <= 0 or seed < 0:
                raise
            actual_source = BESTBUY_URL
            raw = _read_source(actual_source)
    products = select_products(raw, limit, seed)
    # A small run must not shrink a useful offline seed down to --limit rows.
    try:
        cached_products = select_products(raw, 1000, seed)
    except ValueError:
        cached_products = products
    if source is not None or not cache.is_file() or actual_source == BESTBUY_URL:
        write_json(cache, [p.model_dump(mode="json") for p in cached_products])
    return write_bronze(
        products,
        destination,
        source="bestbuy",
        seed=seed,
        products=products,
        parameters={
            "limit": limit,
            "source": str(actual_source),
            "upstream": BESTBUY_URL,
            "snapshot_at": SNAPSHOT_AT.isoformat().replace("+00:00", "Z"),
        },
    )


def main(argv: list[str] | None = None) -> None:
    """Run python -m recommender.ingestion.catalog."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seed", type=int, default=settings.random_seed)
    parser.add_argument(
        "--source",
        help="Explicit Best Buy JSON file or HTTP(S) URL; default: offline seed, then GitHub",
    )
    args = parser.parse_args(argv)
    try:
        path = ingest_catalog(
            source=args.source, output=args.output, limit=args.limit, seed=args.seed
        )
    except (ValueError, TypeError, OSError) as exc:
        parser.exit(2, f"catalog: {exc}\n")
    print(f"Published {args.limit} products: {path}")


if __name__ == "__main__":
    main()
