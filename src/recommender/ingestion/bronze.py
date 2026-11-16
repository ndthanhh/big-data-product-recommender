"""Publish streamed Bronze JSONL with byte checksums and contract v1 metadata."""

import hashlib
import json
import os
import subprocess
import tempfile
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from recommender.config import PROJECT_ROOT
from recommender.schemas import SCHEMA_VERSION, Event, Product


def catalog_version(products: Sequence[Product]) -> str:
    """Identify normalized catalog contents independently of input line order."""
    digest = hashlib.sha256()
    for product in sorted(products, key=lambda item: item.product_id):
        digest.update(
            json.dumps(product.model_dump(mode="json"), sort_keys=True).encode("utf-8")
        )
        digest.update(b"\n")
    return f"catalog-{digest.hexdigest()}"


def write_json(path: Path, value: object) -> None:
    """Atomically replace a UTF-8/LF JSON document after writing it completely."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(
                (
                    json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
                    + "\n"
                ).encode("utf-8")
            )
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _provenance() -> tuple[str, bool]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return commit, bool(dirty)
    except (OSError, subprocess.CalledProcessError):
        return "unknown", True


def write_bronze(
    records: Iterable[Product | Event],
    output: Path,
    *,
    source: str,
    seed: int,
    parameters: dict[str, Any],
    products: Sequence[Product],
    generator_version: str | None = None,
) -> Path:
    """Write bounded-memory JSONL, then publish its manifest as the last step.

    Statistics retain catalog/user/session/cart IDs, never individual events.
    A generator failure leaves the previous published data and manifest intact.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    users: set[str] = set()
    sessions: set[str] = set()
    carts: set[str] = set()
    referenced: set[str] = set()
    types: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    minimum = maximum = None
    rows = size = event_count = 0
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=output.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            for record in records:
                data = (record.model_dump_json() + "\n").encode("utf-8")
                stream.write(data)
                digest.update(data)
                rows += 1
                size += len(data)
                if isinstance(record, Event):
                    event_count += 1
                    users.add(record.user_id)
                    sessions.add(record.session_id)
                    referenced.add(record.product_id)
                    if record.cart_id is not None:
                        carts.add(record.cart_id)
                    types[record.event_type] += 1
                    sources[record.source] += 1
                    minimum = (
                        record.event_at
                        if minimum is None
                        else min(minimum, record.event_at)
                    )
                    maximum = (
                        record.event_at
                        if maximum is None
                        else max(maximum, record.event_at)
                    )
        checksum = digest.hexdigest()
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        commit, dirty = _provenance()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "created_at": now,
            "collected_at": now,
            "source": source,
            "record_count": rows,
            "checksum_sha256": checksum,
            "code_commit": commit,
            "code_dirty": dirty,
            "generator_version": generator_version,
            "catalog_version": catalog_version(products),
            "seed": seed,
            "parameters": parameters,
            "counts": {
                "product_ids": len({p.product_id for p in products}),
                "catalog_versions": len(products),
                "referenced_product_ids": len(referenced),
                "users": len(users),
                "sessions": len(sessions),
                "carts": len(carts),
                "events": event_count,
                "events_by_type": dict(sorted(types.items())),
                "events_by_source": dict(sorted(sources.items())),
            },
            "event_time_min": minimum.isoformat().replace("+00:00", "Z")
            if minimum
            else None,
            "event_time_max": maximum.isoformat().replace("+00:00", "Z")
            if maximum
            else None,
            "files": [
                {
                    "path": output.name,
                    "format": "jsonl",
                    "rows": rows,
                    "bytes": size,
                    "sha256": checksum,
                }
            ],
        }
        # Include publication metadata so a version never identifies a different package.
        identity = hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode("utf-8")
        ).hexdigest()
        manifest["dataset_version"] = f"{source}-{identity}"
        os.replace(temporary, output)
        write_json(output.parent / "manifest.json", manifest)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return output
