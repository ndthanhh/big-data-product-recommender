"""Kiểm tra snapshot phục vụ đầy đủ và bất biến trước khi mở giao dịch ghi."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import TypeAdapter

from recommender.schemas import (
    AssociationRule,
    GoldProduct,
    PathId,
    RecommendationItem,
    SeenProduct,
    UtcTimestamp,
    validate_recommendation_items,
)

TABLE_MODELS = {
    "products": GoldProduct,
    "recommendation_items": RecommendationItem,
    "seen_products": SeenProduct,
    "association_rules": AssociationRule,
}


@dataclass(frozen=True)
class ValidatedSnapshot:
    manifest: dict
    fingerprint: str
    tables: dict[str, list[dict]]
    paths: dict[str, list[Path]]


def _check_schema(table: pa.Table, model) -> None:
    if set(table.column_names) != set(model.model_fields):
        raise ValueError(f"Unexpected columns for {model.__name__}")
    for field in table.schema:
        kind = field.type
        expected = (
            pa.types.is_timestamp(kind)
            if field.name == "collected_at"
            else pa.types.is_map(kind)
            if field.name == "attributes"
            else pa.types.is_list(kind) and pa.types.is_string(kind.value_type)
            if field.name == "antecedent_ids"
            else pa.types.is_int32(kind)
            if field.name == "rank"
            else pa.types.is_float64(kind)
            if field.name in {"score", "support", "confidence", "lift"}
            else pa.types.is_string(kind)
            or (
                pa.types.is_null(kind)
                and model.model_fields[field.name].default is None
            )
        )
        if not expected:
            raise ValueError(f"Unexpected Parquet type for {field.name}: {kind}")


def validate_snapshot(path: Path) -> ValidatedSnapshot:
    content = (path / "manifest.json").read_bytes()
    m = json.loads(content)
    required = {
        "schema_version",
        "run_id",
        "dataset_version",
        "created_at",
        "code_commit",
        "code_dirty",
        "seed",
        "training_start",
        "training_end",
        "evaluation_window",
        "parameters",
        "environment",
        "counts",
        "files",
    }
    if (
        not required <= m.keys()
        or type(m["schema_version"]) is not int
        or m["schema_version"] != 1
    ):
        raise ValueError("Incomplete Gold manifest")
    for key in ("run_id", "dataset_version"):
        TypeAdapter(PathId).validate_python(m[key])
    if path.name != m["run_id"]:
        raise ValueError("Snapshot directory must equal run_id")
    start = TypeAdapter(UtcTimestamp).validate_python(m["training_start"])
    end = TypeAdapter(UtcTimestamp).validate_python(m["training_end"])
    TypeAdapter(UtcTimestamp).validate_python(m["created_at"])
    if (
        start >= end
        or type(m["seed"]) is not int
        or not -(2**63) <= m["seed"] < 2**63
        or type(m["code_dirty"]) is not bool
    ):
        raise ValueError("Invalid training metadata")
    if any(not isinstance(m[k], dict) for k in ("parameters", "environment", "counts")):
        raise ValueError("Manifest metadata must be objects")
    tables = {name: [] for name in TABLE_MODELS}
    paths = {name: [] for name in TABLE_MODELS}
    used = set()
    for entry in m["files"]:
        relative = entry["path"]
        target = path / relative
        if (
            not relative
            or "\\" in relative
            or ":" in relative
            or ".." in Path(relative).parts
            or Path(relative).is_absolute()
            or not target.resolve().is_relative_to(path.resolve())
            or relative in used
            or relative == "manifest.json"
        ):
            raise ValueError("Unsafe or duplicate manifest file path")
        used.add(relative)
        data = target.read_bytes()
        if (
            len(data) != entry["bytes"]
            or hashlib.sha256(data).hexdigest() != entry["sha256"]
        ):
            raise ValueError("File checksum/size mismatch")
        name = entry.get("table")
        if name not in TABLE_MODELS:
            continue
        if entry["format"] != "parquet":
            raise ValueError("Serving tables require Parquet")
        table = pq.read_table(pa.BufferReader(data))
        _check_schema(table, TABLE_MODELS[name])
        if type(entry["rows"]) is not int or table.num_rows != entry["rows"]:
            raise ValueError("File row count mismatch")
        paths[name].append(target)
        for row in table.to_pylist():
            if name == "products":
                row["attributes"] = dict(row["attributes"])
                timestamp = row["collected_at"]
                if timestamp is not None and timestamp.tzinfo is None:
                    row["collected_at"] = timestamp.replace(tzinfo=UTC)
            model = TABLE_MODELS[name].model_validate(row)
            if model.run_id != m["run_id"]:
                raise ValueError("Mixed run_id in snapshot")
            tables[name].append(model.model_dump())
    for name, rows in tables.items():
        if (
            not paths[name]
            or type(m["counts"].get(name)) is not int
            or len(rows) != m["counts"][name]
        ):
            raise ValueError(f"Missing table or incorrect count: {name}")
    products = {row["product_id"]: row for row in tables["products"]}
    if len(products) != len(tables["products"]):
        raise ValueError("Duplicate product ID")
    if any(row["collected_at"] >= end for row in products.values()):
        raise ValueError("Catalog metadata after cutoff")
    seen = set()
    for row in tables["seen_products"]:
        key = (row["user_id"], row["product_id"])
        if key in seen or row["product_id"] not in products:
            raise ValueError("Invalid seen product key")
        seen.add(key)
    recs = [
        RecommendationItem.model_validate(row) for row in tables["recommendation_items"]
    ]
    validate_recommendation_items(recs)
    methods = set()
    for row in recs:
        if row.product_id not in products:
            raise ValueError("Recommendation outside catalog")
        if row.kind == "personalized":
            if (row.context_id, row.product_id) in seen:
                raise ValueError("Seen personalized item")
            if row.method != "popularity":
                methods.add(row.method)
        if row.kind == "substitute" and (
            row.context_id not in products
            or products[row.context_id]["category_id"]
            != products[row.product_id]["category_id"]
        ):
            raise ValueError("Substitute must belong to same category")
    if len(methods) > 1:
        raise ValueError("Run must use a single personalized algorithm")
    for context in {row.context_id for row in recs if row.kind == "personalized"}:
        group = sorted(
            (
                row
                for row in recs
                if row.kind == "personalized" and row.context_id == context
            ),
            key=lambda row: row.rank,
        )
        fallback = False
        for row in group:
            if fallback and row.method != "popularity":
                raise ValueError(
                    "Popularity fallback must follow primary recommendations"
                )
            fallback |= row.method == "popularity"
    keys = set()
    for row in tables["association_rules"]:
        key = (tuple(row["antecedent_ids"]), row["consequent_id"])
        if (
            key in keys
            or not set(row["antecedent_ids"] + [row["consequent_id"]])
            <= products.keys()
        ):
            raise ValueError("Invalid association rule key or catalog reference")
        keys.add(key)
    return ValidatedSnapshot(m, hashlib.sha256(content).hexdigest(), tables, paths)
