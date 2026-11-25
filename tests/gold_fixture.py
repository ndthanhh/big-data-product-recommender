"""Small serving snapshot with actual Parquet files, explicitly synthetic."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def make_gold_fixture(output: Path, run_id: str = "fixture_run") -> Path:
    root = output / run_id
    root.mkdir(parents=True)
    instant = datetime(2026, 1, 1, tzinfo=UTC)
    products = [
        {
            "run_id": run_id,
            "product_id": f"fixture:{p}",
            "name": f"Product {p}",
            "category_id": "accessories",
            "source": "fixture",
            "collected_at": instant,
            "description": None,
            "brand": None,
            "image_url": None,
            "product_url": None,
            "attributes": {"connector": "usb-c"},
        }
        for p in "abcde"
    ]
    recs = [
        {
            "run_id": run_id,
            "kind": "popular",
            "context_id": "global",
            "product_id": f"fixture:{p}",
            "rank": n,
            "score": float(10 - n),
            "method": "popularity",
        }
        for n, p in enumerate("abcde", 1)
    ]
    recs += [
        {
            "run_id": run_id,
            "kind": "personalized",
            "context_id": "u1",
            "product_id": f"fixture:{p}",
            "rank": n,
            "score": 0.8,
            "method": "tfidf",
        }
        for n, p in enumerate("bc", 1)
    ]
    recs += [
        {
            "run_id": run_id,
            "kind": "substitute",
            "context_id": "fixture:a",
            "product_id": "fixture:c",
            "rank": 1,
            "score": 1.0,
            "method": "tfidf",
        }
    ]
    data = {
        "products": products,
        "recommendation_items": recs,
        "seen_products": [
            {"run_id": run_id, "user_id": "u1", "product_id": "fixture:a"}
        ],
        "association_rules": [
            {
                "run_id": run_id,
                "antecedent_ids": ["fixture:a"],
                "consequent_id": "fixture:d",
                "support": 0.5,
                "confidence": 0.8,
                "lift": 1.2,
            }
        ],
    }
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "dataset_version": "fixture_v1",
        "created_at": "2026-01-03T00:00:00Z",
        "code_commit": "fixture",
        "code_dirty": True,
        "seed": 42,
        "training_start": "2026-01-01T00:00:00Z",
        "training_end": "2026-01-02T00:00:00Z",
        "evaluation_window": None,
        "parameters": {"fixture": True},
        "environment": {"test": True},
        "counts": {},
        "files": [],
    }
    for name, rows in data.items():
        table = pa.Table.from_pylist(rows)
        if name == "products":
            table = table.set_column(
                table.schema.get_field_index("attributes"),
                "attributes",
                pa.array(
                    [p["attributes"] for p in rows],
                    type=pa.map_(pa.string(), pa.string()),
                ),
            )
        if name == "recommendation_items":
            table = table.set_column(
                table.schema.get_field_index("rank"),
                "rank",
                pa.array([p["rank"] for p in rows], type=pa.int32()),
            )
        path = root / f"{name}.parquet"
        pq.write_table(table, path)
        content = path.read_bytes()
        manifest["counts"][name] = len(rows)
        manifest["files"].append(
            {
                "path": path.name,
                "table": name,
                "format": "parquet",
                "rows": len(rows),
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def rewrite_table(root, table_name, transform):
    path = root / f"{table_name}.parquet"
    table = pq.read_table(path)
    rows = table.to_pylist()
    transform(rows)
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    manifest = json.loads((root / "manifest.json").read_text("utf-8"))
    for entry in manifest["files"]:
        if entry["table"] == table_name:
            entry.update(
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                bytes=path.stat().st_size,
                rows=len(rows),
            )
    manifest["counts"][table_name] = len(rows)
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
