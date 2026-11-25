"""Mutable test helpers around the reproducible demo fixture."""

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq

from recommender.pipeline.demo_fixture import make_gold_fixture as make_gold_fixture  # noqa: PLC0414


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
