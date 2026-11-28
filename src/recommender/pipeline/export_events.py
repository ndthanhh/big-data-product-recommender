"""Xuất toàn bộ sự kiện demo đã commit trong một lần đọc nhất quán, hỗ trợ phát lại ít nhất một lần."""

import argparse
import json
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from psycopg.rows import dict_row

from recommender.api.db import connect
from recommender.config import get_settings
from recommender.ingestion.bronze import write_bronze, write_json
from recommender.schemas import Event, Product


def export_demo_events(database_url: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    # Bộ công bố MinIO bỏ qua thư mục tạm ẩn. Nếu xuất dữ liệu thất bại,
    # cơ sở dữ liệu và các lô đã công bố trước đó vẫn được giữ nguyên.
    temporary = Path(tempfile.mkdtemp(prefix=".export-", dir=output_dir.parent))
    try:
        with connect(database_url) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            maximum = conn.execute(
                "SELECT max(received_at) FROM events WHERE source='demo'"
            ).fetchone()[0]
            runs = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT catalog_run_id FROM events WHERE source='demo' ORDER BY catalog_run_id"
                )
            ]
            with conn.cursor(row_factory=dict_row) as cur:
                products = [
                    Product.model_validate(
                        {k: v for k, v in row.items() if k != "run_id"}
                    )
                    for row in cur.execute(
                        "SELECT * FROM products WHERE run_id=ANY(%s) ORDER BY run_id,product_id",
                        (runs,),
                    )
                ]
            with conn.cursor(name="demo_export", row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT event_id,user_id,session_id,product_id,event_type,event_at,source,cart_id FROM events WHERE source='demo' ORDER BY received_at,event_id"
                )
                records = (
                    Event.model_validate(dict(row, event_id=str(row["event_id"])))
                    for row in cur
                )
                write_bronze(
                    records,
                    temporary / "events.jsonl",
                    source="demo-export",
                    seed=None,
                    parameters={"mode": "full_snapshot", "catalog_run_ids": runs},
                    products=products,
                )
            manifest = json.loads((temporary / "manifest.json").read_text("utf-8"))
            manifest["exported_at"] = (
                datetime.now(UTC).isoformat().replace("+00:00", "Z")
            )
            manifest["received_at_max"] = (
                maximum.isoformat().replace("+00:00", "Z") if maximum else None
            )
            write_json(temporary / "manifest.json", manifest)
        target = output_dir / ("export-" + uuid4().hex)
        temporary.rename(target)
        return target / "manifest.json"
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=get_settings().data_lake_dir / "bronze" / "demo-events",
    )
    args = parser.parse_args()
    print(export_demo_events(get_settings().database_url, args.output_dir))


if __name__ == "__main__":
    main()
