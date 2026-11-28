"""Công bố Gold trong giao dịch; gửi lại không gây lặp, khôi phục phiên bản phải yêu cầu rõ."""

import argparse
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

from recommender.api.db import connect
from recommender.config import get_settings
from recommender.pipeline.snapshot import validate_snapshot


def _insert_table(conn, name: str, rows: list[dict]) -> None:
    if not rows:
        return
    columns = list(rows[0])
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(name),
        sql.SQL(",").join(map(sql.Identifier, columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )
    with conn.cursor() as cur:
        cur.executemany(
            query,
            [
                [
                    Jsonb(row[col]) if isinstance(row[col], dict) else row[col]
                    for col in columns
                ]
                for row in rows
            ],
        )


def import_snapshot(path: Path, database_url: str) -> str:
    with connect(database_url) as conn:
        expected = conn.execute(
            "SELECT active_run_id FROM serving_state WHERE singleton_id=1"
        ).fetchone()[0]
    snapshot = validate_snapshot(path)
    m = snapshot.manifest
    run = m["run_id"]
    with connect(database_url) as conn:
        conn.execute(
            """INSERT INTO batch_runs(run_id,dataset_version,schema_version,manifest_sha256,status,training_start,training_end,seed,code_commit,parameters,environment,row_counts)
            VALUES (%s,%s,1,%s,'loading',%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id) DO NOTHING""",
            (
                run,
                m["dataset_version"],
                snapshot.fingerprint,
                m["training_start"],
                m["training_end"],
                m["seed"],
                m["code_commit"],
                Jsonb(dict(m["parameters"], code_dirty=m["code_dirty"])),
                Jsonb(m["environment"]),
                Jsonb(m["counts"]),
            ),
        )
        previous = conn.execute(
            "SELECT manifest_sha256 FROM batch_runs WHERE run_id=%s", (run,)
        ).fetchone()[0]
        if previous != snapshot.fingerprint:
            raise ValueError("run_id fingerprint conflict")
    try:
        with connect(database_url) as conn:
            active = conn.execute(
                "SELECT active_run_id FROM serving_state WHERE singleton_id=1 FOR UPDATE"
            ).fetchone()[0]
            fingerprint, status = conn.execute(
                "SELECT manifest_sha256,status FROM batch_runs WHERE run_id=%s FOR UPDATE",
                (run,),
            ).fetchone()
            if fingerprint != snapshot.fingerprint:
                raise ValueError("run_id fingerprint conflict")
            if status == "ready":
                return run
            if active != expected:
                raise ValueError(
                    "Active snapshot changed during import; retry explicitly"
                )
            for name, rows in snapshot.tables.items():
                _insert_table(conn, name, rows)
                count = conn.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE run_id=%s").format(
                        sql.Identifier(name)
                    ),
                    (run,),
                ).fetchone()[0]
                if count != len(rows):
                    raise ValueError("Imported row count differs")
            conn.execute(
                "UPDATE batch_runs SET status='ready', activated_at=clock_timestamp(),error_message=NULL WHERE run_id=%s",
                (run,),
            )
            conn.execute(
                "UPDATE serving_state SET active_run_id=%s,updated_at=clock_timestamp() WHERE singleton_id=1",
                (run,),
            )
    except Exception:
        # Không lưu chuỗi kết nối hay nội dung ngoại lệ tùy ý vào thông tin mô tả.
        with connect(database_url) as conn:
            conn.execute(
                "UPDATE batch_runs SET status='failed',error_message='Import failed; see process log' WHERE run_id=%s AND manifest_sha256=%s AND status<>'ready'",
                (run, snapshot.fingerprint),
            )
        raise
    return run


def activate_run(run_id: str, database_url: str) -> None:
    with connect(database_url) as conn:
        conn.execute(
            "SELECT active_run_id FROM serving_state WHERE singleton_id=1 FOR UPDATE"
        )
        row = conn.execute(
            "SELECT status,row_counts FROM batch_runs WHERE run_id=%s", (run_id,)
        ).fetchone()
        if row is None or row[0] != "ready":
            raise ValueError("Can only activate a ready run")
        for name in (
            "products",
            "recommendation_items",
            "seen_products",
            "association_rules",
        ):
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE run_id=%s").format(
                    sql.Identifier(name)
                ),
                (run_id,),
            ).fetchone()[0]
            if count != row[1][name]:
                raise ValueError("Ready run has incomplete tables")
        conn.execute(
            "UPDATE serving_state SET active_run_id=%s,updated_at=clock_timestamp() WHERE singleton_id=1",
            (run_id,),
        )
        conn.execute(
            "UPDATE batch_runs SET activated_at=clock_timestamp() WHERE run_id=%s",
            (run_id,),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--snapshot-dir", type=Path)
    group.add_argument("--activate-run")
    args = parser.parse_args()
    url = get_settings().database_url
    if args.activate_run:
        activate_run(args.activate_run, url)
    else:
        print(import_snapshot(args.snapshot_dir, url))


if __name__ == "__main__":
    main()
