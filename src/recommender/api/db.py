"""Kết nối cơ sở dữ liệu ngắn hạn theo UTC; không mở kết nối khi import mô-đun."""

from contextlib import contextmanager

import psycopg

from recommender.api.errors import error


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, options="-c timezone=UTC", connect_timeout=5)


def active_run(conn) -> str:
    run = conn.execute(
        "SELECT active_run_id FROM serving_state WHERE singleton_id=1"
    ).fetchone()[0]
    if run is None:
        raise error(503, "SNAPSHOT_UNAVAILABLE", "Chưa có snapshot phục vụ")
    return run


@contextmanager
def read_snapshot(database_url: str):
    with connect(database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        yield conn, active_run(conn)
