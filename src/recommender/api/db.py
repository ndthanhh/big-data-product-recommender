"""Short-lived UTC database connections; no connections at import time."""

import psycopg


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, options="-c timezone=UTC", connect_timeout=5)
