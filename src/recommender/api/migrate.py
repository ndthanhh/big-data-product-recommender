"""Apply packaged PostgreSQL migrations, once, in a serialized transaction."""

from importlib.resources import files

from recommender.api.db import connect
from recommender.config import get_settings


def migrate(database_url: str) -> None:
    with connect(database_url) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(651829301)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        for path in sorted(
            files("recommender.api").joinpath("migrations").iterdir(),
            key=lambda item: item.name,
        ):
            if (
                path.name.endswith(".sql")
                and not conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version=%s", (path.name,)
                ).fetchone()
            ):
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute(
                    "INSERT INTO schema_migrations(version) VALUES (%s)", (path.name,)
                )


if __name__ == "__main__":
    migrate(get_settings().database_url)
