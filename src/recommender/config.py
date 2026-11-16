"""Load local configuration without changing the process environment."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATABASE_URL = "postgresql://recommender:change-me@127.0.0.1:5432/recommender"
DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_RANDOM_SEED = 42
DEFAULT_MINIO_ENDPOINT = "127.0.0.1:9000"
DEFAULT_MINIO_CONSOLE_URL = "http://127.0.0.1:9001"
DEFAULT_MINIO_ACCESS_KEY = "minioadmin"
DEFAULT_MINIO_SECRET_KEY = "minioadmin"
DEFAULT_MINIO_SECURE = False


@dataclass(frozen=True, slots=True)
class Settings:
    """Configuration shared by the offline pipeline and serving applications."""

    data_lake_dir: Path = PROJECT_ROOT / "data"
    database_url: str = DEFAULT_DATABASE_URL
    api_base_url: str = DEFAULT_API_BASE_URL
    random_seed: int = DEFAULT_RANDOM_SEED
    minio_endpoint: str = DEFAULT_MINIO_ENDPOINT
    minio_console_url: str = DEFAULT_MINIO_CONSOLE_URL
    minio_access_key: str = DEFAULT_MINIO_ACCESS_KEY
    minio_secret_key: str = DEFAULT_MINIO_SECRET_KEY
    minio_secure: bool = DEFAULT_MINIO_SECURE


def get_settings(env_file: str | Path | None = PROJECT_ROOT / ".env") -> Settings:
    """Read defaults < .env < environment; resolve local data paths from the repo.

    Pass ``None`` to disable .env loading. Configuration is read on each call so
    importing this module neither mutates os.environ nor caches stale settings.
    """
    file_values = (
        dotenv_values(env_file, encoding="utf-8") if env_file is not None else {}
    )

    def read(key: str, default: str) -> str:
        value = os.environ.get(key, file_values.get(key, default))
        if value is None or not value.strip():
            raise ValueError(f"{key} must not be empty")
        return value

    data_lake_dir = Path(read("DATA_LAKE_DIR", "data"))
    if not data_lake_dir.is_absolute():
        data_lake_dir = PROJECT_ROOT / data_lake_dir
    try:
        random_seed = int(read("RANDOM_SEED", str(DEFAULT_RANDOM_SEED)))
    except ValueError as exc:
        raise ValueError("RANDOM_SEED must be an integer") from exc

    minio_secure_str = read("MINIO_SECURE", "false").lower()
    minio_secure = minio_secure_str in ("true", "1", "yes")

    return Settings(
        data_lake_dir=data_lake_dir.resolve(),
        database_url=read("DATABASE_URL", DEFAULT_DATABASE_URL),
        api_base_url=read("API_BASE_URL", DEFAULT_API_BASE_URL),
        random_seed=random_seed,
        minio_endpoint=read("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
        minio_console_url=read("MINIO_CONSOLE_URL", DEFAULT_MINIO_CONSOLE_URL),
        minio_access_key=read("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY),
        minio_secret_key=read("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY),
        minio_secure=minio_secure,
    )
