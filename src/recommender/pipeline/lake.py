"""Data Lake access shared by ETL, ranking and snapshot export.

Spark reads and writes Parquet/JSONL through URIs: local paths under
``DATA_LAKE_DIR`` or ``s3a://<layer>/...`` buckets on MinIO. Small control files
(manifests, quality reports, checksums) go through :class:`LakeStore`, which
uses the same layout on both backends, so a manifest written locally and one
written to MinIO list identical relative paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from recommender.config import PROJECT_ROOT, Settings, get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LAYERS = ("bronze", "silver", "gold")
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def utc_now() -> str:
    """Current time as ISO 8601 UTC with the ``Z`` suffix."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def to_utc(value: datetime) -> datetime:
    """Aware UTC datetime from a Spark-collected value.

    PySpark returns ``TimestampType`` as *naive* datetimes in the Python
    process's local timezone; ``astimezone`` interprets naive values the same
    way, so this is correct on a UTC server and on a UTC+7 laptop alike.
    """
    return value.astimezone(UTC)


def iso_z(value: datetime) -> str:
    """Serialize a datetime (aware, or Spark-collected naive local) as UTC ``Z``."""
    return to_utc(value).isoformat().replace("+00:00", "Z")


def provenance() -> tuple[str, bool]:
    """Return ``(git commit, dirty)``; ``("unknown", True)`` outside a checkout."""
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


@dataclass(frozen=True, slots=True)
class LakeFile:
    """One object below a lake prefix."""

    path: str  # relative to the listed prefix, '/' separated
    size: int


class LakeStore(ABC):
    """Minimal object-store interface for control files and checksums."""

    @abstractmethod
    def uri(self, layer: str, *parts: str) -> str:
        """Spark-readable URI of ``layer/parts``."""

    @abstractmethod
    def exists(self, layer: str, *parts: str) -> bool:
        """Whether an object exists."""

    @abstractmethod
    def read_bytes(self, layer: str, *parts: str) -> bytes:
        """Read one object."""

    @abstractmethod
    def write_bytes(self, layer: str, *parts: str, data: bytes) -> None:
        """Write one object atomically from a reader's perspective."""

    @abstractmethod
    def list_files(self, layer: str, *parts: str) -> list[LakeFile]:
        """Recursively list objects below a prefix, sorted by relative path."""

    def read_json(self, layer: str, *parts: str) -> Any:
        """Read a UTF-8 JSON document."""
        return json.loads(self.read_bytes(layer, *parts).decode("utf-8"))

    def write_json(self, layer: str, *parts: str, value: object) -> None:
        """Write UTF-8/LF JSON, rejecting NaN and infinities."""
        text = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
        self.write_bytes(layer, *parts, data=(text + "\n").encode("utf-8"))

    def sha256(self, layer: str, *parts: str) -> str:
        """SHA-256 of the stored bytes."""
        return hashlib.sha256(self.read_bytes(layer, *parts)).hexdigest()


def _check_parts(layer: str, parts: tuple[str, ...]) -> None:
    if layer not in LAYERS:
        raise ValueError(f"Unknown lake layer: {layer}")
    for part in parts:
        pure = PurePosixPath(part)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError(f"Lake paths must be relative: {part}")


class LocalLakeStore(LakeStore):
    """Lake rooted at ``DATA_LAKE_DIR`` (``data/`` by default)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, layer: str, *parts: str) -> Path:
        _check_parts(layer, parts)
        return self.root.joinpath(layer, *parts)

    def uri(self, layer: str, *parts: str) -> str:
        return str(self._path(layer, *parts))

    def exists(self, layer: str, *parts: str) -> bool:
        return self._path(layer, *parts).exists()

    def read_bytes(self, layer: str, *parts: str) -> bytes:
        return self._path(layer, *parts).read_bytes()

    def write_bytes(self, layer: str, *parts: str, data: bytes) -> None:
        path = self._path(layer, *parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=path.parent, delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(data)
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def list_files(self, layer: str, *parts: str) -> list[LakeFile]:
        base = self._path(layer, *parts)
        if not base.exists():
            return []
        files = [
            LakeFile(path.relative_to(base).as_posix(), path.stat().st_size)
            for path in base.rglob("*")
            if path.is_file()
        ]
        return sorted(files, key=lambda item: item.path)


class MinioLakeStore(LakeStore):
    """Lake on MinIO: one bucket per layer, accessed by Spark through s3a://."""

    def __init__(self, settings: Settings) -> None:
        from minio import Minio

        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

    @staticmethod
    def _key(parts: tuple[str, ...]) -> str:
        return "/".join(part.strip("/") for part in parts if part)

    def uri(self, layer: str, *parts: str) -> str:
        _check_parts(layer, parts)
        return f"s3a://{layer}/{self._key(parts)}"

    def exists(self, layer: str, *parts: str) -> bool:
        from minio.error import S3Error

        _check_parts(layer, parts)
        try:
            self.client.stat_object(layer, self._key(parts))
            return True
        except S3Error:
            return any(True for _ in self._objects(layer, self._key(parts)))

    def read_bytes(self, layer: str, *parts: str) -> bytes:
        _check_parts(layer, parts)
        response = self.client.get_object(layer, self._key(parts))
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def write_bytes(self, layer: str, *parts: str, data: bytes) -> None:
        import io

        _check_parts(layer, parts)
        if not self.client.bucket_exists(layer):
            self.client.make_bucket(layer)
        self.client.put_object(layer, self._key(parts), io.BytesIO(data), len(data))

    def _objects(self, layer: str, prefix: str) -> Iterator[Any]:
        if not self.client.bucket_exists(layer):
            return iter(())
        return self.client.list_objects(
            layer, prefix=prefix.rstrip("/") + "/" if prefix else "", recursive=True
        )

    def list_files(self, layer: str, *parts: str) -> list[LakeFile]:
        _check_parts(layer, parts)
        prefix = self._key(parts)
        files = []
        for item in self._objects(layer, prefix):
            relative = (
                item.object_name[len(prefix) :].lstrip("/")
                if prefix
                else (item.object_name)
            )
            if relative and not item.is_dir:
                files.append(LakeFile(relative, int(item.size or 0)))
        return sorted(files, key=lambda item: item.path)


def lake_store(settings: Settings | None = None) -> LakeStore:
    """Return the store selected by ``DATA_LAKE_STORAGE``."""
    settings = settings or get_settings()
    if settings.data_lake_storage == "minio":
        return MinioLakeStore(settings)
    return LocalLakeStore(settings.data_lake_dir)


def create_spark_session(
    app_name: str,
    settings: Settings | None = None,
    *,
    master: str = "local[2]",
    shuffle_partitions: int = 4,
    driver_memory: str = "1g",
) -> SparkSession:
    """Create the project's Spark session: UTC, small shuffle, optional S3A.

    The configuration matches EXPERIMENT_PLAN.md section 9 so every stage of
    the pipeline measures and writes under the same timezone and partitioning.
    """
    from pyspark.sql import SparkSession

    settings = settings or get_settings()
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )
    if settings.data_lake_storage == "minio":
        scheme = "https" if settings.minio_secure else "http"
        builder = (
            builder.config("spark.jars.packages", HADOOP_AWS_PACKAGE)
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                f"{scheme}://{settings.minio_endpoint}",
            )
            .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.minio_secure).lower(),
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def spark_environment(spark: SparkSession) -> dict[str, Any]:
    """Runtime details recorded in manifests (EXPERIMENT_PLAN.md section 9)."""
    conf = spark.sparkContext.getConf()
    keys = (
        "spark.master",
        "spark.driver.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.session.timeZone",
    )
    java = spark.sparkContext._jvm.System.getProperty("java.version")  # type: ignore[union-attr]
    return {
        "python": sys.version.split()[0],
        "java": java,
        "spark": spark.version,
        "os": f"{platform.system()} {platform.release()} {platform.machine()}",
        "spark_conf": {key: conf.get(key, None) for key in keys},
    }


def _is_data_file(name: str) -> bool:
    leaf = name.rsplit("/", 1)[-1]
    return not (leaf.startswith(("_", ".")) or leaf.endswith(".crc"))


def parquet_file_entries(
    spark: SparkSession,
    store: LakeStore,
    layer: str,
    manifest_dir: str,
    relative_dir: str,
    table: str | None = None,
) -> list[dict[str, Any]]:
    """Manifest entries (path, rows, bytes, sha256) for one Parquet directory.

    Rows per file come from Spark's ``input_file_name`` so no extra Parquet
    library is required; paths are relative to ``manifest_dir``.
    """
    from pyspark.sql import functions as F

    root = "/".join(part for part in (manifest_dir, relative_dir) if part)
    files = [item for item in store.list_files(layer, root) if _is_data_file(item.path)]
    if not files:
        return []
    counts = {
        row["file"].rstrip("/").rsplit("/", 1)[-1]: row["rows"]
        for row in spark.read.parquet(store.uri(layer, root))
        .groupBy(F.input_file_name().alias("file"))
        .agg(F.count(F.lit(1)).alias("rows"))
        .collect()
    }
    entries = []
    for item in files:
        leaf = item.path.rsplit("/", 1)[-1]
        entry: dict[str, Any] = {
            "path": f"{relative_dir}/{item.path}" if relative_dir else item.path,
            "format": "parquet",
            "rows": int(counts.get(leaf, 0)),
            "bytes": item.size,
            "sha256": store.sha256(layer, root, item.path),
        }
        if table is not None:
            entry["table"] = table
        entries.append(entry)
    return entries


def json_file_entry(
    store: LakeStore, layer: str, manifest_dir: str, relative_path: str
) -> dict[str, Any]:
    """Manifest entry for one JSON control file below ``manifest_dir``."""
    parts = [part for part in (manifest_dir, relative_path) if part]
    data = store.read_bytes(layer, *parts)
    return {
        "path": relative_path,
        "format": "json",
        "rows": None,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
