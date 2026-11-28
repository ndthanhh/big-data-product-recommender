"""ETL T04: Bronze JSONL -> Silver Parquet, kèm vùng cách ly và báo cáo chất lượng.

Quy trình (DATA_CONTRACTS.md mục 2-3, ARCHITECTURE.md mục 3.2):

1. Đọc từng dòng Bronze ``*.jsonl`` dưới dạng văn bản để đếm được JSON lỗi,
   thay vì bị bộ đọc JSON của Spark âm thầm bỏ qua.
2. Kiểm tra từng dòng bằng hợp đồng Pydantic chung (``Product`` / ``Event``)
   trong Python UDF: dùng cùng quy tắc ID, UUID, thời gian UTC, enum, trường
   lạ và ràng buộc ``add_to_cart`` -> ``cart_id`` cho cả repo.
   UDF còn sinh SHA-256 của nội dung đã chuẩn hóa.
3. Các kiểm tra quan hệ chạy trực tiếp trong Spark: phát hiện trùng/xung đột
   theo khóa, khóa ngoại sản phẩm và quyền sở hữu phiên/giỏ hàng.
4. Ghi Silver Parquet, Parquet cách ly và ``quality/<dataset_version>.json``
   trước; ghi manifest cuối cùng để đánh dấu công bố. Xung đột chặn công bố
   (không có manifest, mã thoát 3).

``dataset_version`` được định danh từ mã kiểm tra file Bronze và
``ETL_VERSION``: chạy lại cùng đầu vào không tạo thay đổi.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from recommender.config import Settings, get_settings
from recommender.pipeline.lake import (
    LakeStore,
    create_spark_session,
    iso_z,
    json_file_entry,
    lake_store,
    parquet_file_entries,
    provenance,
    utc_now,
)
from recommender.schemas import SCHEMA_VERSION, Event, Product

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

ETL_VERSION = "1.2.0"
REJECT_REASONS = (
    "invalid_json",
    "invalid_schema",
    "invalid_timestamp",
    "unknown_product",
    "invalid_cart_owner",
    "duplicate",
    "conflicting_event_id",
    "conflicting_product_version",
)
EVENTS_PER_FILE = 500_000
_TIMESTAMP_FIELDS = {"event_at", "collected_at"}


class ConflictError(RuntimeError):
    """Ngoại lệ khi ID hoặc phiên bản xung đột khiến bộ dữ liệu Silver không thể công bố."""

    def __init__(self, dataset_version: str, report: dict[str, Any]) -> None:
        super().__init__(
            f"Silver {dataset_version} not published: conflicts quarantined, "
            "resolve them in Bronze and rerun"
        )
        self.dataset_version = dataset_version
        self.report = report


@dataclass(slots=True)
class SilverDataset:
    """Một bộ dữ liệu Silver đã công bố."""

    dataset_version: str
    manifest: dict[str, Any]
    reused: bool = False
    report: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Kiểm tra từng dòng dữ liệu (chạy trong Python UDF)
# --------------------------------------------------------------------------- #


def _classify(exc: ValidationError) -> tuple[str, str]:
    errors = exc.errors(include_url=False)
    fields_ = {str(err["loc"][0]) for err in errors if err.get("loc")}
    reason = (
        "invalid_timestamp"
        if fields_ and fields_ <= _TIMESTAMP_FIELDS
        else "invalid_schema"
    )
    detail = "; ".join(
        f"{'.'.join(str(p) for p in err.get('loc', ())) or '<root>'}: {err['msg']}"
        for err in errors[:5]
    )
    return reason, detail[:1000]


def _parse_line(line: str | None) -> tuple[str | None, str | None]:
    """Trả ``(reason, detail)`` cho dòng không phải đối tượng JSON."""
    if line is None or not line.strip():
        return "invalid_json", "empty line"
    try:
        value = json.loads(line)
    except (ValueError, RecursionError) as exc:
        return "invalid_json", str(exc)[:1000]
    if not isinstance(value, dict):
        return "invalid_schema", "record must be a JSON object"
    return None, None


def validate_event_line(line: str | None) -> dict[str, Any]:
    """Kiểm tra một dòng sự kiện Bronze; trả các trường đã chuẩn hóa hoặc lý do lỗi."""
    reason, detail = _parse_line(line)
    if reason is None:
        try:
            event = Event.model_validate_json(line)  # type: ignore[arg-type]
        except ValidationError as exc:
            reason, detail = _classify(exc)
        else:
            payload = event.model_dump_json()
            return {
                "reason": None,
                "detail": None,
                "event_id": event.event_id,
                "user_id": event.user_id,
                "session_id": event.session_id,
                "product_id": event.product_id,
                "event_type": event.event_type,
                "event_at": event.event_at,
                "source": event.source,
                "cart_id": event.cart_id,
                "payload_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            }
    return {"reason": reason, "detail": detail}


def validate_product_line(line: str | None) -> dict[str, Any]:
    """Kiểm tra một dòng sản phẩm Bronze; trả các trường đã chuẩn hóa hoặc lý do lỗi."""
    reason, detail = _parse_line(line)
    if reason is None:
        try:
            product = Product.model_validate_json(line)  # type: ignore[arg-type]
        except ValidationError as exc:
            reason, detail = _classify(exc)
        else:
            payload = product.model_dump_json()
            data = product.model_dump()
            data.update(
                reason=None,
                detail=None,
                payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            )
            return data
    return {"reason": reason, "detail": detail}


# --------------------------------------------------------------------------- #
# Các lược đồ dữ liệu Spark
# --------------------------------------------------------------------------- #


def silver_event_schema():
    """Lược đồ Spark của sự kiện Silver (DATA_CONTRACTS.md mục 3)."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("user_id", T.StringType(), False),
            T.StructField("session_id", T.StringType(), False),
            T.StructField("product_id", T.StringType(), False),
            T.StructField("event_type", T.StringType(), False),
            T.StructField("event_at", T.TimestampType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("cart_id", T.StringType(), True),
        ]
    )


def silver_product_schema():
    """Lược đồ Spark của sản phẩm Silver (DATA_CONTRACTS.md mục 3)."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("product_id", T.StringType(), False),
            T.StructField("name", T.StringType(), False),
            T.StructField("category_id", T.StringType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("collected_at", T.TimestampType(), False),
            T.StructField("description", T.StringType(), True),
            T.StructField("brand", T.StringType(), True),
            T.StructField("image_url", T.StringType(), True),
            T.StructField("product_url", T.StringType(), True),
            T.StructField(
                "attributes",
                T.MapType(T.StringType(), T.StringType(), valueContainsNull=False),
                False,
            ),
        ]
    )


def _udf_schema(base):
    from pyspark.sql import types as T

    fields = [
        T.StructField("reason", T.StringType()),
        T.StructField("detail", T.StringType()),
    ]
    fields += [T.StructField(f.name, f.dataType, True) for f in base.fields]
    fields.append(T.StructField("payload_sha256", T.StringType()))
    return T.StructType(fields)


def quarantine_schema():
    """Các dòng bị loại khỏi Silver, giữ văn bản thô để chẩn đoán lỗi."""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("reason", T.StringType(), False),
            T.StructField("detail", T.StringType(), True),
            T.StructField("record_key", T.StringType(), True),
            T.StructField("raw", T.StringType(), True),
            T.StructField("source_file", T.StringType(), True),
        ]
    )


# --------------------------------------------------------------------------- #
# Các giai đoạn xử lý Spark
# --------------------------------------------------------------------------- #


def read_bronze_lines(spark: SparkSession, uris: list[str]) -> DataFrame:
    """Đọc các dòng của đúng những file JSONL đã công bố này, kèm tên file."""
    from pyspark.sql import functions as F

    return spark.read.text(uris).select(
        F.col("value").alias("raw"),
        F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1).alias("source_file"),
    )


def _validated(lines: DataFrame, validator, base_schema) -> DataFrame:
    from pyspark.sql import functions as F

    check = F.udf(validator, _udf_schema(base_schema))
    return lines.select("raw", "source_file", check("raw").alias("r")).select(
        "raw", "source_file", "r.*"
    )


def _quarantine(df: DataFrame, reason: str | None, key: str | None) -> DataFrame:
    from pyspark.sql import functions as F

    return df.select(
        (F.lit(reason) if reason else F.col("reason")).alias("reason"),
        F.col("detail") if "detail" in df.columns else F.lit(None).cast("string"),
        (F.col(key) if key else F.lit(None)).cast("string").alias("record_key"),
        "raw",
        "source_file",
    )


def _dedup(valid: DataFrame, key_cols: list[str], conflict_reason: str):
    """Chia các dòng thành nhóm duy nhất, trùng lặp và xung đột theo khóa logic."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    by_key = Window.partitionBy(*key_cols)
    ordered = by_key.orderBy("payload_sha256", "source_file", "raw")
    marked = valid.withColumn(
        "_hashes", F.size(F.collect_set("payload_sha256").over(by_key))
    ).withColumn("_rn", F.row_number().over(ordered))
    conflict = marked.where(F.col("_hashes") > 1).withColumn(
        "detail",
        F.concat(F.lit("payload variants for key: "), F.col("_hashes").cast("string")),
    )
    clean = marked.where(F.col("_hashes") == 1)
    unique = clean.where(F.col("_rn") == 1).drop("_hashes", "_rn")
    duplicate = clean.where(F.col("_rn") > 1).withColumn(
        "detail", F.lit("identical normalized payload merged")
    )
    return (
        unique,
        duplicate.drop("_hashes", "_rn"),
        conflict.drop("_hashes", "_rn"),
        (conflict_reason),
    )


def transform_products(lines: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Các dòng sản phẩm Bronze -> (sản phẩm Silver, dữ liệu cách ly)."""
    from pyspark.sql import functions as F

    checked = _validated(lines, validate_product_line, silver_product_schema())
    invalid = _quarantine(checked.where(F.col("reason").isNotNull()), None, None)
    valid = checked.where(F.col("reason").isNull())
    unique, duplicate, conflict, reason = _dedup(
        valid, ["product_id", "collected_at"], "conflicting_product_version"
    )
    quarantine = invalid.unionByName(
        _quarantine(duplicate, "duplicate", "product_id")
    ).unionByName(_quarantine(conflict, reason, "product_id"))
    columns = [f.name for f in silver_product_schema().fields]
    return unique.select(*columns), quarantine


def transform_events(
    lines: DataFrame, product_ids: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Các dòng sự kiện Bronze -> (sự kiện Silver, dữ liệu cách ly).

    ``product_ids`` chứa các ``product_id`` duy nhất của sản phẩm Silver được
    chấp nhận, ở bất kỳ phiên bản nào. Mỗi phiên thuộc đúng một người dùng,
    mỗi giỏ thuộc đúng một cặp (người dùng, phiên). Mọi sự kiện của phiên hoặc
    giỏ vi phạm đều bị loại với lý do ``invalid_cart_owner``.
    """
    from pyspark.sql import functions as F

    checked = _validated(lines, validate_event_line, silver_event_schema())
    invalid = _quarantine(checked.where(F.col("reason").isNotNull()), None, None)
    valid = checked.where(F.col("reason").isNull())
    unique, duplicate, conflict, reason = _dedup(
        valid, ["event_id"], "conflicting_event_id"
    )

    known = unique.join(F.broadcast(product_ids), "product_id", "left_semi")
    unknown = unique.join(F.broadcast(product_ids), "product_id", "left_anti")

    bad_sessions = (
        known.groupBy("session_id")
        .agg(F.countDistinct("user_id").alias("n"))
        .where(F.col("n") > 1)
        .select("session_id")
    )
    bad_carts = (
        known.where(F.col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(F.countDistinct("user_id", "session_id").alias("n"))
        .where(F.col("n") > 1)
        .select("cart_id")
    )
    flagged = (
        known.join(bad_sessions.withColumn("_bad_s", F.lit(True)), "session_id", "left")
        .join(bad_carts.withColumn("_bad_c", F.lit(True)), "cart_id", "left")
        .withColumn(
            "_bad",
            F.coalesce(F.col("_bad_s"), F.lit(False))
            | F.coalesce(F.col("_bad_c"), F.lit(False)),
        )
    )
    owner = flagged.where("_bad").withColumn(
        "detail",
        F.when(F.col("_bad_s").isNotNull(), "session owned by several users").otherwise(
            "cart owned by several user/session pairs"
        ),
    )
    accepted = flagged.where(~F.col("_bad"))

    quarantine = (
        invalid.unionByName(_quarantine(duplicate, "duplicate", "event_id"))
        .unionByName(_quarantine(conflict, reason, "event_id"))
        .unionByName(
            _quarantine(
                unknown.withColumn("detail", F.lit("product_id not in catalog")),
                "unknown_product",
                "event_id",
            )
        )
        .unionByName(_quarantine(owner, "invalid_cart_owner", "event_id"))
    )
    columns = [f.name for f in silver_event_schema().fields]
    return accepted.select(*columns), quarantine


# --------------------------------------------------------------------------- #
# Điều phối các bước xử lý
# --------------------------------------------------------------------------- #


def _bronze_table(data: bytes, name: str) -> str | None:
    """Phân loại lô đồng bộ có phiên bản mà không giả định tên thư mục nguồn."""
    leaf = name.rsplit("/", 1)[-1]
    known = {
        "products.jsonl": "products",
        "events.jsonl": "events",
        "users.jsonl": "users",
    }
    if leaf in known:
        return known[leaf]
    for line in data.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        if not isinstance(row, dict):
            continue
        if "event_id" in row:
            return "events"
        if "product_id" in row and "name" in row and "category_id" in row:
            return "products"
        if "user_id" in row and "primary_interest" in row:
            return "users"
    return None


def bronze_inputs(
    store: LakeStore,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Tìm các lô cũ, streaming, xuất dữ liệu và đồng bộ bất biến đã được kiểm tra.

    Chỉ đọc file có trong manifest, khớp kích thước và mã băm. Bỏ qua thư mục
    tạm ẩn dùng để công bố/tải xuống ở mọi cách bố trí. Đồng bộ dữ liệu giữ
    nguyên khóa đối tượng; bảng dữ liệu được xác định từ nội dung JSONL.
    """
    inputs = []
    skipped = []
    for prefix, default_table in (
        ("products", "products"),
        ("events", "events"),
        ("demo-events", "events"),
        ("datasets", None),
    ):
        listed = {
            item.path: item
            for item in store.list_files("bronze", prefix)
            if not any(part.startswith(".") for part in item.path.split("/"))
        }
        covered = set()
        for name in sorted(listed):
            if name.rsplit("/", 1)[-1] != "manifest.json":
                continue
            folder = name.rsplit("/", 1)[0] if "/" in name else ""
            try:
                manifest_content = store.read_bytes("bronze", prefix, name)
                manifest = json.loads(manifest_content)
                entries = [
                    entry
                    for entry in manifest.get("files", [])
                    if entry.get("format") == "jsonl"
                ]
            except (ValueError, AttributeError, TypeError):
                skipped.append(
                    {"path": f"{prefix}/{name}", "reason": "invalid_manifest"}
                )
                continue
            for entry in entries:
                rel = "/".join(
                    part for part in (folder, str(entry.get("path", ""))) if part
                )
                covered.add(rel)
                item = listed.get(rel)
                if item is None or ".." in rel.split("/"):
                    skipped.append(
                        {"path": f"{prefix}/{rel}", "reason": "missing_file"}
                    )
                    continue
                data = store.read_bytes("bronze", prefix, rel)
                digest = hashlib.sha256(data).hexdigest()
                if (
                    digest != entry.get("sha256")
                    or item.size != entry.get("bytes")
                    or len(data) != item.size
                ):
                    skipped.append(
                        {"path": f"{prefix}/{rel}", "reason": "checksum_mismatch"}
                    )
                    continue
                table = default_table or _bronze_table(data, rel)
                if table not in {"products", "events"}:
                    skipped.append(
                        {
                            "path": f"{prefix}/{rel}",
                            "reason": "unsupported_bronze_table",
                        }
                    )
                    continue
                inputs.append(
                    {
                        "path": f"{prefix}/{rel}",
                        "table": table,
                        "bytes": item.size,
                        "sha256": digest,
                        "source_manifest": {
                            "path": f"{prefix}/{name}",
                            "sha256": hashlib.sha256(manifest_content).hexdigest(),
                            **{
                                key: manifest.get(key)
                                for key in (
                                    "dataset_version",
                                    "catalog_version",
                                    "generator_version",
                                    "seed",
                                    "parameters",
                                )
                            },
                        },
                    }
                )
        for name in sorted(listed):
            if name.endswith(".jsonl") and name not in covered:
                skipped.append({"path": f"{prefix}/{name}", "reason": "no_manifest"})
    return sorted(inputs, key=lambda item: item["path"]), skipped


def dataset_version_for(inputs: list[dict[str, Any]]) -> str:
    """Phiên bản bộ dữ liệu Silver được định danh theo nội dung."""
    digest = hashlib.sha256(f"etl={ETL_VERSION}\n".encode())
    for item in inputs:
        digest.update(f"{item['path']}\t{item['sha256']}\n".encode())
    return f"ds-{digest.hexdigest()[:24]}"


def bronze_provenance(inputs: list[dict[str, Any]]) -> dict[str, Any]:
    """Chỉ tổng hợp manifest có đóng góp file sản phẩm hoặc sự kiện được chấp nhận."""
    sources = {}
    for item in inputs:
        metadata = item.get("source_manifest")
        if metadata:
            sources[(item["table"], metadata["path"], metadata["sha256"])] = metadata
    product_sources = [
        value for (table, *_), value in sorted(sources.items()) if table == "products"
    ]
    event_sources = [
        value for (table, *_), value in sorted(sources.items()) if table == "events"
    ]
    catalogs = sorted(
        {
            value["catalog_version"]
            for value in product_sources
            if value.get("catalog_version")
        }
    )
    versions = {
        table: sorted(
            {
                value["dataset_version"]
                for (kind, *_), value in sources.items()
                if kind == table and value.get("dataset_version")
            }
        )
        for table in ("products", "events")
    }
    seeds = sorted(
        {value["seed"] for value in event_sources if type(value.get("seed")) is int}
    )
    generators = sorted(
        {
            value["generator_version"]
            for value in event_sources
            if value.get("generator_version")
        }
    )
    return {
        "catalog_version": catalogs[0]
        if len(catalogs) == 1
        else f"catalog-{dataset_version_for([item for item in inputs if item['table'] == 'products'])}",
        "catalog_versions": catalogs,
        "bronze_dataset_versions": versions,
        "seeds": seeds,
        "generator_versions": generators,
        "generator_parameters": [
            value["parameters"]
            for value in event_sources
            if value.get("parameters") is not None
        ],
    }


def manifest_name(dataset_version: str) -> str:
    """Tên đối tượng manifest ở gốc Silver để các đường dẫn bên trong vẫn là tương đối."""
    return f"{dataset_version}.manifest.json"


def _bronze_manifest(store: LakeStore, table: str) -> dict[str, Any] | None:
    if store.exists("bronze", table, "manifest.json"):
        return store.read_json("bronze", table, "manifest.json")
    return None


def _reason_counts(quarantine: DataFrame) -> dict[str, int]:
    rows = quarantine.groupBy("reason").count().collect()
    counts = {reason: 0 for reason in REJECT_REASONS}
    counts.update({row["reason"]: int(row["count"]) for row in rows})
    return counts


def run_etl(
    spark: SparkSession,
    store: LakeStore,
    *,
    settings: Settings | None = None,
) -> SilverDataset:
    """Công bố hoặc dùng lại bộ dữ liệu Silver từ các file Bronze hiện tại."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    inputs, skipped = bronze_inputs(store)
    product_uris = [
        store.uri("bronze", i["path"]) for i in inputs if i["table"] == "products"
    ]
    event_uris = [
        store.uri("bronze", i["path"]) for i in inputs if i["table"] == "events"
    ]
    if not product_uris:
        raise FileNotFoundError(
            "No published Bronze product JSONL (with manifest) under bronze/products/"
        )
    version = dataset_version_for(inputs)
    if store.exists("silver", manifest_name(version)):
        manifest = store.read_json("silver", manifest_name(version))
        return SilverDataset(version, manifest, reused=True)

    products, product_q = transform_products(read_bronze_lines(spark, product_uris))
    products = products.persist(StorageLevel.MEMORY_AND_DISK)
    product_q = product_q.persist(StorageLevel.MEMORY_AND_DISK)
    if event_uris:
        events, event_q = transform_events(
            read_bronze_lines(spark, event_uris),
            products.select("product_id").distinct(),
        )
    else:
        events = spark.createDataFrame([], silver_event_schema())
        event_q = spark.createDataFrame([], quarantine_schema())
    events = events.persist(StorageLevel.MEMORY_AND_DISK)
    event_q = event_q.persist(StorageLevel.MEMORY_AND_DISK)

    n_products, n_events = products.count(), events.count()
    product_reasons, event_reasons = _reason_counts(product_q), _reason_counts(event_q)
    conflicts = (
        product_reasons["conflicting_product_version"]
        + event_reasons["conflicting_event_id"]
    )

    # Ghi dữ liệu trước, ghi manifest sau cùng để đánh dấu công bố.
    products.orderBy("product_id", "collected_at").coalesce(1).write.mode(
        "overwrite"
    ).parquet(store.uri("silver", "products", version))
    n_files = max(1, math.ceil(n_events / EVENTS_PER_FILE))
    events.orderBy("event_at", "event_id").coalesce(n_files).write.mode(
        "overwrite"
    ).parquet(store.uri("silver", "events", version))
    product_q.coalesce(1).write.mode("overwrite").parquet(
        store.uri("silver", "quarantine", version, "products")
    )
    event_q.coalesce(1).write.mode("overwrite").parquet(
        store.uri("silver", "quarantine", version, "events")
    )

    def table_report(input_rows: int, accepted: int, reasons: dict[str, int]) -> dict:
        rejected = {k: v for k, v in reasons.items() if k != "duplicate"}
        return {
            "input_rows": input_rows,
            "accepted": accepted,
            "duplicate": reasons["duplicate"],
            "rejected": rejected,
            "rejected_total": sum(rejected.values()),
        }

    stats = events.agg(
        F.min("event_at").alias("tmin"),
        F.max("event_at").alias("tmax"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("session_id").alias("sessions"),
        F.countDistinct("cart_id").alias("carts"),
    ).first()
    by_type = {
        r["event_type"]: int(r["count"])
        for r in events.groupBy("event_type").count().collect()
    }
    by_source = {
        r["source"]: int(r["count"]) for r in events.groupBy("source").count().collect()
    }
    n_product_ids = products.select("product_id").distinct().count()

    report = {
        "schema_version": SCHEMA_VERSION,
        "dataset_version": version,
        "created_at": utc_now(),
        "etl_version": ETL_VERSION,
        "publishable": conflicts == 0,
        "bronze_files_read": len(inputs),
        "bronze_files_skipped": skipped,
        "products": table_report(
            n_products + sum(product_reasons.values()), n_products, product_reasons
        ),
        "events": table_report(
            n_events + sum(event_reasons.values()), n_events, event_reasons
        ),
    }
    quality_path = f"quality/{version}.json"
    store.write_json("silver", quality_path, value=report)
    if conflicts:
        products.unpersist(), events.unpersist()
        raise ConflictError(version, report)

    source_metadata = bronze_provenance(inputs)
    commit, dirty = provenance()
    files = (
        parquet_file_entries(
            spark, store, "silver", "", f"products/{version}", "products"
        )
        + parquet_file_entries(
            spark, store, "silver", "", f"events/{version}", "events"
        )
        + parquet_file_entries(
            spark,
            store,
            "silver",
            "",
            f"quarantine/{version}/products",
            "quarantine_products",
        )
        + parquet_file_entries(
            spark,
            store,
            "silver",
            "",
            f"quarantine/{version}/events",
            "quarantine_events",
        )
        + [json_file_entry(store, "silver", "", quality_path)]
    )
    if sum(f["rows"] for f in files if f.get("table") == "events") != n_events:
        raise RuntimeError("Silver events row count does not match its files")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "dataset_version": version,
        "created_at": utc_now(),
        "code_commit": commit,
        "code_dirty": dirty,
        "generator_version": source_metadata["generator_versions"][0]
        if len(source_metadata["generator_versions"]) == 1
        else None,
        "catalog_version": source_metadata["catalog_version"],
        "seed": source_metadata["seeds"][0]
        if len(source_metadata["seeds"]) == 1
        else None,
        "parameters": {
            "etl_version": ETL_VERSION,
            "bronze_inputs": inputs,
            **source_metadata,
        },
        "counts": {
            "product_ids": n_product_ids,
            "catalog_versions": n_products,
            "users": int(stats["users"]),
            "sessions": int(stats["sessions"]),
            "carts": int(stats["carts"]),
            "events": n_events,
            "events_by_type": dict(sorted(by_type.items())),
            "events_by_source": dict(sorted(by_source.items())),
        },
        "event_time_min": iso_z(stats["tmin"]) if stats["tmin"] else None,
        "event_time_max": iso_z(stats["tmax"]) if stats["tmax"] else None,
        "quality_report": quality_path,
        "files": files,
    }
    store.write_json("silver", manifest_name(version), value=manifest)
    for frame in (products, events, product_q, event_q):
        frame.unpersist()
    return SilverDataset(version, manifest, report=report)


def list_silver_versions(store: LakeStore) -> list[dict[str, Any]]:
    """Các manifest Silver đã công bố, sắp từ cũ đến mới."""
    manifests = [
        store.read_json("silver", item.path)
        for item in store.list_files("silver")
        if "/" not in item.path and item.path.endswith(".manifest.json")
    ]
    return sorted(manifests, key=lambda m: (m["created_at"], m["dataset_version"]))


def load_silver(
    spark: SparkSession, store: LakeStore, dataset_version: str | None = None
) -> tuple[DataFrame, DataFrame, dict[str, Any]]:
    """Đọc bộ dữ liệu Silver đã công bố; lấy bản mới nhất nếu không chỉ định phiên bản."""
    if dataset_version is None:
        published = list_silver_versions(store)
        if not published:
            raise FileNotFoundError("No published Silver dataset; run the ETL first")
        manifest = published[-1]
        dataset_version = manifest["dataset_version"]
    else:
        if not store.exists("silver", manifest_name(dataset_version)):
            raise FileNotFoundError(
                f"Silver dataset {dataset_version} is not published"
            )
        manifest = store.read_json("silver", manifest_name(dataset_version))
    products = spark.read.schema(silver_product_schema()).parquet(
        store.uri("silver", "products", dataset_version)
    )
    events = spark.read.schema(silver_event_schema()).parquet(
        store.uri("silver", "events", dataset_version)
    )
    return products, events, manifest


def main(argv: list[str] | None = None) -> int:
    """Chạy ``python -m recommender.pipeline.etl``."""
    parser = argparse.ArgumentParser(description="Bronze JSONL -> Silver Parquet")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="1g")
    args = parser.parse_args(argv)
    settings = get_settings()
    store = lake_store(settings)
    spark = create_spark_session(
        "silver-etl", settings, master=args.master, driver_memory=args.driver_memory
    )
    try:
        result = run_etl(spark, store, settings=settings)
    except ConflictError as exc:
        print(json.dumps(exc.report, indent=2, ensure_ascii=False))
        print(f"etl: {exc}", file=sys.stderr)
        return 3
    finally:
        spark.stop()
    state = "reused" if result.reused else "published"
    counts = result.manifest["counts"]
    print(
        f"Silver {result.dataset_version} {state}: "
        f"{counts['catalog_versions']} product versions, {counts['events']} events"
    )
    if result.report:
        print(json.dumps(result.report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
