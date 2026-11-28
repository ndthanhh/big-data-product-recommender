"""Nhận sự kiện Kafka và lưu bền vững thành các lô Bronze nhỏ trên máy hoặc MinIO."""

import argparse
import logging
import math
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from minio.error import MinioException
from urllib3.exceptions import HTTPError

from recommender.config import get_settings
from recommender.ingestion.bronze import write_bronze
from recommender.schemas import Event

LOGGER = logging.getLogger(__name__)


def create_minio_client() -> Any:
    """Giới hạn thời gian kết nối và đọc để chuyển sang lưu cục bộ nhanh khi mất kết nối."""
    from minio import Minio
    from urllib3 import PoolManager, Timeout

    settings = get_settings()
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
        http_client=PoolManager(timeout=Timeout(connect=0.5, read=2), retries=False),
    )


class BronzeSink:
    """Giữ các lô cục bộ hoàn chỉnh và tùy chọn sao chép lên bucket Bronze.

    Mỗi lô có thư mục và manifest riêng; khởi động lại không ghi đè lô đã
    công bố. Kafka có thể phát lại ít nhất một lần; ETL loại trùng event_id.
    Lỗi tải lên MinIO vẫn giữ lô cục bộ để phát lại sau.
    """

    def __init__(
        self,
        output_dir: str | Path,
        *,
        minio_client: Any = None,
        local_only: bool = False,
        topic: str = "product-events",
    ) -> None:
        self.output_dir = Path(output_dir)
        self.minio_client = minio_client
        self.local_only = local_only
        self.topic = topic

    def write_batch(self, events: Sequence[Event]) -> Path:
        """Công bố dữ liệu và manifest cục bộ trước; khi tải lên, gửi manifest sau cùng."""
        if not events:
            raise ValueError("Cannot publish an empty micro-batch")
        batch_id = f"batch_{uuid4()}"
        path = write_bronze(
            events,
            self.output_dir / batch_id / "events.jsonl",
            source="kafka",
            seed=None,
            products=[],
            parameters={"topic": self.topic, "catalog_validation": "deferred_to_etl"},
        )
        if not self.local_only:
            try:
                if self.minio_client is None:
                    self.minio_client = create_minio_client()
                if not self.minio_client.bucket_exists("bronze"):
                    self.minio_client.make_bucket("bronze")
                for file in (path, path.parent / "manifest.json"):
                    self.minio_client.fput_object(
                        "bronze",
                        f"events/streaming/{batch_id}/{file.name}",
                        str(file),
                        content_type="application/x-ndjson"
                        if file.suffix == ".jsonl"
                        else "application/json",
                    )
            except (ImportError, OSError, MinioException, HTTPError) as exc:
                # Chỉ bắt lỗi ở bước sao chép từ xa tùy chọn; lỗi ghi cục bộ
                # phải được chuyển tiếp và ngăn commit offset Kafka.
                LOGGER.warning(
                    "MinIO upload unavailable (%s); retained local batch %s",
                    type(exc).__name__,
                    path,
                )
        return path


def create_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str) -> Any:
    """Đọc byte JSON thô và chỉ commit offset rõ ràng sau khi lưu bền vững."""
    from kafka import KafkaConsumer

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        api_version_auto_timeout_ms=5000,
    )


def consume_events(
    kafka_consumer: Any,
    sink: BronzeSink,
    *,
    batch_size: int = 100,
    flush_interval: float = 5,
    max_events: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    should_stop: Callable[[], bool] | None = None,
) -> int:
    """Ghi lô theo kích thước/thời gian và khi Ctrl+C; chỉ commit offset đã lưu thành công.

    Chỉ rõ offset cho từng phân vùng để tránh xác nhận thừa bản ghi được lấy
    ở lần poll cuối. Nội dung không hợp lệ hoặc lỗi lưu trữ sẽ dừng mà không
    commit lô đó, cho phép phát lại sau khi khắc phục nguyên nhân.
    """
    from kafka.structs import OffsetAndMetadata, TopicPartition

    if (
        batch_size <= 0
        or not math.isfinite(flush_interval)
        or flush_interval <= 0
        or (max_events is not None and max_events < 0)
    ):
        raise ValueError(
            "batch-size/flush-interval must be positive; max-events nonnegative"
        )
    batch: list[Event] = []
    offsets: dict[Any, Any] = {}
    processed = 0
    last_flush = monotonic()

    def flush() -> None:
        nonlocal last_flush
        if batch:
            sink.write_batch(tuple(batch))
            kafka_consumer.commit(offsets=dict(offsets))
            batch.clear()
            offsets.clear()
            last_flush = monotonic()

    try:
        while max_events is None or processed < max_events:
            if should_stop is not None and should_stop():
                break
            until_flush = flush_interval - (monotonic() - last_flush)
            timeout_ms = max(1, min(1000, int(until_flush * 1000))) if batch else 1000
            remaining = batch_size - len(batch)
            if max_events is not None:
                remaining = min(remaining, max_events - processed)
            polled = kafka_consumer.poll(timeout_ms=timeout_ms, max_records=remaining)
            for records in polled.values():
                for message in records:
                    if max_events is not None and processed >= max_events:
                        break
                    event = Event.model_validate_json(message.value)
                    batch.append(event)
                    offsets[TopicPartition(message.topic, message.partition)] = (
                        OffsetAndMetadata(message.offset + 1, "")
                    )
                    processed += 1
                    if len(batch) >= batch_size:
                        flush()
                if max_events is not None and processed >= max_events:
                    break
            if monotonic() - last_flush >= flush_interval:
                flush()
    except KeyboardInterrupt:
        flush()
    else:
        flush()
    finally:
        try:
            kafka_consumer.close(autocommit=False)
        except OSError:
            pass
    return processed


def main(argv: list[str] | None = None) -> None:
    """Chạy python -m recommender.streaming.consumer; Ctrl+C ghi nốt dữ liệu rồi dừng."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic_events)
    parser.add_argument("--group-id", default="bronze-writer")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=settings.data_lake_dir / "bronze/events/streaming",
    )
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--flush-interval", type=float, default=5)
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Keep local Bronze without attempting MinIO",
    )
    args = parser.parse_args(argv)
    if (
        args.batch_size <= 0
        or not math.isfinite(args.flush_interval)
        or args.flush_interval <= 0
        or (args.max_events is not None and args.max_events < 0)
    ):
        parser.error(
            "batch-size/flush-interval must be positive; max-events nonnegative"
        )
    if args.max_events == 0:
        print("Stored 0 events")
        return
    try:
        client = create_kafka_consumer(
            args.bootstrap_servers, args.topic, args.group_id
        )
        sink = BronzeSink(args.output_dir, local_only=args.local_only, topic=args.topic)
        count = consume_events(
            client,
            sink,
            batch_size=args.batch_size,
            flush_interval=args.flush_interval,
            max_events=args.max_events,
        )
    except (ValueError, OSError, ImportError) as exc:
        parser.exit(2, f"consumer: {exc}\n")
    print(f"Stored {count} events")


if __name__ == "__main__":
    main()
