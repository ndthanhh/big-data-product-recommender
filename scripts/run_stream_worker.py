"""Unified Background Streaming Worker: Producer + Consumer in one process."""

import argparse
import logging
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from recommender.config import get_settings
from recommender.streaming.consumer import BronzeSink, consume_events, create_kafka_consumer
from recommender.streaming.producer import run_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
)
LOGGER = logging.getLogger("stream_worker")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--batch-size", type=int, default=100, help="Flush batch size")
    parser.add_argument(
        "--flush-interval", type=float, default=5.0, help="Flush interval in seconds"
    )
    args = parser.parse_args()

    settings = get_settings()
    catalog_path = settings.data_lake_dir / "bronze/products/products.jsonl"
    users_path = settings.data_lake_dir / "bronze/users/users.jsonl"

    if not catalog_path.exists() or not users_path.exists():
        LOGGER.error("Catalog or users not found. Run publish_catalog first.")
        sys.exit(1)

    stop_event = threading.Event()

    def consumer_thread():
        LOGGER.info("Starting Consumer thread...")
        client = create_kafka_consumer(
            settings.kafka_bootstrap_servers,
            settings.kafka_topic_events,
            group_id="bronze-continuous-worker",
        )
        sink = BronzeSink(
            settings.data_lake_dir / "bronze/events/streaming",
            topic=settings.kafka_topic_events,
        )
        try:
            consume_events(
                client,
                sink,
                batch_size=args.batch_size,
                flush_interval=args.flush_interval,
            )
        except Exception as e:
            LOGGER.error("Consumer stopped with error: %s", e)

    def producer_thread():
        LOGGER.info("Starting Producer thread (rate=%.1f events/s)...", args.rate)
        try:
            run_producer(
                catalog_path,
                users_path,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                topic=settings.kafka_topic_events,
                rate=args.rate,
            )
        except Exception as e:
            LOGGER.error("Producer stopped with error: %s", e)

    t_cons = threading.Thread(target=consumer_thread, name="ConsumerThread", daemon=True)
    t_prod = threading.Thread(target=producer_thread, name="ProducerThread", daemon=True)

    t_cons.start()
    time.sleep(1)
    t_prod.start()

    LOGGER.info("Continuous Streaming Worker is ACTIVE!")
    LOGGER.info("Streaming to MinIO S3: s3a://bronze/events/streaming/")
    LOGGER.info("Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOGGER.info("Stopping worker...")


if __name__ == "__main__":
    main()
