"""Run producer and consumer with cooperative shutdown and failure propagation."""

import argparse
import logging
import math
import threading
from collections.abc import Callable
from pathlib import Path
from queue import Queue

from recommender.config import get_settings
from recommender.streaming.consumer import (
    BronzeSink,
    consume_events,
    create_kafka_consumer,
)
from recommender.streaming.producer import run_producer


def run_workers(
    producer: Callable[[], None],
    consumer: Callable[[], None],
    stopped: threading.Event,
) -> None:
    """Stop both workers on completion, failure or interrupt, then report failures."""
    failures: Queue[Exception] = Queue()

    def invoke(action: Callable[[], None]) -> None:
        try:
            action()
        except Exception as exc:  # noqa: BLE001 -- propagate worker failures to main
            failures.put(exc)
        finally:
            stopped.set()

    threads = [
        threading.Thread(target=invoke, args=(action,), name=name)
        for name, action in (("consumer", consumer), ("producer", producer))
    ]
    for thread in threads:
        thread.start()
    try:
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                thread.join(0.2)
    except KeyboardInterrupt:
        stopped.set()
    finally:
        for thread in threads:
            thread.join()
    if not failures.empty():
        error = failures.get()
        raise RuntimeError(f"Streaming worker failed: {error}") from error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--flush-interval", type=float, default=5)
    parser.add_argument("--stop-file", type=Path)
    args = parser.parse_args()
    if (
        not math.isfinite(args.rate)
        or args.rate <= 0
        or args.batch_size <= 0
        or not math.isfinite(args.flush_interval)
        or args.flush_interval <= 0
    ):
        parser.error("rate, batch-size and flush-interval must be finite and positive")
    settings = get_settings()
    catalog = settings.data_lake_dir / "bronze/products/products.jsonl"
    users = settings.data_lake_dir / "bronze/users/users.jsonl"
    if not catalog.is_file() or not users.is_file():
        parser.error("Catalog and user files are required")
    stopped = threading.Event()

    def should_stop() -> bool:
        return stopped.is_set() or bool(args.stop_file and args.stop_file.exists())

    def consume() -> None:
        client = create_kafka_consumer(
            settings.kafka_bootstrap_servers,
            settings.kafka_topic_events,
            group_id="bronze-continuous-worker",
        )
        consume_events(
            client,
            BronzeSink(
                settings.data_lake_dir / "bronze/events/streaming",
                topic=settings.kafka_topic_events,
            ),
            batch_size=args.batch_size,
            flush_interval=args.flush_interval,
            should_stop=should_stop,
        )

    def produce() -> None:
        run_producer(
            catalog,
            users,
            rate=args.rate,
            should_stop=should_stop,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic_events,
        )

    logging.basicConfig(level=logging.INFO)
    run_workers(produce, consume, stopped)


if __name__ == "__main__":
    main()
