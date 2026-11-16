"""Streaming contracts and Kafka/S3 boundaries run entirely offline."""

import json
from datetime import UTC, datetime, timedelta
from itertools import islice
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from kafka.structs import TopicPartition

from recommender.ingestion.synthetic import load_catalog
from recommender.ingestion.users import generate_users, load_users
from recommender.schemas import Event
from recommender.streaming import consumer, producer

START = datetime(2026, 11, 16, tzinfo=UTC)


@pytest.fixture
def inputs(tmp_path: Path) -> tuple[Path, Path]:
    catalog = tmp_path / "products.jsonl"
    source = Path(__file__).resolve().parents[1] / "data/raw/products_bestbuy_1000.json"
    catalog.write_text(
        "\n".join(json.dumps(p) for p in json.loads(source.read_text("utf-8"))), "utf-8"
    )
    users = tmp_path / "users.json"
    users.write_text(
        json.dumps([u.model_dump(mode="json") for u in generate_users(20)]), "utf-8"
    )
    return catalog, users


def stream_events(inputs: tuple[Path, Path], count: int = 12) -> list[Event]:
    return list(
        islice(
            producer.generate_stream(
                load_catalog(inputs[0]), load_users(inputs[1]), seed=42, start_at=START
            ),
            count,
        )
    )


def test_stream_restarts_use_distinct_event_ids(inputs: tuple[Path, Path]) -> None:
    products, profiles = load_catalog(inputs[0]), load_users(inputs[1])
    first = list(
        islice(producer.generate_stream(products, profiles, start_at=START), 10)
    )
    second = list(
        islice(
            producer.generate_stream(
                products, profiles, start_at=START + timedelta(seconds=1)
            ),
            10,
        )
    )
    assert {e.event_id for e in first}.isdisjoint(e.event_id for e in second)


def test_stream_reproducible_funnels_preferences_and_ownership(
    inputs: tuple[Path, Path],
) -> None:
    events = stream_events(inputs, 2000)
    assert events == stream_events(inputs, 2000)
    assert len({e.event_id for e in events}) == 2000
    categories = {p.product_id: p.category_id for p in load_catalog(inputs[0])}
    profiles = {u.user_id: u for u in generate_users(20)}
    owners, carts, funnels = {}, {}, {}
    hits = 0
    for event in events:
        assert Event.model_validate_json(event.model_dump_json()) == event
        assert event.user_id in profiles
        assert owners.setdefault(event.session_id, event.user_id) == event.user_id
        profile = profiles[event.user_id]
        hits += categories[event.product_id] in (
            profile.primary_interest,
            profile.secondary_interest,
        )
        previous = funnels.setdefault((event.session_id, event.product_id), set())
        if event.event_type == "click":
            assert "view" in previous
        if event.event_type == "add_to_cart":
            assert {"view", "click"} <= previous
            assert carts.setdefault(
                event.cart_id, (event.user_id, event.session_id)
            ) == (event.user_id, event.session_id)
        previous.add(event.event_type)
    assert hits / len(events) > 0.8
    assert {e.event_type for e in events} == {"view", "click", "add_to_cart"}


def test_producer_dry_run_never_connects(
    inputs: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setattr(
        producer,
        "create_kafka_producer",
        Mock(side_effect=AssertionError("Network forbidden")),
    )
    producer.main(
        [
            "--catalog",
            str(inputs[0]),
            "--users",
            str(inputs[1]),
            "--max-events",
            "7",
            "--rate",
            "1000000",
            "--dry-run",
        ]
    )
    events = [
        Event.model_validate_json(line) for line in capsys.readouterr().out.splitlines()
    ]
    assert len(events) == 7


def test_producer_mock_kafka_serialization_pacing_and_cleanup(
    inputs: tuple[Path, Path],
) -> None:
    kafka = Mock()
    clock = Mock(return_value=0.0)
    sleep = Mock()
    assert (
        producer.run_producer(
            *inputs,
            rate=2,
            max_events=3,
            kafka_producer=kafka,
            monotonic=clock,
            sleep=sleep,
        )
        == 3
    )
    assert kafka.send.call_count == 3
    for call in kafka.send.call_args_list:
        event = Event.model_validate_json(call.kwargs["value"])
        assert call.args == ("product-events",)
        assert call.kwargs["key"] == event.user_id.encode("utf-8")
    assert [call.args[0] for call in sleep.call_args_list] == [0.5, 1.0]
    kafka.flush.assert_called_once()
    kafka.close.assert_called_once()


@pytest.mark.parametrize(
    "options", [{"rate": 0}, {"rate": float("nan")}, {"max_events": -1}, {"seed": -1}]
)
def test_producer_rejects_invalid_options(
    inputs: tuple[Path, Path], options: dict
) -> None:
    with pytest.raises(ValueError):
        producer.run_producer(*inputs, dry_run=True, **options)


def messages(events: list[Event]) -> list[SimpleNamespace]:
    return [
        SimpleNamespace(
            value=e.model_dump_json().encode(),
            topic="product-events",
            partition=0,
            offset=i,
        )
        for i, e in enumerate(events)
    ]


def fake_consumer(polls: list[dict]) -> Mock:
    kafka = Mock()
    kafka.poll.side_effect = [*polls, KeyboardInterrupt()]
    return kafka


def test_consumer_size_limit_flushes_partial_and_commits_only_processed(
    inputs: tuple[Path, Path],
) -> None:
    records = messages(stream_events(inputs))
    partition = TopicPartition("product-events", 0)
    kafka = fake_consumer([{partition: records}])
    sink = Mock()
    order = Mock()
    order.attach_mock(sink.write_batch, "write")
    order.attach_mock(kafka.commit, "commit")
    assert consumer.consume_events(kafka, sink, batch_size=4, max_events=7) == 7
    assert [len(c.args[0]) for c in sink.write_batch.call_args_list] == [4, 3]
    assert [
        c.kwargs["offsets"][partition].offset for c in kafka.commit.call_args_list
    ] == [4, 7]
    assert [c[0] for c in order.mock_calls] == ["write", "commit", "write", "commit"]
    kafka.close.assert_called_once_with(autocommit=False)


def test_consumer_time_flush_and_shutdown_partial(inputs: tuple[Path, Path]) -> None:
    records = messages(stream_events(inputs, 2))
    partition = TopicPartition("product-events", 0)
    kafka = fake_consumer([{partition: records[:1]}, {}, {partition: records[1:]}])
    sink = Mock()
    ticks = iter([0, 0, 0, 6, 6, 6, 6, 6, 6, 6])
    assert consumer.consume_events(kafka, sink, monotonic=lambda: next(ticks)) == 2
    assert [len(c.args[0]) for c in sink.write_batch.call_args_list] == [1, 1]


def test_consumer_storage_failure_does_not_commit(inputs: tuple[Path, Path]) -> None:
    kafka = fake_consumer(
        [{TopicPartition("product-events", 0): messages(stream_events(inputs, 1))}]
    )
    sink = Mock()
    sink.write_batch.side_effect = OSError("Disk full")
    with pytest.raises(OSError, match="Disk full"):
        consumer.consume_events(kafka, sink, batch_size=1)
    kafka.commit.assert_not_called()
    kafka.close.assert_called_once_with(autocommit=False)


def test_idle_consumer_keeps_bounded_polling_without_busy_loop() -> None:
    kafka = fake_consumer([{}, {}])
    ticks = iter([0, 6, 6, 7, 7, 8])
    assert consumer.consume_events(kafka, Mock(), monotonic=lambda: next(ticks)) == 0
    assert all(call.kwargs["timeout_ms"] >= 500 for call in kafka.poll.call_args_list)


def test_consumer_commits_each_partition_only_after_writing(
    inputs: tuple[Path, Path],
) -> None:
    records = messages(stream_events(inputs, 6))
    first, second = (
        TopicPartition("product-events", 0),
        TopicPartition("product-events", 1),
    )
    for record in records[3:]:
        record.partition = 1
        record.offset -= 3
    kafka = fake_consumer([{first: records[:3], second: records[3:]}])
    sink = Mock()
    assert consumer.consume_events(kafka, sink, max_events=4) == 4
    offsets = kafka.commit.call_args.kwargs["offsets"]
    assert offsets[first].offset == 3
    assert offsets[second].offset == 1


def test_producer_closes_client_after_send_failure(inputs: tuple[Path, Path]) -> None:
    kafka = Mock()
    kafka.send.return_value.get.side_effect = RuntimeError("Send failed")
    with pytest.raises(RuntimeError, match="Send failed"):
        producer.run_producer(*inputs, max_events=1, kafka_producer=kafka)
    kafka.close.assert_called_once()


def test_consumer_rejects_invalid_wire_event_without_commit() -> None:
    kafka = fake_consumer(
        [{TopicPartition("product-events", 0): [SimpleNamespace(value=b"{}")]}]
    )
    with pytest.raises(ValueError):
        consumer.consume_events(kafka, Mock(), batch_size=1)
    kafka.commit.assert_not_called()


def test_bronze_sink_minio_fallback_and_manifest_last(
    inputs: tuple[Path, Path], tmp_path: Path
) -> None:
    events = stream_events(inputs)
    minio = Mock()
    sink = consumer.BronzeSink(tmp_path / "bronze", minio_client=minio)
    first = sink.write_batch(events)
    assert first.is_file()
    calls = minio.fput_object.call_args_list
    assert len(calls) == 2
    assert calls[0].args[0] == calls[1].args[0] == "bronze"
    assert calls[0].args[1].endswith("events.jsonl")
    assert calls[1].args[1].endswith("manifest.json")
    minio.fput_object.side_effect = OSError("MinIO is off")
    second = sink.write_batch(events)
    assert second != first and second.is_file()
    assert (second.parent / "manifest.json").is_file()
    assert first.read_bytes() == second.read_bytes()


def test_consumer_factory_disables_auto_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    import kafka

    constructor = Mock()
    monkeypatch.setattr(kafka, "KafkaConsumer", constructor)
    consumer.create_kafka_consumer("localhost:9092", "product-events", "test-group")
    assert constructor.call_args.kwargs["enable_auto_commit"] is False
    assert constructor.call_args.kwargs["group_id"] == "test-group"


def test_consumer_cli_with_mock_and_local_sink(
    inputs: tuple[Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kafka = fake_consumer(
        [{TopicPartition("product-events", 0): messages(stream_events(inputs, 3))}]
    )
    monkeypatch.setattr(consumer, "create_kafka_consumer", Mock(return_value=kafka))
    consumer.main(
        ["--output-dir", str(tmp_path / "output"), "--max-events", "3", "--local-only"]
    )
    assert len(list((tmp_path / "output").rglob("manifest.json"))) == 1
    assert kafka.commit.call_count == 1
