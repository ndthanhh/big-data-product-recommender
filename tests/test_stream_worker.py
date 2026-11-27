"""Cooperative shutdown and propagation of streaming worker failures."""

import importlib.util
import json
import os
import shutil
import subprocess
import threading
from pathlib import Path
from unittest.mock import Mock

import pytest
from test_streaming import fake_consumer, messages, stream_events
from test_streaming import inputs as inputs  # noqa: PLC0414 -- pytest fixture re-export

from recommender.streaming.consumer import consume_events
from recommender.streaming.producer import run_producer

WORKER_PATH = Path(__file__).resolve().parents[1] / "scripts/run_stream_worker.py"
spec = importlib.util.spec_from_file_location("stream_worker", WORKER_PATH)
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


def test_stopped_producer_sends_no_events(inputs):
    client = Mock()
    count = run_producer(*inputs, kafka_producer=client, should_stop=lambda: True)
    assert count == 0
    client.send.assert_not_called()
    client.close.assert_called_once()


def test_stopped_consumer_flushes_partial_batch(inputs):
    stopped = threading.Event()
    kafka = fake_consumer([])
    records = messages(stream_events(inputs, 2))

    def poll(**kwargs):
        stopped.set()
        return {0: records}

    kafka.poll.side_effect = poll
    sink = Mock()
    assert consume_events(kafka, sink, should_stop=stopped.is_set) == 2
    assert len(sink.write_batch.call_args.args[0]) == 2
    assert kafka.commit.call_args.kwargs["offsets"]


def test_worker_failure_stops_other_worker():
    stopped = threading.Event()

    def failing():
        raise ValueError("broken source")

    def waiting():
        assert stopped.wait(2)

    with pytest.raises(RuntimeError, match="broken source"):
        worker.run_workers(failing, waiting, stopped)


def test_normal_worker_completion_stops_other_worker():
    stopped = threading.Event()
    worker.run_workers(lambda: None, lambda: stopped.wait(2), stopped)
    assert stopped.is_set()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell process lifecycle")
def test_stop_script_rejects_pid_reuse(tmp_path):
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / "stop_continuous_stream.ps1"
    shutil.copyfile(WORKER_PATH.parent / script.name, script)
    metadata = tmp_path / ".stream_pids"
    metadata.write_text(
        json.dumps(
            {"pid": os.getpid(), "start_time": "2000-01-01", "executable": "wrong"}
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "different process" in result.stderr
    assert metadata.exists()
    assert not (tmp_path / ".stream_stop").exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell process lifecycle")
def test_stop_script_without_worker_is_safe(tmp_path):
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / "stop_continuous_stream.ps1"
    shutil.copyfile(WORKER_PATH.parent / script.name, script)
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell process lifecycle")
def test_stop_script_clears_stale_legacy_pid_file(tmp_path):
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / "stop_continuous_stream.ps1"
    shutil.copyfile(WORKER_PATH.parent / script.name, script)
    metadata = tmp_path / ".stream_pids"
    metadata.write_text("2147483647,consumer\n2147483646,producer", encoding="utf-8")
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert not metadata.exists()
