import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import produce_fake


class _StopLoop(Exception):
    pass


class _FakeProducer:
    def __init__(self):
        self.messages = []

    def poll(self, timeout):
        pass

    def produce(self, **kwargs):
        self.messages.append(kwargs)

    def flush(self):
        pass


def _stop_loop(wait):
    raise _StopLoop


def test_normalize_partition_treats_missing_and_minus_one_as_auto():
    assert produce_fake._normalize_partition(None) is None
    assert produce_fake._normalize_partition("") is None
    assert produce_fake._normalize_partition(-1) is None
    assert produce_fake._normalize_partition("-1") is None


def test_normalize_partition_preserves_explicit_fixed_partition():
    assert produce_fake._normalize_partition(0) == 0
    assert produce_fake._normalize_partition("3") == 3


def test_produce_rows_omits_partition_for_auto(monkeypatch):
    producer = _FakeProducer()

    def _rows(elapsed, started_at):
        return [{"value": 1}], 0.0

    monkeypatch.setattr(produce_fake.time, "sleep", _stop_loop)

    with pytest.raises(_StopLoop):
        produce_fake._produce_rows(
            producer=producer,
            topic="edge-001",
            partition=None,
            key=None,
            flush=True,
            report_interval=10,
            output_type="json",
            row_factory=_rows,
        )

    assert producer.messages
    assert "partition" not in producer.messages[0]


def test_produce_rows_keeps_explicit_fixed_partition(monkeypatch):
    producer = _FakeProducer()

    def _rows(elapsed, started_at):
        return [{"value": 1}], 0.0

    monkeypatch.setattr(produce_fake.time, "sleep", _stop_loop)

    with pytest.raises(_StopLoop):
        produce_fake._produce_rows(
            producer=producer,
            topic="edge-001",
            partition=0,
            key=None,
            flush=True,
            report_interval=10,
            output_type="json",
            row_factory=_rows,
        )

    assert producer.messages[0]["partition"] == 0


def test_main_edge_run_pattern_mode(monkeypatch, tmp_path):
    config_path = tmp_path / "edge.json"
    config_path.write_text(
        json.dumps(
            {
                "name": "edge-001",
                "topic_name": "edge-001",
                "mode": "pattern",
                "rate": 2,
                "interval": 1.0,
                "report_interval": 10,
                "output_type": "json",
                "timestamp_enabled": True,
                "date_enabled": True,
                "custom_row": {"site": "local", "edge_id": "edge-001"},
                "fake_config": {
                    "columns": [
                        {
                            "name": "temperature",
                            "type": "float",
                            "generator": {
                                "kind": "constant",
                                "value": 42,
                            },
                        }
                    ]
                },
                "kafka": {
                    "bootstrap_servers": "broker:9093",
                    "security_protocol": "PLAINTEXT",
                    "compression_type": "gzip",
                    "delivery_timeout_ms": 30000,
                    "linger_ms": 1000,
                    "batch_size": 1000000,
                    "batch_num_messages": 10000,
                    "message_max_bytes": 1000000,
                    "acks": 0,
                    "flush": True,
                    "partition": 0,
                    "key": None,
                },
            }
        ),
        encoding="utf-8",
    )

    captured = {}

    def _fake_produce_rows(**kwargs):
        rows, batch_elapsed = kwargs["row_factory"](0.0, produce_fake.datetime.now(produce_fake.timezone.utc))
        captured["topic"] = kwargs["topic"]
        captured["rows"] = rows
        captured["output_type"] = kwargs["output_type"]
        raise _StopLoop

    monkeypatch.setattr(produce_fake, "_build_producer", lambda kafka: object())
    monkeypatch.setattr(produce_fake, "_produce_rows", _fake_produce_rows)
    monkeypatch.setattr(sys, "argv", ["produce_fake.py", "--edge-run", str(config_path)])

    with pytest.raises(_StopLoop):
        produce_fake.main()

    assert captured["topic"] == "edge-001"
    assert captured["output_type"] == "json"
    assert len(captured["rows"]) == 2
    assert captured["rows"][0]["temperature"] == 42
    assert captured["rows"][0]["site"] == "local"
    assert captured["rows"][0]["edge_id"] == "edge-001"
    assert "timestamp" in captured["rows"][0]


def test_main_edge_run_file_mode(monkeypatch, tmp_path):
    input_path = tmp_path / "rows.jsonl"
    input_path.write_text('{"value": 1}\n{"value": 2}\n', encoding="utf-8")
    config_path = tmp_path / "edge.json"
    config_path.write_text(
        json.dumps(
            {
                "name": "edge-file-01",
                "topic_name": "edge-file-01",
                "mode": "file",
                "rate": 2,
                "interval": 1.0,
                "playback_speed": 2.0,
                "report_interval": 10,
                "output_type": "json",
                "timestamp_enabled": True,
                "date_enabled": False,
                "custom_row": {"site": "local"},
                "source": {
                    "filepath": str(input_path),
                    "input_type": "jsonl",
                },
                "kafka": {
                    "bootstrap_servers": "broker:9093",
                    "security_protocol": "PLAINTEXT",
                    "compression_type": "gzip",
                    "delivery_timeout_ms": 30000,
                    "linger_ms": 1000,
                    "batch_size": 1000000,
                    "batch_num_messages": 10000,
                    "message_max_bytes": 1000000,
                    "acks": 0,
                    "flush": True,
                    "partition": 0,
                    "key": None,
                },
            }
        ),
        encoding="utf-8",
    )

    captured = {}

    def _fake_produce_rows(**kwargs):
        rows, batch_elapsed = kwargs["row_factory"](0.0, produce_fake.datetime.now(produce_fake.timezone.utc))
        captured["topic"] = kwargs["topic"]
        captured["rows"] = rows
        captured["batch_elapsed"] = batch_elapsed
        raise _StopLoop

    monkeypatch.setattr(produce_fake, "_build_producer", lambda kafka: object())
    monkeypatch.setattr(produce_fake, "_produce_rows", _fake_produce_rows)
    monkeypatch.setattr(sys, "argv", ["produce_fake.py", "--edge-run", str(config_path)])

    with pytest.raises(_StopLoop):
        produce_fake.main()

    assert captured["topic"] == "edge-file-01"
    assert [row["value"] for row in captured["rows"]] == [1, 2]
    assert all(row["site"] == "local" for row in captured["rows"])
    assert all("timestamp" in row for row in captured["rows"])
    assert captured["batch_elapsed"] == 1.0
