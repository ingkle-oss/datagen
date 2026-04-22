import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import produce_fake


class _StopLoop(Exception):
    pass


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
