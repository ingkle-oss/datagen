import json
from pathlib import Path
from types import SimpleNamespace
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.multi_edge import build_runtime_config, load_multi_edge_specs


def _args():
    return SimpleNamespace(
        kafka_bootstrap_servers="broker:9093",
        kafka_security_protocol="SASL_PLAINTEXT",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="user",
        kafka_sasl_password="pass",
        kafka_ssl_ca_location=None,
        kafka_compression_type="gzip",
        kafka_delivery_timeout_ms=30000,
        kafka_linger_ms=1000,
        kafka_batch_size=1000000,
        kafka_batch_num_messages=10000,
        kafka_message_max_bytes=1000000,
        kafka_acks=0,
        kafka_flush=True,
        kafka_partition=0,
        kafka_key=None,
        s3_endpoint="http://s3.local",
        s3_accesskey="ak",
        s3_secretkey="sk",
        report_interval=10,
    )


def test_multi_edge_spec_expands_count_and_builds_runtime_config(tmp_path):
    spec_path = tmp_path / "edges.json"
    spec_path.write_text(
        json.dumps(
            {
                "defaults": {
                    "mode": "pattern",
                    "rate": 2,
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
                },
                "edges": [
                    {
                        "count": 2,
                        "name_pattern": "{prefix}-{i:03d}",
                        "start_index": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    specs = load_multi_edge_specs(str(spec_path), "edge")

    assert [spec.name for spec in specs] == ["edge-001", "edge-002"]
    assert specs[0].custom_row["edge_id"] == "edge-001"
    assert any(column["name"] == "edge_id" for column in specs[0].fake_config["columns"])

    runtime = build_runtime_config(specs[0], _args())
    assert runtime["topic_name"] == "edge-001"
    assert runtime["kafka"]["sasl_username"] == "user"
    assert runtime["fake_config"]["columns"][-1]["generator"]["value"] == "edge-001"
