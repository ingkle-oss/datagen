from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.fake_config import NZFakerConfig, load_fake_config_obj, resolve_config


def test_time_aware_generators_resolve_and_generate():
    config = load_fake_config_obj(
        {
            "columns": [
                {
                    "name": "edge_id",
                    "type": "string",
                    "generator": {"kind": "constant", "value": "edge-001"},
                },
                {
                    "name": "temperature",
                    "type": "float",
                    "generator": {
                        "kind": "sine",
                        "amplitude": 10,
                        "period_sec": 60,
                        "offset": 50,
                    },
                },
                {
                    "name": "phase",
                    "type": "string",
                    "generator": {
                        "kind": "step",
                        "values": ["cold", "warm", "hot"],
                        "dwell_sec": 10,
                    },
                },
                {
                    "name": "load",
                    "type": "float",
                    "generator": {
                        "kind": "random_walk",
                        "start": 10,
                        "step_stddev": 0.5,
                        "min": 0,
                        "max": 20,
                    },
                },
            ]
        }
    )

    faker = NZFakerConfig(resolve_config(config))
    first = faker.values(0)
    later = faker.values(15)

    assert first["edge_id"] == "edge-001"
    assert first["phase"] == "cold"
    assert later["phase"] == "warm"
    assert 40 <= first["temperature"] <= 60
    assert 40 <= later["temperature"] <= 60
    assert 0 <= first["load"] <= 20
    assert 0 <= later["load"] <= 20
