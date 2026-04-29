import copy
import logging
from pathlib import Path
from typing import Any, Literal

import orjson
from pydantic import BaseModel, Field, model_validator


class EdgeSource(BaseModel):
    s3_uri: str | None = None
    filepath: str | None = None
    input_type: Literal["csv", "json", "jsonl", "bson", "parquet"] | None = None

    @model_validator(mode="after")
    def validate_source(self) -> "EdgeSource":
        if not self.s3_uri and not self.filepath:
            raise ValueError("file mode requires source.s3_uri or source.filepath")
        if not self.input_type:
            raise ValueError("file mode requires source.input_type")
        return self


class ResolvedEdgeSpec(BaseModel):
    name: str
    mode: Literal["pattern", "file"] = "pattern"
    rate: int = 1
    interval: float = 1.0
    output_type: Literal["csv", "json", "bson", "txt", "edge"] = "json"
    playback_speed: float = 1.0
    topic_partitions: int = 1
    topic_replication_factor: int = 1
    timestamp_enabled: bool = True
    date_enabled: bool = True
    auto_inject_edge_id: bool = True
    fake_config: dict[str, Any] = Field(default_factory=dict)
    source: EdgeSource | None = None
    custom_row: dict[str, Any] = Field(default_factory=dict)
    topic_name: str | None = None

    @model_validator(mode="after")
    def validate_mode_shape(self) -> "ResolvedEdgeSpec":
        if self.mode == "pattern" and not self.fake_config.get("columns"):
            raise ValueError("pattern mode requires fake_config.columns")
        if self.mode == "file" and self.source is None:
            raise ValueError("file mode requires source")
        return self


def _load_multi_edge_obj(filepath: str) -> dict[str, Any]:
    raw = Path(filepath).read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        obj = yaml.safe_load(raw)
    except ModuleNotFoundError:
        logging.warning("PyYAML not installed, falling back to JSON parse for %s", filepath)
        obj = orjson.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("multi-edge spec root must be an object")
    return obj


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _render_scalar(value: str, *, edge_name: str, prefix: str, index: int | None) -> str:
    rendered = value.replace("__EDGE_NAME__", edge_name)
    if "{" in rendered and "}" in rendered:
        try:
            rendered = rendered.format(prefix=prefix, i=index)
        except (IndexError, KeyError, ValueError):
            return rendered
    return rendered


def _render_placeholders(value: Any, *, edge_name: str, prefix: str, index: int | None):
    if isinstance(value, str):
        return _render_scalar(value, edge_name=edge_name, prefix=prefix, index=index)
    if isinstance(value, list):
        return [
            _render_placeholders(item, edge_name=edge_name, prefix=prefix, index=index)
            for item in value
        ]
    if isinstance(value, dict):
        return {
            key: _render_placeholders(item, edge_name=edge_name, prefix=prefix, index=index)
            for key, item in value.items()
        }
    return value


def _ensure_edge_id(fake_config: dict[str, Any], edge_name: str):
    columns = fake_config.setdefault("columns", [])
    if any(column.get("name") == "edge_id" for column in columns):
        return
    columns.append(
        {
            "name": "edge_id",
            "type": "string",
            "generator": {
                "kind": "constant",
                "value": edge_name,
            },
        }
    )


def _expand_edge_entries(edges: list[dict[str, Any]], prefix: str) -> list[tuple[dict[str, Any], int | None]]:
    expanded: list[tuple[dict[str, Any], int | None]] = []
    for entry in edges:
        if "count" not in entry:
            expanded.append((copy.deepcopy(entry), None))
            continue

        count = int(entry["count"])
        pattern = entry.get("name_pattern")
        if not pattern:
            raise ValueError("count blocks require name_pattern")
        start_index = int(entry.get("start_index", 0))
        base = {k: v for k, v in entry.items() if k not in {"count", "name_pattern", "start_index"}}
        for offset in range(count):
            index = start_index + offset
            item = copy.deepcopy(base)
            item["name"] = pattern.format(prefix=prefix, i=index)
            expanded.append((item, index))
    return expanded


def load_multi_edge_specs(filepath: str, prefix: str) -> list[ResolvedEdgeSpec]:
    obj = _load_multi_edge_obj(filepath)
    defaults = obj.get("defaults", {}) or {}
    edges = obj.get("edges", []) or []
    if not isinstance(edges, list) or not edges:
        raise ValueError("multi-edge spec requires non-empty edges list")

    resolved: list[ResolvedEdgeSpec] = []
    seen_names: set[str] = set()
    for raw_entry, index in _expand_edge_entries(edges, prefix):
        merged = _deep_merge(defaults, raw_entry)
        name = merged.get("name")
        if not name:
            raise ValueError("edge entry requires name")
        merged = _render_placeholders(merged, edge_name=name, prefix=prefix, index=index)
        merged.setdefault("custom_row", {})
        merged.setdefault("fake_config", {})
        merged.setdefault("topic_name", merged.get("name"))
        if merged.get("auto_inject_edge_id", True):
            if merged.get("mode", "pattern") == "pattern":
                _ensure_edge_id(merged["fake_config"], merged["name"])
            merged["custom_row"].setdefault("edge_id", merged["name"])

        spec = ResolvedEdgeSpec.model_validate(merged)
        if spec.name in seen_names:
            raise ValueError(f"duplicate edge name: {spec.name}")
        seen_names.add(spec.name)
        resolved.append(spec)

    return resolved


def build_runtime_config(spec: ResolvedEdgeSpec, args) -> dict[str, Any]:
    kafka = {
        "bootstrap_servers": getattr(args, "infra_kafka_bootstrap", args.kafka_bootstrap_servers),
        "security_protocol": getattr(args, "infra_kafka_security_protocol", args.kafka_security_protocol),
        "sasl_mechanism": getattr(args, "infra_kafka_sasl_mechanism", args.kafka_sasl_mechanism),
        "sasl_username": args.kafka_sasl_username,
        "sasl_password": args.kafka_sasl_password,
        "ssl_ca_location": args.kafka_ssl_ca_location,
        "compression_type": args.kafka_compression_type,
        "delivery_timeout_ms": args.kafka_delivery_timeout_ms,
        "linger_ms": args.kafka_linger_ms,
        "batch_size": args.kafka_batch_size,
        "batch_num_messages": args.kafka_batch_num_messages,
        "message_max_bytes": args.kafka_message_max_bytes,
        "acks": args.kafka_acks,
        "flush": args.kafka_flush,
        "partition": getattr(args, "kafka_partition", None),
        "key": args.kafka_key,
    }
    source = spec.source.model_dump() if spec.source else None
    if source:
        source.setdefault("s3_endpoint", args.s3_endpoint)
        source.setdefault("s3_accesskey", args.s3_accesskey)
        source.setdefault("s3_secretkey", args.s3_secretkey)

    return {
        "name": spec.name,
        "topic_name": spec.topic_name or spec.name,
        "mode": spec.mode,
        "rate": spec.rate,
        "interval": spec.interval,
        "report_interval": args.report_interval,
        "output_type": spec.output_type,
        "playback_speed": spec.playback_speed,
        "timestamp_enabled": spec.timestamp_enabled,
        "date_enabled": spec.date_enabled,
        "custom_row": copy.deepcopy(spec.custom_row),
        "fake_config": copy.deepcopy(spec.fake_config),
        "source": source,
        "kafka": kafka,
    }
