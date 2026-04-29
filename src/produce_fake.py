#!python

import argparse
import atexit
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
from confluent_kafka import KafkaException, Producer

from utils.fake_config import (
    NZFakerConfig,
    add_fake_args,
    create_faker,
    load_fake_config_obj,
    resolve_config,
)
from utils.k8s_config import add_k8s_pipeline_args
from utils.multi_edge import build_runtime_config, load_multi_edge_specs
from utils.utils import LoadRows, download_s3file, encode


def _signal_handler(sig, frame):
    logging.warning("Interrupted")
    sys.exit(0)


def _build_producer(kafka_config: dict) -> Producer:
    configs = {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "compression.type": kafka_config.get("compression_type", "gzip"),
        "delivery.timeout.ms": kafka_config.get("delivery_timeout_ms", 30000),
        "linger.ms": kafka_config.get("linger_ms", 1000),
        "batch.size": kafka_config.get("batch_size", 1000000),
        "batch.num.messages": kafka_config.get("batch_num_messages", 10000),
        "message.max.bytes": kafka_config.get("message_max_bytes", 1000000),
        "acks": kafka_config.get("acks", 0),
    }
    if configs["security.protocol"].startswith("SASL"):
        configs.update(
            {
                "sasl.mechanism": kafka_config["sasl_mechanism"],
                "sasl.username": kafka_config["sasl_username"],
                "sasl.password": kafka_config["sasl_password"],
            }
        )
    if configs["security.protocol"].endswith("SSL") and kafka_config.get("ssl_ca_location"):
        configs.update(
            {
                "enable.ssl.certificate.verification": False,
                "ssl.ca.location": kafka_config["ssl_ca_location"],
            }
        )

    producer = Producer(configs)
    safe_configs = {k: v for k, v in configs.items() if k != "sasl.password"}
    logging.info("Producer created: %s", safe_configs)
    atexit.register(producer.flush)
    return producer


def _delivery_reporter(report_interval: int):
    count = {"value": 0}

    def _callback(err, msg):
        if err is not None:
            logging.warning("Message delivery failed: %s", err)
            return

        count["value"] += 1
        if count["value"] >= report_interval:
            logging.info(
                "Message delivered topic=%s partition=%s offset=%s latency=%0.6f count=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.latency(),
                count["value"],
            )
            count["value"] = 0

    return _callback


def _normalize_partition(value: int | str | None) -> int | None:
    if value is None or value == "":
        return None

    partition = int(value)
    if partition == -1:
        return None
    if partition < -1:
        raise ValueError("Kafka partition must be -1 for auto partitioning or >= 0")
    return partition


def _inject_time_fields(row: dict, *, started_at: datetime, elapsed_seconds: float, timestamp_enabled: bool, date_enabled: bool):
    ts = started_at + timedelta(seconds=elapsed_seconds)
    new_row = dict(row)
    if date_enabled:
        new_row.pop("date", None)
        new_row = {"date": ts.date()} | new_row
    if timestamp_enabled:
        new_row.pop("timestamp", None)
        new_row = {"timestamp": int(ts.timestamp() * 1e6)} | new_row
    return new_row


def _produce_rows(
    *,
    producer: Producer,
    topic: str,
    partition: int | None,
    key: str | None,
    flush: bool,
    report_interval: int,
    output_type: str,
    row_factory,
):
    started_at = datetime.now(timezone.utc)
    elapsed = 0.0
    callback = _delivery_reporter(report_interval)

    while True:
        loop_started = time.monotonic()
        rows, batch_elapsed = row_factory(elapsed, started_at)
        for row in rows:
            producer.poll(0)
            try:
                message = {
                    "topic": topic,
                    "value": encode(row, output_type),
                    "key": key.encode("utf-8") if key else None,
                    "on_delivery": callback,
                }
                if partition is not None:
                    message["partition"] = partition
                producer.produce(**message)
            except KafkaException as exc:
                logging.error("Kafka producing error: %s", exc)
        if flush:
            producer.flush()

        elapsed += batch_elapsed
        wait = max(0.0, batch_elapsed - (time.monotonic() - loop_started))
        time.sleep(wait)


def _run_pattern_runtime(runtime_config: dict):
    config = load_fake_config_obj(runtime_config["fake_config"])
    faker = NZFakerConfig(resolve_config(config))
    producer = _build_producer(runtime_config["kafka"])
    custom_row = runtime_config.get("custom_row", {})
    rate = int(runtime_config.get("rate", 1))
    interval = float(runtime_config.get("interval", 1.0))

    def _rows(elapsed: float, started_at: datetime):
        rows = []
        current_elapsed = elapsed
        for _ in range(rate):
            row = faker.values(current_elapsed) | custom_row
            row = _inject_time_fields(
                row,
                started_at=started_at,
                elapsed_seconds=current_elapsed,
                timestamp_enabled=runtime_config.get("timestamp_enabled", True),
                date_enabled=runtime_config.get("date_enabled", True),
            )
            rows.append(row)
            current_elapsed += interval
        return rows, rate * interval

    _produce_rows(
        producer=producer,
        topic=runtime_config["topic_name"],
        partition=_normalize_partition(runtime_config["kafka"].get("partition")),
        key=runtime_config["kafka"].get("key"),
        flush=bool(runtime_config["kafka"].get("flush", True)),
        report_interval=int(runtime_config.get("report_interval", 10)),
        output_type=runtime_config["output_type"],
        row_factory=_rows,
    )


def _resolve_source_filepath(source: dict) -> str:
    filepath = source.get("filepath") or source.get("s3_uri")
    if not filepath:
        raise RuntimeError("file mode requires source.filepath or source.s3_uri")
    if filepath.startswith("s3a://"):
        return download_s3file(
            filepath,
            source.get("s3_accesskey"),
            source.get("s3_secretkey"),
            source.get("s3_endpoint"),
        )
    return filepath


def _run_file_runtime(runtime_config: dict):
    source = runtime_config.get("source") or {}
    filepath = _resolve_source_filepath(source)
    input_type = source["input_type"]
    producer = _build_producer(runtime_config["kafka"])
    custom_row = runtime_config.get("custom_row", {})
    rate = int(runtime_config.get("rate", 1))
    interval = float(runtime_config.get("interval", 1.0))
    playback_speed = float(runtime_config.get("playback_speed", 1.0))
    effective_interval = interval / playback_speed if playback_speed > 0 else interval
    rows_iter = LoadRows(filepath, input_type)

    with rows_iter as loaded_rows:

        def _rows(elapsed: float, started_at: datetime):
            rows = []
            current_elapsed = elapsed
            for _ in range(rate):
                try:
                    row = next(loaded_rows)
                except StopIteration:
                    loaded_rows.rewind()
                    row = next(loaded_rows)
                row = dict(row) | custom_row
                row = _inject_time_fields(
                    row,
                    started_at=started_at,
                    elapsed_seconds=current_elapsed,
                    timestamp_enabled=runtime_config.get("timestamp_enabled", True),
                    date_enabled=runtime_config.get("date_enabled", True),
                )
                rows.append(row)
                current_elapsed += effective_interval
            return rows, rate * effective_interval

        _produce_rows(
            producer=producer,
            topic=runtime_config["topic_name"],
            partition=_normalize_partition(runtime_config["kafka"].get("partition")),
            key=runtime_config["kafka"].get("key"),
            flush=bool(runtime_config["kafka"].get("flush", True)),
            report_interval=int(runtime_config.get("report_interval", 10)),
            output_type=runtime_config["output_type"],
            row_factory=_rows,
        )


def _run_edge_runtime(config_path: str):
    runtime_config = orjson.loads(Path(config_path).read_bytes())
    mode = runtime_config.get("mode", "pattern")
    if mode == "pattern":
        _run_pattern_runtime(runtime_config)
        return
    if mode == "file":
        _run_file_runtime(runtime_config)
        return
    raise RuntimeError(f"Unsupported edge-run mode: {mode}")


def _validate_standard_kafka_args(args):
    if not args.kafka_topic:
        raise RuntimeError("--kafka-topic is required for standard fake producer mode")
    if args.kafka_security_protocol.startswith("SASL"):
        if not args.kafka_sasl_username or not args.kafka_sasl_password:
            raise RuntimeError("SASL protocol requires --kafka-sasl-username and --kafka-sasl-password")


def _run_standard_fake(args):
    if args.nz_create_pipeline:
        from utils.k8s_config import build_pipeline_config
        from utils.k8s_deploy import create_unified_pipeline

        ingest_type = "EDGE" if args.output_type == "edge" else "KAFKA"
        config = build_pipeline_config(args, args.kafka_topic, ingest_type)
        create_unified_pipeline(config)

    faker = create_faker(args)
    custom_row = {}
    for kv in args.custom_row:
        key, value = kv.split("=")
        custom_row[key] = value

    producer = _build_producer(
        {
            "bootstrap_servers": args.kafka_bootstrap_servers,
            "security_protocol": args.kafka_security_protocol,
            "sasl_mechanism": args.kafka_sasl_mechanism,
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
            "partition": args.kafka_partition,
            "key": args.kafka_key,
        }
    )

    rate = int(args.rate)
    interval = float(args.interval)

    def _rows(elapsed: float, started_at: datetime):
        rows = []
        current_elapsed = elapsed
        for _ in range(rate):
            row = faker.values(current_elapsed) | custom_row
            row = _inject_time_fields(
                row,
                started_at=started_at,
                elapsed_seconds=current_elapsed,
                timestamp_enabled=args.timestamp_enabled,
                date_enabled=args.date_enabled,
            )
            rows.append(row)
            current_elapsed += interval
        return rows, rate * interval

    _produce_rows(
        producer=producer,
        topic=args.kafka_topic,
        partition=_normalize_partition(args.kafka_partition),
        key=args.kafka_key,
        flush=args.kafka_flush,
        report_interval=args.report_interval,
        output_type=args.output_type,
        row_factory=_rows,
    )


def _parse_edge_names(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _validate_multi_edge_admin_args(args):
    if args.kafka_security_protocol.startswith("SASL"):
        if not args.kafka_sasl_username or not args.kafka_sasl_password:
            raise RuntimeError("multi-edge deploy/teardown requires Kafka SASL credentials")


def _datagen_image(args) -> str:
    return f"{args.image_registry}{args.image_project}/{args.edge_image_name}:{args.edge_app_version}"


def _run_multi_edge_deploy(args):
    from utils.multi_edge_k8s import (
        ensure_config_map,
        ensure_edge_deployment,
        ensure_namespace,
        ensure_topic,
    )

    specs = load_multi_edge_specs(args.multi_edge_deploy, args.k8s_prefix)
    ensure_namespace(args.k8s_namespace)
    image = _datagen_image(args)
    for spec in specs:
        runtime_config = build_runtime_config(spec, args)
        ensure_topic(
            spec.topic_name or spec.name,
            spec.topic_partitions,
            spec.topic_replication_factor,
            args,
            force=args.force,
        )
        ensure_config_map(args.k8s_namespace, spec.name, runtime_config, args.k8s_prefix)
        ensure_edge_deployment(
            args.k8s_namespace,
            spec.name,
            image,
            args.k8s_prefix,
            args.image_pull_secret,
        )
        logging.info("multi-edge deployed: %s", spec.name)


def _run_multi_edge_refresh(args):
    from utils.multi_edge_k8s import list_edges, refresh_edges

    edge_names = _parse_edge_names(args.edge_names) or list_edges(args.k8s_namespace, args.k8s_prefix)
    refresh_edges(args.k8s_namespace, edge_names)
    for edge_name in edge_names:
        logging.info("multi-edge refreshed: %s", edge_name)


def _run_multi_edge_teardown(args):
    from utils.multi_edge_k8s import list_edges, teardown_edge

    edge_names = _parse_edge_names(args.edge_names) or list_edges(args.k8s_namespace, args.k8s_prefix)
    for edge_name in edge_names:
        teardown_edge(args.k8s_namespace, edge_name, args, keep_topics=args.keep_topics)
        logging.info("multi-edge removed: %s", edge_name)


def _run_multi_edge_list(args):
    from utils.multi_edge_k8s import list_edges

    for edge_name in list_edges(args.k8s_namespace, args.k8s_prefix):
        print(edge_name)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("--kafka-bootstrap-servers", default="redpanda.redpanda.svc.cluster.local:9093")
    parser.add_argument("--kafka-security-protocol", default="SASL_PLAINTEXT")
    parser.add_argument("--kafka-sasl-mechanism", default="SCRAM-SHA-512")
    parser.add_argument("--kafka-sasl-username")
    parser.add_argument("--kafka-sasl-password")
    parser.add_argument("--kafka-ssl-ca-location")
    parser.add_argument("--kafka-compression-type", choices=["gzip", "snappy", "lz4", "zstd", "none"], default="gzip")
    parser.add_argument("--kafka-delivery-timeout-ms", default=30000)
    parser.add_argument("--kafka-linger-ms", default=1000)
    parser.add_argument("--kafka-batch-size", default=1000000)
    parser.add_argument("--kafka-batch-num-messages", default=10000)
    parser.add_argument("--kafka-message-max-bytes", default=1000000)
    parser.add_argument("--kafka-acks", choices=[1, 0, -1], type=int, default=0)
    parser.add_argument("--kafka-flush", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--kafka-topic")
    parser.add_argument(
        "--kafka-partition",
        type=int,
        default=None,
        help="Kafka partition. Omit or set -1 to let Kafka auto-partition.",
    )
    parser.add_argument("--kafka-key")

    parser.add_argument("--s3-endpoint", default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80")
    parser.add_argument("--s3-accesskey")
    parser.add_argument("--s3-secretkey")

    parser.add_argument("--output-type", choices=["csv", "json", "bson", "txt", "edge"], default="json")
    parser.add_argument("--custom-row", nargs="*", default=[])
    parser.add_argument("--timestamp-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--date-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--rate", type=int, default=1)
    parser.add_argument("--report-interval", type=int, default=10)
    parser.add_argument("--interval", type=float, default=1.0)

    parser.add_argument("--nz-create-pipeline", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--nz-schema-file")
    parser.add_argument("--nz-schema-file-type", choices=["csv", "json", "jsonl", "bson"], default="json")

    parser.add_argument("--edge-run", help="Runtime config file mounted inside datagen pod")
    parser.add_argument("--multi-edge-deploy", help="Path to multi-edge spec file")
    parser.add_argument("--multi-edge-refresh", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--multi-edge-teardown", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--multi-edge-list", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--edge-names", help="Comma separated edge names for refresh/teardown")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--keep-topics", action=argparse.BooleanOptionalAction, default=False)

    add_k8s_pipeline_args(parser)
    add_fake_args(parser)
    parser.add_argument("--loglevel", default="INFO")
    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    if args.edge_run:
        _run_edge_runtime(args.edge_run)
        return

    if args.multi_edge_deploy:
        _validate_multi_edge_admin_args(args)
        _run_multi_edge_deploy(args)
        return

    if args.multi_edge_refresh:
        _run_multi_edge_refresh(args)
        return

    if args.multi_edge_teardown:
        _validate_multi_edge_admin_args(args)
        _run_multi_edge_teardown(args)
        return

    if args.multi_edge_list:
        _run_multi_edge_list(args)
        return

    _validate_standard_kafka_args(args)
    _run_standard_fake(args)


if __name__ == "__main__":
    main()
