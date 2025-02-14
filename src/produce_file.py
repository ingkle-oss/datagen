#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples


import argparse
import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer

from utils.nazare import pipeline_create, load_schema_file
from utils.utils import LoadRows, download_s3file, encode, eval_create_func

REPORT_COUNT = 0


def delivery_report(err, msg):
    if err is not None:
        logging.warning(f"Message delivery failed: {err}")
        return

    global REPORT_COUNT

    if REPORT_COUNT >= args.kafka_report_interval:
        REPORT_COUNT = 0
        logging.info(
            "Message delivered to error=%s topic=%s partition=%s offset=%s latency=%s",
            msg.error(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.latency(),
        )
    REPORT_COUNT += 1


INCREMENTAL_IDX = 0
INTERVAL_DIFF_PREV = None


def produce(
    producer: Producer,
    output_type: str,
    topic: str,
    partition: int,
    key: str | None,
    values: dict,
    epoch: datetime,
    interval: float,
    interval_field: str,
    interval_field_divisor: float,
    interval_field_diff: str,
    incremental_field: str,
    incremental_field_step: int,
    datetime_field: str,
    datetime_field_format: str,
    eval_field: str,
    eval_func: Callable,
) -> float:
    global INCREMENTAL_IDX

    values = {k: v for k, v in values.items() if v is not None}

    if interval_field and interval_field in values:
        interval = values[interval_field] / interval_field_divisor
    elif interval_field_diff and interval_field_diff in values:
        global INTERVAL_DIFF_PREV
        interval_diff = datetime.fromisoformat((values[interval_field_diff]))
        if INTERVAL_DIFF_PREV:
            interval = (interval_diff - INTERVAL_DIFF_PREV).total_seconds()
        INTERVAL_DIFF_PREV = interval_diff

    if incremental_field:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += incremental_field_step

    if datetime_field and datetime_field_format:
        values[datetime_field] = epoch.strftime(datetime_field_format)

    if eval_field and eval_func:
        values[eval_field] = eval_func(
            **values,
        )

    row = values | {"timestamp": int(epoch.timestamp() * 1e6)}

    producer.poll(0)
    try:
        producer.produce(
            topic=topic,
            value=encode(row, output_type),
            key=key.encode("utf-8") if key else None,
            partition=partition,
            on_delivery=delivery_report,
        )
        logging.debug("Produced: %s:%s", args.kafka_key, row)
    except KafkaException as e:
        logging.error("KafkaException: %s", e)

    return interval


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Kafka
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka bootstrap servers",
        default="redpanda.redpanda.svc.cluster.local:9093",
    )
    parser.add_argument(
        "--kafka-security-protocol",
        help="Kafka security protocol",
        default="SASL_PLAINTEXT",
    )
    parser.add_argument(
        "--kafka-sasl-mechanism", help="Kafka SASL mechanism", default="SCRAM-SHA-512"
    )
    parser.add_argument(
        "--kafka-sasl-username", help="Kafka SASL plain username", required=True
    )
    parser.add_argument(
        "--kafka-sasl-password", help="Kafka SASL plain password", required=True
    )
    parser.add_argument("--kafka-ssl-ca-location", help="Kafka SSL CA file")
    parser.add_argument(
        "--kafka-auto-offset-reset",
        help="Kafka auto offset reset (earliest/latest)",
        default="latest",
    )
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument(
        "--kafka-partition", help="Kafka partition", type=int, default=0
    )
    parser.add_argument("--kafka-key", help="Kafka partition key")
    parser.add_argument(
        "--kafka-compression-type",
        help="Kafka producer compression type",
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        default="gzip",
    )
    parser.add_argument(
        "--kafka-delivery-timeout-ms",
        help="Kafka delivery timeout in ms",
        default=30000,
    )
    parser.add_argument("--kafka-linger-ms", help="Kafka linger ms", default=1000)
    parser.add_argument(
        "--kafka-batch-size",
        help="Kafka maximum size of size (in bytes) of all messages batched in one MessageSet",
        default=1000000,
    )
    parser.add_argument(
        "--kafka-batch-num-messages",
        help="Kafka maximum number of messages batched in one MessageSet",
        default=10000,
    )
    parser.add_argument(
        "--kafka-message-max-bytes", help="Kafka message max bytes", default=1000000
    )
    parser.add_argument(
        "--kafka-acks",
        help="Kafka idempotent delivery option ",
        choices=[1, 0, -1],
        type=int,
        default=0,
    )
    parser.add_argument(
        "--kafka-flush",
        help="Kafka flush after each rate loop (default: True)",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--kafka-report-interval",
        help="Kafka delivery report interval",
        type=int,
        default=10,
    )

    # File
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
    parser.add_argument("--schema-file", help="Schema file")
    parser.add_argument(
        "--schema-file-type",
        help="Schema file type",
        choices=["csv", "jsonl", "bsonl"],
        default="json",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson"],
        default="json",
    )
    parser.add_argument(
        "--custom-rows",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )

    # Rate
    parser.add_argument(
        "--rate",
        help="Number of records for each loop",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )
    parser.add_argument(
        "--interval-field", help="Use field (float) value as interval between records"
    )
    parser.add_argument(
        "--interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
    )
    parser.add_argument(
        "--interval-field-diff",
        help="Use field(datetime) difference as interval between records",
    )

    # Other field options
    parser.add_argument("--incremental-field", help="Incremental field (int)")
    parser.add_argument(
        "--incremental-field-from",
        help="Incremental field start value",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--incremental-field-step",
        help="Incremental field step value",
        type=int,
        default=1,
    )
    parser.add_argument("--datetime-field", help="Datetime field (datetime)")
    parser.add_argument(
        "--datetime-field-format", help="Datetime format", default="%Y-%m-%d %H:%M:%S"
    )
    parser.add_argument("--eval-field", help="Evaluated field")
    parser.add_argument("--eval-field-expr", help="Evaluated field expression")

    # NZStore REST API
    parser.add_argument(
        "--store-api-url",
        help="Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument("--store-api-username", help="Store API username")
    parser.add_argument("--store-api-password", help="Store API password")

    # NZStore pipeline
    parser.add_argument(
        "--pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--pipeline-deltasync-enabled",
        help="Enable deltasync",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    if (
        args.store_api_url
        and args.store_api_username
        and args.store_api_password
        and args.schema_file
        and args.schema_file_type
    ):
        schema_file = args.schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )
        pipeline_create(
            args.store_api_url,
            args.store_api_username,
            args.store_api_password,
            args.kafka_topic,
            load_schema_file(schema_file, args.schema_file_type),
            args.pipeline_deltasync_enabled,
            args.pipeline_retention,
            logger=logging,
        )

    custom_rows = {}
    for kv in args.custom_rows:
        key, row = kv.split("=")
        custom_rows[key] = row

    interval = args.interval
    interval_field_divisor = 1.0
    if args.interval_field:
        if args.interval_field_unit == "second":
            pass
        elif args.interval_field_unit == "millisecond":
            interval_field_divisor = 1e3
        elif args.interval_field_unit == "microsecond":
            interval_field_divisor = 1e6
        elif args.interval_field_unit == "nanosecond":
            interval_field_divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s", args.interval_field_unit
            )
        logging.info("Ignores ---interval")

    INCREMENTAL_IDX = args.incremental_field_from

    eval_func = None
    if args.eval_field and args.eval_field_expr:
        eval_func = eval_create_func(args.eval_field_expr)

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
    # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    configs = {
        "auto.offset.reset": args.kafka_auto_offset_reset,
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "security.protocol": args.kafka_security_protocol,
        "compression.type": args.kafka_compression_type,
        "delivery.timeout.ms": args.kafka_delivery_timeout_ms,
        "linger.ms": args.kafka_linger_ms,
        "batch.size": args.kafka_batch_size,
        "batch.num.messages": args.kafka_batch_num_messages,
        "message.max.bytes": args.kafka_message_max_bytes,
        "acks": args.kafka_acks,
    }
    if args.kafka_security_protocol.startswith("SASL"):
        configs.update(
            {
                "sasl.mechanism": args.kafka_sasl_mechanism,
                "sasl.username": args.kafka_sasl_username,
                "sasl.password": args.kafka_sasl_password,
            }
        )
    if args.kafka_security_protocol.endswith("SSL"):
        # * Mac: brew install openssl
        # * Ubuntu: sudo apt install ca-certificates
        configs.update(
            {
                # ! https://github.com/confluentinc/confluent-kafka-python/issues/1610
                "enable.ssl.certificate.verification": False,
                "ssl.ca.location": args.kafka_ssl_ca_location,
            }
        )

    producer = Producer(configs)
    configs.pop("sasl.password", None)
    logging.info("Producer created:")
    logging.info(configs)

    with LoadRows(filepath, args.input_type) as rows:
        while True:
            elapsed = 0
            start_time = datetime.now(timezone.utc)
            for _ in range(args.rate):
                try:
                    row = next(rows)
                except StopIteration:
                    rows.seek(0)
                    row = next(rows)

                row = row | custom_rows
                if not row:
                    logging.debug("No values to be produced")
                    continue

                interval = produce(
                    producer,
                    args.output_type,
                    args.kafka_topic,
                    args.kafka_partition,
                    args.kafka_key,
                    row,
                    start_time + timedelta(seconds=elapsed),
                    interval,
                    args.interval_field,
                    interval_field_divisor,
                    args.interval_field_diff,
                    args.incremental_field,
                    args.incremental_field_step,
                    args.datetime_field,
                    args.datetime_field_format,
                    args.eval_field,
                    eval_func,
                )
                elapsed += interval if interval > 0 else 0

            if args.kafka_flush:
                producer.flush()

            wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
            wait = 0.0 if wait < 0 else wait
            time.sleep(wait)

    producer.flush()
    logging.info("Finished")
