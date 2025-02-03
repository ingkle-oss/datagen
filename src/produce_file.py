#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples


import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer
from fastnumbers import check_float

from utils.utils import download_s3file, encode, load_rows

INCREMENTAL_IDX = 0
UNIQUE_ALT_PREV_VALUE = None
UNIQUE_ALT_IDX = -1

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


def produce(
    producer: Producer,
    output_type: str,
    topic: str,
    partition: int,
    key: str | None,
    values: dict,
    epoch: datetime,
    interval: float,
    record_interval_field: str,
    record_interval_divisor: float,
    incremental_field: str,
    unique_alt_field: str,
) -> float:
    global INCREMENTAL_IDX
    global UNIQUE_ALT_PREV_VALUE
    global UNIQUE_ALT_IDX

    values = {k: v for k, v in values.items() if v is not None}

    if incremental_field and incremental_field in values:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += 1

    if unique_alt_field and unique_alt_field in values:
        if (UNIQUE_ALT_PREV_VALUE is None) or (
            UNIQUE_ALT_PREV_VALUE != values[unique_alt_field]
        ):
            UNIQUE_ALT_PREV_VALUE = values[unique_alt_field]
            UNIQUE_ALT_IDX += 1
        values[unique_alt_field] = UNIQUE_ALT_IDX

    if record_interval_field and record_interval_field in values:
        interval = values[record_interval_field] / record_interval_divisor

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **values,
    }

    producer.poll(0)
    try:
        producer.produce(
            topic=topic,
            value=encode(row, output_type),
            key=key.encode("utf-8") if key else None,
            partition=partition,
            on_delivery=delivery_report,
        )
    except KafkaException as e:
        logging.error("KafkaException: %s", e)

    logging.debug("Produced: %s:%s", args.kafka_key, row)

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
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    parser.add_argument("--filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--bigfile",
        help="Whether file is big or not (default: False)",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "json", "bson"],
        default="json",
    )

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
        "--record-interval", help="Record interval in seconds", type=float, default=1.0
    )
    parser.add_argument(
        "--record-interval-field", help="Interval field (float) between records"
    )
    parser.add_argument(
        "--record-interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
    )

    # Other field options
    parser.add_argument("--incremental-field", help="Incremental field (int) from 0")
    parser.add_argument(
        "--unique-alt-field",
        help="Use unique values for alternative field (float type)",
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    custom_rows = {}
    for kv in args.custom_rows:
        key, row = kv.split("=")
        custom_rows[key] = row

    interval = args.record_interval

    interval_divisor = 1.0
    if args.record_interval_field:
        if args.record_interval_field_unit == "second":
            pass
        elif args.record_interval_field_unit == "millisecond":
            interval_divisor = 1e3
        elif args.record_interval_field_unit == "microsecond":
            interval_divisor = 1e6
        elif args.record_interval_field_unit == "nanosecond":
            interval_divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s", args.record_interval_field_unit
            )
        logging.info("Ignores ---record-interval")

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

    # For bigfile, load file one by one
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                line = f.readline()
                headers = line.strip().split(",")

            body_start = f.tell()
            while True:
                loop_start = datetime.now(timezone.utc)
                for idx in range(args.rate):
                    line = f.readline()
                    if not line:
                        f.seek(body_start)
                        line = f.readline()

                    row = {}
                    if args.input_type == "csv":
                        row = [
                            float(v) if check_float(v) else v
                            for v in line.strip().split(",")
                        ]
                        row = dict(zip(headers, row))
                    else:
                        row = json.loads(line)

                    row = {
                        **custom_rows,
                        **row,
                    }
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
                        loop_start + timedelta(seconds=interval * idx),
                        interval,
                        args.record_interval_field,
                        interval_divisor,
                        args.incremental_field,
                        args.unique_alt_field,
                    )

                if args.kafka_flush:
                    producer.flush()

                wait = (interval * args.rate) - (
                    datetime.now(timezone.utc) - loop_start
                ).total_seconds()
                wait = 0.0 if wait < 0 else wait
                time.sleep(wait)
    else:
        rows = load_rows(filepath, args.input_type)
        if not rows:
            logging.warning("No values to be produced")
            exit(0)

        row_idx = 0
        while True:
            loop_start = datetime.now(timezone.utc)
            for idx in range(args.rate):
                row = {
                    **custom_rows,
                    **rows[row_idx],
                }
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
                    loop_start + timedelta(seconds=interval * idx),
                    interval,
                    args.record_interval_field,
                    interval_divisor,
                    args.incremental_field,
                    args.unique_alt_field,
                )
                row_idx = (row_idx + 1) % len(rows)

            if args.kafka_flush:
                producer.flush()

            wait = (interval * args.rate) - (
                datetime.now(timezone.utc) - loop_start
            ).total_seconds()
            wait = 0.0 if wait < 0 else wait
            time.sleep(wait)

    producer.flush()
    logging.info("Finished")
