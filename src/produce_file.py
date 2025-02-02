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

from utils.utils import download_s3file, encode, load_values

INCREMENTAL_IDX = 0
UNIQUE_ALT_PREV_VALUE = None
UNIQUE_ALT_IDX = -1

REPORT_COUNT = 0
PREV_OFFSET = {}


def delivery_report(err, msg):
    global REPORT_COUNT
    global PREV_OFFSET

    if err is not None:
        logging.warning(f"Message delivery failed: {err}")
        return

    REPORT_COUNT += 1
    if REPORT_COUNT >= args.kafka_report_interval:
        REPORT_COUNT = 0
        partition = msg.partition()
        offset = msg.offset()
        logging.info(
            "Message delivered to error=%s topic=%s partition=%s offset=%s (delta=%s) latency=%s",
            msg.error(),
            msg.topic(),
            msg.partition(),
            offset,
            (
                (offset - PREV_OFFSET[f"{partition}"])
                if f"{partition}" in PREV_OFFSET and offset is not None
                else 0
            ),
            msg.latency(),
        )
        PREV_OFFSET[f"{partition}"] = offset


def produce(
    producer: Producer,
    output_type: str,
    incremental_field: str,
    unique_alt_field: str,
    record_interval_field: str,
    interval_divisor: float,
    topic: str,
    key: str | None,
    epoch: datetime,
    values: dict,
    partition: int = 0,
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

    wait = None
    if record_interval_field and record_interval_field in values:
        wait = values[record_interval_field] / interval_divisor

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **custom_key_vals,
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

    return wait


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
    parser.add_argument(
        "--kafka-ssl-ca-location", help="Kafka SSL CA file", default=None
    )
    parser.add_argument(
        "--kafka-auto-offset-reset",
        help="Kafka auto offset reset (earliest/latest)",
        default="latest",
    )
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument(
        "--kafka-partition", help="Kafka partition", type=int, default=0
    )
    parser.add_argument("--kafka-key", help="Kafka partition key", default=None)
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
        help="Kafka flush after each produce (default: True)",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--kafka-report--rate-interval",
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
        "--custom-key-vals",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )

    # Rate
    parser.add_argument(
        "--rate",
        help="Number of records to be produced for each rate interval",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--rate-interval",
        help="Rate interval in seconds",
        type=float,
        default=None,
    )

    # Timestamp options
    parser.add_argument(
        "--timestamp-start",
        help="timestamp start in epoch seconds",
        type=float,
        default=None,  # now
    )

    # Record interval
    parser.add_argument(
        "--record-interval",
        help="timestamp difference in seconds",
        type=float,
        default=None,  # args.rate_interval/args.rate
    )
    parser.add_argument(
        "--record-interval-field",
        help="Interval field (float) between records",
        default=None,
    )
    parser.add_argument(
        "--record-interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
        default=None,
    )

    # Field options
    parser.add_argument(
        "--incremental-field",
        help="Incremental field (int) from 0",
        default=None,
    )
    parser.add_argument(
        "--unique-alt-field",
        help="Use unique values for alternative field (float type)",
        default=None,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    timestamp_enabled = False
    if any([args.timestamp_start, args.record_interval]):
        if not all([args.timestamp_start, args.record_interval]):
            raise ValueError(
                (
                    "Some timestamp options are not enough, "
                    f"timestamp-start: {args.timestamp_start}, timestamp-diff: {args.record_interval}",
                )
            )
        timestamp_enabled = True

    custom_key_vals = {}
    for kv in args.custom_key_vals:
        key, val = kv.split("=")
        custom_key_vals[key] = val

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

    rate = args.rate
    divisor = 1.0
    if args.record_interval_field:
        if timestamp_enabled:
            raise RuntimeError(
                "Cannot use --record-interval-field with --timestamp-start"
            )

        if args.record_interval_field_unit == "second":
            pass
        elif args.record_interval_field_unit == "millisecond":
            divisor = 1e3
        elif args.record_interval_field_unit == "microsecond":
            divisor = 1e6
        elif args.record_interval_field_unit == "nanosecond":
            divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s" % args.record_interval_field_unit
            )
        logging.info("Ignores --rate and ---rate-interval options...")
        rate = 1

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    # For bigfile, load file one by one
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                line = f.readline()
                headers = line.strip().split(",")

            body_start = f.tell()

            if timestamp_enabled:
                timestamp_start = datetime.fromtimestamp(
                    args.timestamp_start, timezone.utc
                )

            while True:
                wait = None
                now = datetime.now(timezone.utc)
                for idx in range(rate):
                    if timestamp_enabled:
                        epoch = timestamp_start
                        timestamp_start += timedelta(
                            microseconds=args.record_interval * 1e6
                        )
                    else:
                        epoch = now + timedelta(microseconds=idx * (1000000 / rate))

                    line = f.readline()
                    if not line:
                        f.seek(body_start)
                        line = f.readline()

                    if args.input_type == "csv":
                        values = [
                            float(v) if check_float(v) else v
                            for v in line.strip().split(",")
                        ]
                        values = dict(zip(headers, values))
                    else:
                        values = json.loads(line)

                    if not values and not custom_key_vals:
                        logging.debug("No values to be produced")
                        continue

                    wait = produce(
                        producer,
                        args.output_type,
                        args.incremental_field,
                        args.unique_alt_field,
                        args.record_interval_field,
                        divisor,
                        args.kafka_topic,
                        args.kafka_key,
                        epoch,
                        values,
                        args.kafka_partition,
                    )

                if args.kafka_flush:
                    producer.flush()

                if wait or args.rate_interval:
                    if args.rate_interval:
                        wait = (
                            args.rate_interval
                            - (datetime.now(timezone.utc) - now).total_seconds()
                        )
                        wait = 0.0 if wait < 0 else wait

                    logging.info("Waiting for %f seconds...", wait)
                    time.sleep(wait)
    else:
        values = load_values(filepath, args.input_type)
        if not values and not custom_key_vals:
            logging.warning("No values to be produced")
            exit(0)

        if timestamp_enabled:
            timestamp_start = datetime.fromtimestamp(args.timestamp_start, timezone.utc)

        val_idx = 0
        while True:
            wait = None
            now = datetime.now(timezone.utc)
            for idx in range(rate):
                if timestamp_enabled:
                    epoch = timestamp_start
                    timestamp_start += timedelta(
                        microseconds=args.record_interval * 1e6
                    )
                else:
                    epoch = now + timedelta(microseconds=idx * (1000000 / rate))

                wait = produce(
                    producer,
                    args.output_type,
                    args.incremental_field,
                    args.unique_alt_field,
                    args.record_interval_field,
                    divisor,
                    args.kafka_topic,
                    args.kafka_key,
                    epoch,
                    values[val_idx],
                    args.kafka_partition,
                )
                val_idx = (val_idx + 1) % len(values)

            if args.kafka_flush:
                producer.flush()

            if wait or args.rate_interval:
                if args.rate_interval:
                    wait = (
                        args.rate_interval
                        - (datetime.now(timezone.utc) - now).total_seconds()
                    )
                    wait = 0.0 if wait < 0 else wait

                logging.info("Waiting for %f seconds...", wait)
                time.sleep(wait)

    producer.flush()
    logging.info("Finished")
