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
    parser.add_argument("--kafka-key", help="Kafka partition key", default=None)
    parser.add_argument(
        "--kafka-compression-type",
        help="Kafka producer compression type",
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        default="gzip",
    )
    parser.add_argument(
        "--kafka-delivery-timeout-ms", help="delivery timeout in ms", default=30000
    )
    parser.add_argument("--kafka-linger-ms", help="linger ms", default=1000)
    parser.add_argument(
        "--kafka-batch-size",
        help="Maximum size of size (in bytes) of all messages batched in one MessageSet",
        default=1000000,
    )
    parser.add_argument(
        "--kafka-batch-num-messages",
        help="Maximum number of messages batched in one MessageSet",
        default=10000,
    )
    parser.add_argument(
        "--kafka-message-max-bytes", help="message max bytes", default=1000000
    )
    parser.add_argument(
        "--kafka-acks",
        help="Idempotent delivery option ",
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
        "--kafka-report-interval",
        help="Kafka delivery report interval",
        type=int,
        default=10,
    )

    # File
    parser.add_argument(
        "--s3endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3accesskey", help="S3 accesskey")
    parser.add_argument("--s3secretkey", help="S3 secretkey")

    parser.add_argument("--filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--bigfile",
        help="Whether file is big or not (default: False)",
        action="store_true",
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
        "--key-vals",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--rate", help="records / seconds (1~1000000)", type=int, default=1
    )

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

    key_vals = {}
    for kv in args.key_vals:
        key, val = kv.split("=")
        key_vals[key] = val

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

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3accesskey, args.s3secretkey, args.s3endpoint
        )

    incremental_idx = 0
    unique_alt_prev_value = None
    unique_alt_idx = -1

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
                now = datetime.now(timezone.utc)
                for idx in range(args.rate):
                    epoch = now + timedelta(microseconds=idx * (1000000 / args.rate))

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

                    if not values and not key_vals:
                        logging.debug("No values to be produced")
                        continue

                    if args.incremental_field:
                        values[args.incremental_field] = incremental_idx
                        incremental_idx += 1

                    if args.unique_alt_field:
                        if (unique_alt_prev_value is None) or (
                            unique_alt_prev_value != values[args.unique_alt_field]
                        ):
                            unique_alt_prev_value = values[args.unique_alt_field]
                            unique_alt_idx += 1
                        values[args.unique_alt_field] = unique_alt_idx

                    row = {
                        "timestamp": int(epoch.timestamp() * 1e6),
                        **key_vals,
                        **values,
                    }

                    producer.poll(0)
                    try:
                        producer.produce(
                            args.kafka_topic,
                            encode(row, args.output_type),
                            args.kafka_key.encode("utf-8") if args.kafka_key else None,
                            on_delivery=delivery_report,
                        )
                    except KafkaException as e:
                        logging.error("KafkaException: %s", e)

                    logging.debug("Produced: %s:%s", args.kafka_key, row)

                if args.kafka_flush:
                    producer.flush()

                wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
                wait = 0.0 if wait < 0 else wait
                logging.info("Waiting for %f seconds...", wait)
                time.sleep(wait)

            producer.flush()
            logging.info("Finished")
    else:
        values = load_values(filepath, args.input_type)
        if not values and not key_vals:
            logging.warning("No values to be produced")
            exit(0)

        val_idx = 0
        while True:
            now = datetime.now(timezone.utc)
            for idx in range(args.rate):
                epoch = now + timedelta(microseconds=idx * (1000000 / args.rate))

                if args.incremental_field:
                    values[val_idx][args.incremental_field] = incremental_idx
                    incremental_idx += 1

                if args.unique_alt_field:
                    if (unique_alt_prev_value is None) or (
                        unique_alt_prev_value != values[val_idx][args.unique_alt_field]
                    ):
                        unique_alt_prev_value = values[val_idx][args.unique_alt_field]
                        unique_alt_idx += 1
                    values[val_idx][args.unique_alt_field] = unique_alt_idx

                row = {
                    "timestamp": int(epoch.timestamp() * 1e6),
                    **key_vals,
                    **values[val_idx],
                }
                val_idx = (val_idx + 1) % len(values)

                producer.poll(0)
                try:
                    producer.produce(
                        args.kafka_topic,
                        encode(row, args.output_type),
                        args.kafka_key.encode("utf-8") if args.kafka_key else None,
                        on_delivery=delivery_report,
                    )
                except KafkaException as e:
                    logging.error("KafkaException: %s", e)

                logging.debug("Produced: %s:%s", args.kafka_key, row)

            if args.kafka_flush:
                producer.flush()

            wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
            wait = 0.0 if wait < 0 else wait
            logging.info("Waiting for %f seconds...", wait)
            time.sleep(wait)

        producer.flush()
        logging.info("Finished")
