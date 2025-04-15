#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import atexit
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from confluent_kafka import KafkaException, Producer

from utils.nazare import edge_load_datasources, nz_load_fields, nz_pipeline_create
from utils.nzfake import NaFaker, NZFakerEdge, NZFakerField
from utils.utils import download_s3file, encode


def _cleanup(producer: Producer):
    logging.info("Clean up...")
    # signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    producer.flush()
    # signal.signal(signal.SIGTERM, signal.SIG_DFL)
    # signal.signal(signal.SIGINT, signal.SIG_DFL)


def _signal_handler(sig, frame):
    logging.warning("Interrupted")
    sys.exit(0)


REPORT_COUNT = 0


def delivery_report(err, msg):
    if err is not None:
        logging.warning(f"Message delivery failed: {err}")
        return

    global REPORT_COUNT

    if REPORT_COUNT >= args.kafka_report_interval:
        logging.info(
            "Message delivered to error=%s topic=%s partition=%s offset=%s latency=%s, count=%s",
            msg.error(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.latency(),
            REPORT_COUNT,
        )
        logging.debug("Message (%s): %s", msg.key(), msg.value())
        REPORT_COUNT = 0
    REPORT_COUNT += 1


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
    # Kafka producer configs
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
        "--kafka-report-interval",
        help="Kafka delivery report interval",
        type=int,
        default=10,
    )
    # Kafka others
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument(
        "--kafka-partition", help="Kafka partition", type=int, default=0
    )
    parser.add_argument("--kafka-key", help="Kafka partition key")

    # File
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
        choices=["csv", "json", "bson", "txt", "edge"],
        default="json",
    )
    parser.add_argument(
        "--custom-row",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--timestamp-enabled",
        help="Enable timestamp",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--date-enabled",
        help="Enable date",
        action=argparse.BooleanOptionalAction,
        default=True,
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

    # NZStore REST API
    parser.add_argument(
        "--nz-create-pipeline",
        help="Create Nazare pipeline",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--nz-schema-file", help="Nazare Schema file")
    parser.add_argument(
        "--nz-schema-file-type",
        help="Nazare Schema file type",
        choices=["csv", "json", "jsonl", "bson"],
        default="json",
    )
    parser.add_argument(
        "--nz-api-url",
        help="Nazare Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument("--nz-api-username", help="Nazare Store API username")
    parser.add_argument("--nz-api-password", help="Nazare Store API password")
    parser.add_argument(
        "--nz-pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--nz-pipeline-deltasync-enabled",
        help="Enable deltasync",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    # Faker
    parser.add_argument(
        "--fake-string-length", help="Length of string field", type=int, default=10
    )
    parser.add_argument(
        "--fake-string-cardinality",
        help="Number of string field cardinality",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--fake-binary-length", help="Length of binary field", type=int, default=10
    )
    parser.add_argument(
        "--fake-timestamp-tzinfo", help="Datetime timezone", default="UTC"
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    schema_file = None
    if args.nz_schema_file and args.nz_schema_file_type:
        schema_file = args.nz_schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )

    if args.nz_create_pipeline:
        if not schema_file:
            raise RuntimeError(
                "Please provide both --nz-schema-file and --nz-schema-file-type to create pipeline that requires schema file"
            )

        if not args.nz_api_url or not args.nz_api_username or not args.nz_api_password:
            raise RuntimeError("Nazare API credentials are required")

        nz_pipeline_create(
            args.nz_api_url,
            args.nz_api_username,
            args.nz_api_password,
            args.kafka_topic,
            args.nz_schema_file_type,
            schema_file,
            "EDGE" if args.output_type == "edge" else "KAFKA",
            args.nz_pipeline_deltasync_enabled,
            args.nz_pipeline_retention,
        )

    faker: NaFaker = None
    if args.output_type == "edge":
        if not schema_file:
            raise RuntimeError(
                "Please provide both --nz-schema-file and --nz-schema-file-type to edge output type that requires schema file"
            )
        faker: NZFakerEdge = NZFakerEdge(
            edge_load_datasources(schema_file, args.nz_schema_file_type),
        )
    else:
        faker: NaFaker = NZFakerField(
            nz_load_fields(schema_file, args.nz_schema_file_type),
            args.fake_string_length,
            args.fake_string_cardinality,
            args.fake_binary_length,
            ZoneInfo(args.fake_timestamp_tzinfo),
        )

    custom_row = {}
    for kv in args.custom_row:
        key, val = kv.split("=")
        custom_row[key] = val

    interval = args.interval
    # https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
    # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    configs = {
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

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    atexit.register(_cleanup, producer=producer)

    while True:
        elapsed = 0
        start_time = datetime.now(timezone.utc)
        for _ in range(args.rate):
            ts = start_time + timedelta(seconds=elapsed)

            row = faker.values() | custom_row

            if args.date_enabled:
                if "date" in row:
                    del row["date"]
                row = {"date": ts.date()} | row
            if args.timestamp_enabled:
                if "timestamp" in row:
                    del row["timestamp"]
                row = {"timestamp": int(ts.timestamp() * 1e6)} | row

            producer.poll(0)
            try:
                values = encode(row, args.output_type)
                producer.produce(
                    topic=args.kafka_topic,
                    value=values,
                    key=args.kafka_key.encode("utf-8") if args.kafka_key else None,
                    partition=args.kafka_partition,
                    on_delivery=delivery_report,
                )
                logging.debug("Produced: %s:%s", args.kafka_key, values)
            except KafkaException as e:
                logging.error("Kafka producing error: %s", e)

            elapsed += interval

        if args.kafka_flush:
            producer.flush()

        wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
        wait = 0.0 if wait < 0 else wait
        time.sleep(wait)
