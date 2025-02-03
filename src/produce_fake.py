#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer

from utils.nzfake import NZFaker, NZFakerEdge, NZFakerStore
from utils.utils import encode


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
        "--kafka-report-interval",
        help="Kafka delivery report interval",
        type=int,
        default=10,
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
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )

    # PostgreSQL
    parser.add_argument(
        "--postgresql-host",
        help="PostgreSQL host",
        default="postgresql-ha-pgpool.postgresql-ha.svc.cluster.local",
    )
    parser.add_argument(
        "--postgresql-port", help="PostgreSQL port", type=int, default=5432
    )
    parser.add_argument(
        "--postgresql-username", help="PostgreSQL username", default=None
    )
    parser.add_argument(
        "--postgresql-password", help="PostgreSQL password", default=None
    )
    parser.add_argument(
        "--postgresql-database", help="PostgreSQL database", default="store"
    )
    parser.add_argument("--postgresql-table", help="PostgreSQL table", default=None)

    # 1. if use_postgresql_store is True, then use PostgreSQL store
    parser.add_argument(
        "--use-postgresql-store",
        help="Use PostgreSQL store",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--postgresql-store-table-name",
        help="store table name for fake schema",
        default=None,
    )

    # 2. if use_postgresql_edge is True, then use PostgreSQL edge data specs
    parser.add_argument(
        "--use-postgresql-edge",
        help="Use PostgreSQL Edge",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--postgresql-edge-id",
        help="Edge ID for fake schema",
        default=None,
    )

    # 3. else, parameters for fake schema
    parser.add_argument(
        "--field-bool-count", help="Number of bool field", type=int, default=0
    )
    parser.add_argument(
        "--field-int-count", help="Number of int field", type=int, default=0
    )
    parser.add_argument(
        "--field-float-count", help="Number of float field", type=int, default=0
    )
    parser.add_argument(
        "--field-str-count", help="Number of string field", type=int, default=0
    )
    parser.add_argument(
        "--field-str-cardinality",
        help="Number of string field cardinality",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--field-str-length", help="Length of string field", type=int, default=10
    )
    parser.add_argument(
        "--field-word-count", help="Number of word field", type=int, default=0
    )
    parser.add_argument(
        "--field-text-count", help="Number of text field", type=int, default=0
    )
    parser.add_argument(
        "--field-name-count", help="Number of name field", type=int, default=0
    )

    parser.add_argument(
        "--schema-update-interval",
        help="PostgreSQL update interval in seconds",
        type=int,
        default=30,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    custom_rows = {}
    for kv in args.custom_rows:
        key, val = kv.split("=")
        custom_rows[key] = val

    interval = args.interval

    if args.use_postgresql_store:
        if not all(
            [
                args.postgresql_host,
                args.postgresql_port,
                args.postgresql_username,
                args.postgresql_password,
                args.postgresql_database,
                args.postgresql_table,
                args.postgresql_store_table_name,
            ]
        ):
            raise ValueError("postgresql options are not enough for store")

        logging.info("Using faker from PostgreSQL store DB...")
        fake = NZFakerStore(
            host=args.postgresql_host,
            port=args.postgresql_port,
            username=args.postgresql_username,
            password=args.postgresql_password,
            database=args.postgresql_database,
            table=args.postgresql_table,
            table_name=args.postgresql_store_table_name,
            loglevel=args.loglevel,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
        )
    elif args.use_postgresql_edge:
        if not all(
            [
                args.postgresql_host,
                args.postgresql_port,
                args.postgresql_username,
                args.postgresql_password,
                args.postgresql_database,
                args.postgresql_table,
                args.postgresql_edge_id,
            ]
        ):
            raise ValueError("postgresql options are not enough for edge data specs")

        logging.info("Using faker from PostgreSQL edge DB...")
        fake = NZFakerEdge(
            host=args.postgresql_host,
            port=args.postgresql_port,
            username=args.postgresql_username,
            password=args.postgresql_password,
            database=args.postgresql_database,
            table=args.postgresql_table,
            edge_id=args.postgresql_edge_id,
            loglevel=args.loglevel,
        )
    else:
        logging.info("Using faker from parameters...")
        fake = NZFaker(
            bool_count=args.field_bool_count,
            int_count=args.field_int_count,
            float_count=args.field_float_count,
            word_count=args.field_word_count,
            text_count=args.field_text_count,
            name_count=args.field_name_count,
            str_count=args.field_str_count,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
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

    prev = datetime.now(timezone.utc)
    while True:
        if (
            datetime.now(timezone.utc) - prev
        ).total_seconds() > args.schema_update_interval:
            fake.update_schema()
            prev = datetime.now(timezone.utc)

        if not fake.get_schema() and not custom_rows:
            logging.warning("No schema found to be used or no custom key values")
            time.sleep(interval * args.rate)
            continue

        elapsed = 0
        loop_start = datetime.now(timezone.utc)
        for _ in range(args.rate):
            row = {
                "timestamp": int(
                    (loop_start + timedelta(seconds=elapsed)).timestamp() * 1e6
                ),
                **custom_rows,
                **fake.values(),
            }

            producer.poll(0)
            try:
                producer.produce(
                    topic=args.kafka_topic,
                    value=encode(row, args.output_type),
                    key=args.kafka_key.encode("utf-8") if args.kafka_key else None,
                    partition=args.kafka_partition,
                    on_delivery=delivery_report,
                )
                logging.debug(row)
                logging.debug("Produced: %s:%s", args.kafka_key, row)
            except KafkaException as e:
                logging.error("KafkaException: %s", e)

            elapsed += interval

        if args.kafka_flush:
            producer.flush()

        wait = elapsed - (datetime.now(timezone.utc) - loop_start).total_seconds()
        wait = 0.0 if wait < 0 else wait
        time.sleep(wait)

    producer.flush()
    logging.info("Finished")
