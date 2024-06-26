#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import logging
import time

import pendulum
from confluent_kafka import KafkaException, Producer

from utils.nzfake import NZFaker, NZFakerStore
from utils.utils import encode

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        help="Kafka bootstrap servers",
        default="redpanda.redpanda.svc.cluster.local:9093",
    )
    parser.add_argument(
        "--security-protocol", help="Kafka security protocol", default="SASL_SSL"
    )
    parser.add_argument(
        "--sasl-mechanism", help="Kafka SASL mechanism", default="SCRAM-SHA-512"
    )
    parser.add_argument(
        "--sasl-username", help="Kafka SASL plain username", required=True
    )
    parser.add_argument(
        "--sasl-password", help="Kafka SASL plain password", required=True
    )
    parser.add_argument(
        "--ssl-ca-location",
        help="Kafka SSL CA file",
        default=None,
    )

    parser.add_argument("--topic", help="Kafka topic name", required=True)
    parser.add_argument("--key", help="Kafka partition key", default=None)

    parser.add_argument(
        "--compression-type",
        help="Kafka producer compression type",
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        default="gzip",
    )
    parser.add_argument(
        "--delivery-timeout-ms", help="delivery timeout in ms", default=30000
    )
    parser.add_argument("--linger-ms", help="linger ms", default=5)
    parser.add_argument(
        "--batch-num-messages",
        help="Maximum number of messages batched in one MessageSet",
        default=10000,
    )
    parser.add_argument(
        "--batch-size",
        help="Maximum size of size (in bytes) of all messages batched in one MessageSet",
        default=1000000,
    )
    parser.add_argument(
        "--message-max-bytes", help="message max bytes", default=1000000
    )
    parser.add_argument(
        "--acks",
        help="Idempotent delivery option ",
        choices=[1, 0, -1],
        type=int,
        default=0,
    )

    parser.add_argument("--postgresql-host", help="postgresql host")
    parser.add_argument("--postgresql-port", help="Postgresql port", type=int)
    parser.add_argument("--postgresql-username", help="Postgresql username")
    parser.add_argument("--postgresql-password", help="Postgresql password")
    parser.add_argument("--postgresql-database", help="Postgresql database name")
    parser.add_argument("--postgresql-table")
    parser.add_argument("--postgresql-table-name", help="table name for fake schema")

    parser.add_argument(
        "--field-int-count", help="Number of int field", type=int, default=5
    )
    parser.add_argument(
        "--field-float-count", help="Number of float field", type=int, default=4
    )
    parser.add_argument(
        "--field-str-count", help="Number of string field", type=int, default=1
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
        "--field-date",
        help="Add date field (e.g. 2024-02-26)",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--field-hour",
        help="Add date field (e.g 12)",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

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
        "--report-interval", help="Delivery report interval", type=int, default=1
    )

    parser.add_argument(
        "--flush",
        help="Flush after each produce (default: True)",
        action=argparse.BooleanOptionalAction,
        default=True,
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
        "bootstrap.servers": args.bootstrap_servers,
        "security.protocol": args.security_protocol,
        "compression.type": args.compression_type,
        "delivery.timeout.ms": args.delivery_timeout_ms,
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "batch.num.messages": args.batch_num_messages,
        "message.max.bytes": args.message_max_bytes,
        "acks": args.acks,
    }
    if args.security_protocol.startswith("SASL"):
        configs.update(
            {
                "sasl.mechanism": args.sasl_mechanism,
                "sasl.username": args.sasl_username,
                "sasl.password": args.sasl_password,
            }
        )
    if args.security_protocol.endswith("SSL"):
        # * Mac: brew install openssl
        # * Ubuntu: sudo apt install ca-certificates
        configs.update(
            {
                # ! https://github.com/confluentinc/confluent-kafka-python/issues/1610
                "enable.ssl.certificate.verification": False,
                "ssl.ca.location": args.ssl_ca_location,
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
        if REPORT_COUNT >= args.report_interval:
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

    if all(
        [
            args.postgresql_host,
            args.postgresql_port,
            args.postgresql_username,
            args.postgresql_password,
            args.postgresql_database,
            args.postgresql_table,
            args.postgresql_table_name,
        ]
    ):
        fake = NZFakerStore(
            host=args.postgresql_host,
            port=args.postgresql_port,
            username=args.postgresql_username,
            password=args.postgresql_password,
            database=args.postgresql_database,
            table=args.postgresql_table,
            table_name=args.postgresql_table_name,
            loglevel=args.loglevel,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
        )
        print("FakerStore is created")
    elif any(
        [
            args.postgresql_host,
            args.postgresql_port,
            args.postgresql_username,
            args.postgresql_password,
            args.postgresql_database,
            args.postgresql_table,
            args.postgresql_table_name,
        ]
    ):
        print(args)
        raise ValueError("postgresql options are not enough")
    else:
        fake = NZFaker(
            int_count=args.field_int_count,
            float_count=args.field_float_count,
            word_count=args.field_word_count,
            text_count=args.field_text_count,
            name_count=args.field_name_count,
            str_count=args.field_str_count,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
        )
        print("Faker is created")
    print("Produced fields: ")
    print(len(fake.fields), fake.fields)

    while True:
        now = pendulum.now("UTC")
        for idx in range(args.rate):
            epoch = now + pendulum.duration(microseconds=idx * (1000000 / args.rate))

            row = {
                "timestamp": epoch.timestamp(),
                **key_vals,
                **fake.values(),
            }
            if args.field_date:
                row["date"] = epoch.format("YYYY-MM-DD")
            if args.field_hour:
                row["hour"] = epoch.format("HH")

            producer.poll(0)
            try:
                producer.produce(
                    args.topic,
                    encode(row, args.output_type),
                    args.key.encode("utf-8") if args.key else None,
                    on_delivery=delivery_report,
                )
            except KafkaException as e:
                logging.error("KafkaException: %s", e)

            logging.debug("Produced: %s:%s", args.key, row)

        if args.flush:
            producer.flush()

        wait = 1.0 - (pendulum.now("UTC") - now).total_seconds()
        wait = 0.0 if wait < 0 else wait
        logging.info("Waiting for %f seconds...", wait)
        time.sleep(wait)

    producer.flush()
    logging.info("Finished")
