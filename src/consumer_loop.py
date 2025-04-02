#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import atexit
import logging
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer

from utils.nazare import edge_load_sources, edge_row_decode
from utils.utils import decode, download_s3file


def _cleanup(consumer: Consumer):
    logging.info("Clean up...")
    # signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    consumer.close()
    # signal.signal(signal.SIGTERM, signal.SIG_DFL)
    # signal.signal(signal.SIGINT, signal.SIG_DFL)


def _signal_handler(sig, frame):
    logging.warning("Interrupted")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka bootstrap servers",
        default="kafka.kafka.svc.cluster.local:9095",
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
    parser.add_argument(
        "--kafka-enable-auto-commit",
        help="Kafka enable auto commit",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--kafka-auto-commit-interval-ms",
        help="Kafka auto commit interval ms",
        type=int,
        default=5000,
    )
    parser.add_argument(
        "--kafka-fetch-min-bytes",
        help="Kafka fetch min bytes",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--kafka-session-timeout-ms",
        help="Kafka session timeout ms",
        type=int,
        default=45000,
    )
    parser.add_argument(
        "--kafka-group-id",
        help="Kafka consumer group id",
        default=f"consumer-test-{int(datetime.now().timestamp())}",
    )
    parser.add_argument(
        "--kafka-consume-count", help="Kafka consume count", type=int, default=1
    )
    parser.add_argument(
        "--kafka-consume-timeout",
        help="Kafka consume timeout",
        type=float,
        default=1.0,
    )
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)

    parser.add_argument(
        "--input-type",
        help="Input message type",
        choices=["csv", "json", "bson", "txt", "edge"],
        default="txt",
    )

    # Nazare Specific Options
    parser.add_argument("--nz-schema-file", help="Nazare Schema file")
    parser.add_argument(
        "--nz-schema-file-type",
        help="Nazare Schema file type",
        choices=["csv", "json", "jsonl", "bson"],
        default="json",
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    datasources = []
    if args.input_type == "edge":
        if not args.nz_schema_file or not args.nz_schema_file_type:
            raise RuntimeError(
                "Please provide both --nz-schema-file and --nz-schema-file-type for edge input type"
            )
        schema_file = args.nz_schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )
        datasources = edge_load_sources(schema_file, args.nz_schema_file_type)

    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
    configs = {
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "security.protocol": args.kafka_security_protocol,
        "group.id": args.kafka_group_id,
        "auto.offset.reset": args.kafka_auto_offset_reset,
        "enable.auto.commit": args.kafka_enable_auto_commit,
        "auto.commit.interval.ms": args.kafka_auto_commit_interval_ms,
        "fetch.min.bytes": args.kafka_fetch_min_bytes,
        "session.timeout.ms": args.kafka_session_timeout_ms,
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

    consumer = Consumer(configs)
    consumer.subscribe([args.kafka_topic])

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    atexit.register(_cleanup, consumer=consumer)

    while True:
        logging.info(
            "Try consuming %d messages from topic...: %s for %s seconds",
            args.kafka_consume_count,
            args.kafka_topic,
            args.kafka_consume_timeout,
        )

        cnt = 0
        prev = datetime.now(timezone.utc)
        msgs = consumer.consume(
            args.kafka_consume_count, timeout=args.kafka_consume_timeout
        )
        logging.info("Consumed %d messages...", len(msgs))

        for msg in msgs:
            if msg is None:
                logging.info("No message received by consumer")
            elif msg.error():
                logging.info("Consumer error: %s", msg.error())
            try:
                values = decode(msg.value(), args.input_type)
                if args.input_type == "edge":
                    values = edge_row_decode(values, datasources)
                logging.debug(
                    "Message received, partition: %s, offset: %s, key: %s, value:%s",
                    msg.partition,
                    msg.offset,
                    msg.key,
                    values,
                )
            except Exception as e:
                logging.error("Message decoding error: %s", e)
                continue

            logging.debug("Message decoded: %s:%s", msg.key, values)
            cnt += 1

        logging.info(
            "Message report, rate: %f records/sec",
            cnt / (datetime.now(timezone.utc) - prev).total_seconds(),
        )
        cnt = 0
