#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer

from utils.nazare import nz_edge_row_decode, nz_edge_load_specs
from utils.utils import decode, download_s3file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Kafka bootstrap servers",
        default="kafka.kafka.svc.cluster.local:9095",
    )
    parser.add_argument(
        "--kafka-security-protocol", help="Kafka security protocol", default="SASL_SSL"
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
        "--kafka-ssl-ca-location",
        help="Kafka SSL CA file",
        default=None,
    )

    parser.add_argument(
        "--kafka-group-id",
        help="Kafka consumer group id",
        default=f"pipeline-test-kafka-consumer-{int(datetime.now().timestamp())}",
    )
    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument(
        "--kafka-auto-offset-reset",
        help="Kafka auto offset reset (earliest/latest)",
        default="latest",
    )
    parser.add_argument(
        "--input-type",
        help="Input message type",
        choices=["csv", "json", "bson", "txt", "edge"],
        default="txt",
    )
    parser.add_argument("--count", help="Kafka consumer count", type=int, default=1)
    parser.add_argument(
        "--timeout", help="Kafka consumer timeout", type=float, default=1.0
    )

    # Nazare Specific Options
    parser.add_argument("--nz-schema-file", help="Nazare Schema file")
    parser.add_argument(
        "--nz-schema-file-type",
        help="Nazare Schema file type",
        choices=["csv", "json", "jsonl", "bson"],
        default="jsonl",
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    dataspecs = []
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
        dataspecs = nz_edge_load_specs(schema_file, args.nz_schema_file_type)

    # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
    configs = {
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "security.protocol": args.kafka_security_protocol,
        "group.id": args.kafka_group_id,
        "auto.offset.reset": args.kafka_auto_offset_reset,
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

    try:
        while True:
            logging.info(
                "Consuming %d messages from topic...: %s for %s seconds",
                args.count,
                args.kafka_topic,
                args.timeout,
            )

            cnt = 0
            prev = datetime.now(timezone.utc)
            msgs = consumer.consume(args.count, timeout=args.timeout)
            logging.info(
                "Consumed %d messages from topic...: %s", len(msgs), args.kafka_topic
            )

            for msg in msgs:
                if msg is None:
                    logging.info("No message received by consumer")
                elif msg.error():
                    logging.info("Consumer error: %s", msg.error())
                try:
                    val = decode(msg.value(), args.input_type)
                    logging.debug("Message received: %s:%s", msg.key, val)
                except Exception as e:
                    logging.error("Error processing message: %s", e)
                    continue

                if args.input_type == "edge":
                    val = nz_edge_row_decode(val, dataspecs)

                cnt += 1
                logging.debug("Message decoded: %s:%s", msg.key, val)

            logging.info(
                "Report: Message rate: %f records/sec",
                float(cnt / (datetime.now(timezone.utc) - prev).total_seconds()),
            )
            cnt = 0

    finally:
        consumer.close()
        logging.info("Finished")
