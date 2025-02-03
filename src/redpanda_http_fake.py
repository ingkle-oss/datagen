#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from utils.nzfake import NZFaker, NZFakerEdge, NZFakerStore
from utils.utils import encode

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # PandaProxy
    parser.add_argument(
        "--redpanda-host",
        help="Redpanda proxy host",
        default="redpanda.redpanda.svc.cluster.local",
    )
    parser.add_argument(
        "--redpanda-port",
        help="Redpanda proxy port",
        type=int,
        default=8082,
    )
    parser.add_argument(
        "--redpanda-ssl",
        help="Redpanda proxy http scheme",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--redpanda-verify",
        help="Redpanda proxy http verify",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    # Kafka
    parser.add_argument(
        "--kafka-sasl-username", help="Kafka SASL plain username", required=True
    )
    parser.add_argument(
        "--kafka-sasl-password", help="Kafka SASL plain password", required=True
    )

    parser.add_argument("--kafka-topic", help="Kafka topic name", required=True)
    parser.add_argument("--kafka-key", help="Kafka partition key", default=None)
    parser.add_argument(
        "--kafka-partition", help="Kafka partition", type=int, default=0
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
    parser.add_argument("--postgresql-table", help="PostgreSQL table", default="fields")

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

    # 3. else, default Faker
    parser.add_argument(
        "--field-bool-count", help="Number of bool field", type=int, default=0
    )
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
        "--schema-update-interval",
        help="PostgreSQL update interval in seconds",
        type=int,
        default=30,
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
        help="Number of records to be posted for each rate interval",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--record-interval", help="Record interval in seconds", type=float, default=1.0
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

    interval = args.record_interval

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

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    scheme = "https" if args.redpanda_ssl else "http"

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
        records = []
        loop_start = datetime.now(timezone.utc)
        for _ in range(args.rate):
            row = {
                "timestamp": int(
                    (loop_start + timedelta(seconds=elapsed)).timestamp() * 1e6
                ),
                **custom_rows,
                **fake.values(),
            }

            if args.kafka_key is None:
                record = dict(value=row, partition=args.kafka_partition)
            else:
                record = dict(
                    key=args.kafka_key.encode("utf-8"),
                    value=row,
                    partition=args.kafka_partition,
                )
            records.append(record)
            elapsed += interval

        res = requests.post(
            url=f"{scheme}://{args.kafka_sasl_username}:{args.kafka_sasl_password}@{args.redpanda_host}:{args.redpanda_port}/topics/{args.kafka_topic}",
            data=encode({"records": records}, args.output_type),
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            verify=args.redpanda_verify,
        ).json()
        logging.debug(encode({"records": records}, args.output_type))
        logging.info(
            f"Total {len(records)} messages delivered: {json.dumps(res, indent=2)}"
        )

        wait = elapsed - (datetime.now(timezone.utc) - loop_start).total_seconds()
        wait = 0.0 if wait < 0 else wait
        time.sleep(wait)

    logging.info("Finished")
