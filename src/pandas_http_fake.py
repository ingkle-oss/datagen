#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time

import pendulum
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from utils.nzfake import NZFaker, NZFakerStore
from utils.utils import encode

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        help="Pandas proxy host",
        default="redpanda.redpanda.svc.cluster.local",
    )
    parser.add_argument(
        "--port",
        help="Pandas proxy port",
        type=int,
        default=8082,
    )
    parser.add_argument(
        "--ssl",
        help="Pandas proxy http scheme",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--verify",
        help="Pandas proxy http verify",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--sasl-username", help="Kafka SASL plain username", required=True
    )
    parser.add_argument(
        "--sasl-password", help="Kafka SASL plain password", required=True
    )

    parser.add_argument("--topic", help="Kafka topic name", required=True)
    parser.add_argument("--key", help="Kafka partition key", default=None)
    parser.add_argument("--partition", help="Kafka partition", type=int, default=0)

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

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    scheme = "https" if args.ssl else "http"

    key_vals = {}
    for kv in args.key_vals:
        key, val = kv.split("=")
        key_vals[key] = val

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
        rows = []
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

            if args.key is None:
                row = dict(
                    value=encode(row, args.output_type), partition=args.partition
                )
            else:
                row = dict(
                    key=args.key.encode("utf-8"),
                    value=encode(row, args.output_type),
                    partition=args.partition,
                )
            rows.append(row)

        res = requests.post(
            url=f"{scheme}://{args.sasl_username}:{args.sasl_password}@{args.host}:{args.port}/topics/{args.topic}",
            data=json.dumps(dict(records=rows)),
            headers={
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "content-encoding": "gzip",
            },
            verify=args.verify,
        ).json()
        logging.info(
            f"Total {len(rows)} messages delivered: {json.dumps(res, indent=2)}"
        )

        wait = 1.0 - (pendulum.now() - now).total_seconds()
        wait = 0.0 if wait < 0 else wait
        logging.info("Waiting for %f seconds...", wait)
        time.sleep(wait)

    logging.info("Finished")
