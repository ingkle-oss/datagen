#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import random
import string
import time

import pendulum
import requests
from faker import Faker
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from utils import encode

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
    parser.add_argument("--username", help="Kafka SASL plain username", required=True)
    parser.add_argument("--password", help="Kafka SASL plain password", required=True)

    parser.add_argument("--topic", help="Kafka topic name", required=True)
    parser.add_argument("--key", help="Kafka partition key", default=None)
    parser.add_argument("--partition", help="Kafka partition", type=int, default=0)

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

    fake = Faker(use_weighting=False)
    fields = (
        [f"int_{i}" for i in range(args.field_int_count)]
        + [f"float_{i}" for i in range(args.field_float_count)]
        + [f"word_{i}" for i in range(args.field_word_count)]
        + [f"text_{i}" for i in range(args.field_text_count)]
        + [f"name_{i}" for i in range(args.field_name_count)]
        + [f"str_{i}" for i in range(args.field_str_count)]
    )
    print("Produced fields: ")
    print(len(fields), fields)

    if args.field_str_cardinality:
        str_choice = [
            fake.unique.pystr(max_chars=args.field_str_length)
            for _ in range(args.field_str_cardinality)
        ]

    while True:
        now = pendulum.now()
        records = []
        for idx in range(args.rate):
            epoch = now + pendulum.duration(microseconds=idx * (1000000 / args.rate))

            value = (
                [
                    random.randint(-(2**31), (2**31) - 1)
                    for _ in range(args.field_int_count)
                ]
                + [
                    random.uniform(-(2**31), (2**31) - 1)
                    for _ in range(args.field_float_count)
                ]
                + fake.words(args.field_word_count)
                + fake.texts(args.field_text_count)
                + [fake.name() for _ in range(args.field_name_count)]
            )
            if args.field_str_cardinality:
                value += [
                    random.choice(str_choice) for _ in range(args.field_str_length)
                ]
            else:
                value += [
                    "".join(
                        random.choice(string.ascii_letters + string.digits)
                        for _ in range(args.field_str_length)
                    )
                    for _ in range(args.field_str_count)
                ]

            value = {
                "timestamp": epoch.timestamp(),
                **key_vals,
                **dict(zip(fields, value)),
            }
            if args.field_date:
                value["date"] = epoch.format("YYYY-MM-DD")
            if args.field_hour:
                value["hour"] = epoch.format("HH")

            if args.key is None:
                record = dict(
                    value=encode(value, args.output_type), partition=args.partition
                )
            else:
                record = dict(
                    key=args.key.encode("utf-8"),
                    value=encode(value, args.output_type),
                    partition=args.partition,
                )
            records.append(record)

        res = requests.post(
            url=f"{scheme}://{args.username}:{args.password}@{args.host}:{args.port}/topics/{args.topic}",
            data=json.dumps(dict(records=records)),
            headers={
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "content-encoding": "gzip",
            },
            verify=args.verify,
        ).json()
        logging.info(
            f"Total {len(records)} messages delivered: {json.dumps(res, indent=2)}"
        )

        wait = 1.0 - (pendulum.now() - now).total_seconds()
        wait = 0.0 if wait < 0 else wait
        logging.info("Waiting for %f seconds...", wait)
        time.sleep(wait)

    logging.info("Finished")
