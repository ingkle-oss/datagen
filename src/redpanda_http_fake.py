#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from utils.nazare import Field, load_schema_file, pipeline_create
from utils.nzfake import NZFakerField
from utils.utils import download_s3file, encode

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

    # File
    parser.add_argument("--schema-file", help="Schema file", required=True)
    parser.add_argument(
        "--schema-file-type",
        help="Schema file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:8333",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson"],
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
        help="Number of records to be posted for each rate interval",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )

    # NZStore REST API
    parser.add_argument(
        "--store-api-url",
        help="Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument("--store-api-username", help="Store API username")
    parser.add_argument("--store-api-password", help="Store API password")

    # NZStore pipeline
    parser.add_argument(
        "--pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--pipeline-deltasync-enabled",
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

    schema_file = args.schema_file
    if schema_file.startswith("s3a://"):
        schema_file = download_s3file(
            schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )
    fields: list[Field] = load_schema_file(schema_file, args.schema_file_type)

    if args.store_api_url and args.store_api_username and args.store_api_password:
        pipeline_create(
            args.store_api_url,
            args.store_api_username,
            args.store_api_password,
            args.kafka_topic,
            fields,
            args.pipeline_deltasync_enabled,
            args.pipeline_retention,
            logger=logging,
        )

    custom_row = {}
    for kv in args.custom_row:
        key, val = kv.split("=")
        custom_row[key] = val

    interval = args.interval

    fake = NZFakerField(
        fields,
        args.fake_string_length,
        args.fake_string_cardinality,
        args.fake_binary_length,
        ZoneInfo(args.fake_timestamp_tzinfo),
    )

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    scheme = "https" if args.redpanda_ssl else "http"

    while True:
        elapsed = 0
        records = []
        start_time = datetime.now(timezone.utc)
        for _ in range(args.rate):
            ts = start_time + timedelta(seconds=elapsed)
            row = fake.values() | custom_row

            if args.date_enabled:
                if "date" in row:
                    del row["date"]
                row = {"date": ts.date()} | row
            if args.timestamp_enabled:
                if "timestamp" in row:
                    del row["timestamp"]
                row = {"timestamp": int(ts.timestamp() * 1e6)} | row

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
            url=(
                f"{scheme}://{args.redpanda_host}:{args.redpanda_port}"
                f"/topics/{args.kafka_topic}"
            ),
            auth=(args.kafka_sasl_username, args.kafka_sasl_password),
            data=encode({"records": records}, args.output_type),
            headers={
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "content-encoding": "gzip",
            },
            verify=args.redpanda_verify,
        )
        res.raise_for_status()
        logging.debug("%s, %s", records, args.output_type)
        logging.info(
            "Total %s messages delivered: %s",
            len(records),
            json.dumps(res.json(), indent=2),
        )

        wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
        wait = 0.0 if wait < 0 else wait
        time.sleep(wait)

    logging.info("Finished")
