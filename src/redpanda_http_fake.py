#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import atexit
import json
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import urllib3

from utils.nazare import edge_load_datasources, nz_load_fields, nz_pipeline_create
from utils.nzfake import NaFaker, NZFakerEdge, NZFakerField
from utils.utils import download_s3file, encode


def _cleanup():
    logging.info("Clean up...")
    # signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    # signal.signal(signal.SIGTERM, signal.SIG_DFL)
    # signal.signal(signal.SIGINT, signal.SIG_DFL)


def _signal_handler(sig, frame):
    logging.warning("Interrupted")
    sys.exit(0)


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
        help="Number of records to be posted for each rate interval",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--report-interval",
        help="Delivery report interval",
        type=int,
        default=10,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )

    # Nazare Specific Options
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

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    scheme = "https" if args.redpanda_ssl else "http"

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    atexit.register(_cleanup)

    elapsed = 0
    report_count = 0
    records = []
    while True:
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

            values = encode(row, args.output_type)

            if args.kafka_key is None:
                record = dict(value=values, partition=args.kafka_partition)
            else:
                record = dict(
                    key=args.kafka_key.encode("utf-8"),
                    value=values,
                    partition=args.kafka_partition,
                )
            records.append(record)

            elapsed += args.interval

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
        records = []

        report_count += 1
        if report_count >= args.report_interval:
            logging.info(
                "Message posted: count=%s, len=%s, response=%s",
                report_count,
                len(records),
                json.dumps(res.json()),
            )
            report_count = 0

        wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
        wait = 0.0 if wait < 0 else wait
        elapsed = 0
        time.sleep(wait)
