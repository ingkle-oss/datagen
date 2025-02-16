#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import requests
import urllib3

from utils.nazare import RowTransformer, load_schema_file, pipeline_create
from utils.utils import LoadRows, download_s3file, encode, eval_create_func

INCREMENTAL_IDX = 0
INTERVAL_DIFF_PREV = None


def create_record(
    kafka_partition: str,
    key: str | None,
    values: dict,
    epoch: datetime,
    interval: float,
    interval_field: str,
    interval_field_divisor: float,
    interval_field_diff: str,
    incremental_field: str,
    incremental_field_step: int,
    datetime_field: str,
    datetime_field_format: str,
    eval_field: str,
    eval_func: Callable,
) -> tuple[dict, float]:
    global INCREMENTAL_IDX

    values = {k: v for k, v in values.items() if v is not None}

    if interval_field and interval_field in values:
        interval = values[interval_field] / interval_field_divisor
    elif interval_field_diff and interval_field_diff in values:
        global INTERVAL_DIFF_PREV
        interval_diff = datetime.fromisoformat((values[interval_field_diff]))
        if INTERVAL_DIFF_PREV:
            interval = (interval_diff - INTERVAL_DIFF_PREV).total_seconds()
        INTERVAL_DIFF_PREV = interval_diff

    if incremental_field:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += incremental_field_step

    if datetime_field and datetime_field_format:
        values[datetime_field] = epoch.strftime(datetime_field_format)

    if eval_field and eval_func:
        values[eval_field] = eval_func(
            **values,
        )

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **values,
    }

    if key is None:
        record = dict(value=row, partition=kafka_partition)
    else:
        record = dict(
            key=key.encode("utf-8"),
            value=row,
            partition=kafka_partition,
        )
    return record, interval


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
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
    parser.add_argument("--schema-file", help="Schema file")
    parser.add_argument(
        "--schema-file-type",
        help="Schema file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
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
        help="Number of records for each loop",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )
    parser.add_argument(
        "--interval-field", help="Use field (float) value as interval between records"
    )
    parser.add_argument(
        "--interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
    )
    parser.add_argument(
        "--interval-field-diff",
        help="Use field(datetime) difference as interval between records",
    )
    parser.add_argument(
        "--interval-field-diff-format",
        help="Interval field difference format",
        default="%Y-%m-%d %H:%M:%S",
    )

    # Other field options
    parser.add_argument("--incremental-field", help="Incremental field (int)")
    parser.add_argument(
        "--incremental-field-from",
        help="Incremental field start value",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--incremental-field-step",
        help="Incremental field step value",
        type=int,
        default=1,
    )
    parser.add_argument("--datetime-field", help="Datetime field (datetime)")
    parser.add_argument(
        "--datetime-field-format", help="Datetime format", default="%Y-%m-%d %H:%M:%S"
    )
    parser.add_argument("--eval-field", help="Evaluated field")
    parser.add_argument("--eval-field-expr", help="Evaluated field expression")

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

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    if (
        args.store_api_url
        and args.store_api_username
        and args.store_api_password
        and args.schema_file
        and args.schema_file_type
    ):
        schema_file = args.schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )

        pipeline_create(
            args.store_api_url,
            args.store_api_username,
            args.store_api_password,
            args.kafka_topic,
            load_schema_file(schema_file, args.schema_file_type),
            args.pipeline_deltasync_enabled,
            args.pipeline_retention,
            logger=logging,
        )

    custom_row = {}
    for kv in args.custom_row:
        key, val = kv.split("=")
        custom_row[key] = val

    tf = RowTransformer(
        args.incremental_field_from,
        args.interval_field,
        args.interval_field_unit,
        args.interval_field_diff,
        args.interval_field_diff_format,
        args.incremental_field,
        args.incremental_field_step,
        args.datetime_field,
        args.datetime_field_format,
    )

    eval_func = None
    if args.eval_field and args.eval_field_expr:
        eval_func = eval_create_func(args.eval_field_expr)

    filepath = args.input_filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    scheme = "https" if args.redpanda_ssl else "http"

    with LoadRows(filepath, args.input_type) as rows:
        while True:
            elapsed = 0
            records = []
            start_time = datetime.now(timezone.utc)
            for _ in range(args.rate):
                ts = start_time + timedelta(seconds=elapsed)
                try:
                    row = next(rows)
                except StopIteration:
                    rows.seek(0)
                    row = next(rows)
                row, interval = tf.transform(row, ts, args.interval)
                row = row | custom_row

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

                elapsed += interval if interval > 0 else 0

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
            logging.debug(
                "%s, %s",
                encode({"records": records}, args.output_type),
                args.output_type,
            )
            logging.info(
                "Total %s messages delivered: %s",
                len(records),
                json.dumps(res.json(), indent=2),
            )

            wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
            wait = 0.0 if wait < 0 else wait
            time.sleep(wait)

        logging.info("Finished")
