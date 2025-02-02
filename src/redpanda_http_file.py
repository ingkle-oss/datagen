#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import requests
from fastnumbers import check_float
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from utils.utils import download_s3file, encode, load_values


def create_record(
    incremental_field: str,
    unique_alt_field: str,
    record_interval_field: str,
    interval_divisor: float,
    kafka_partition: str,
    key: str | None,
    epoch: datetime,
    values: dict,
) -> tuple[dict, float]:
    global INCREMENTAL_IDX
    global UNIQUE_ALT_PREV_VALUE
    global UNIQUE_ALT_IDX

    values = {k: v for k, v in values.items() if v is not None}

    if incremental_field and incremental_field in values:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += 1

    if unique_alt_field and unique_alt_field in values:
        if (UNIQUE_ALT_PREV_VALUE is None) or (
            UNIQUE_ALT_PREV_VALUE != values[unique_alt_field]
        ):
            UNIQUE_ALT_PREV_VALUE = values[unique_alt_field]
            UNIQUE_ALT_IDX += 1
        values[unique_alt_field] = UNIQUE_ALT_IDX

    wait = None
    if record_interval_field and record_interval_field in values:
        wait = values[record_interval_field] / interval_divisor

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **custom_key_vals,
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
    return record, wait


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

    parser.add_argument("--filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--bigfile",
        help="Whether file is big or not (default: False)",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "json", "bson"],
        default="json",
    )

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson"],
        default="json",
    )
    parser.add_argument(
        "--custom-key-vals",
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
    parser.add_argument(
        "--rate-interval",
        help="Rate interval in seconds",
        type=float,
        default=None,
    )

    # Record interval
    parser.add_argument(
        "--record-interval-field",
        help="Interval field (float) between records",
        default=None,
    )
    parser.add_argument(
        "--record-interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
        default=None,
    )

    # Field options
    parser.add_argument(
        "--incremental-field",
        help="Incremental field (int) from 0",
        default=None,
    )
    parser.add_argument(
        "--unique-alt-field",
        help="Use unique values for alternative field (float type)",
        default=None,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    custom_key_vals = {}
    for kv in args.custom_key_vals:
        key, val = kv.split("=")
        custom_key_vals[key] = val

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    scheme = "https" if args.redpanda_ssl else "http"

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    rate = args.rate
    divisor = 1.0
    if args.record_interval_field:
        if args.record_interval_field_unit == "second":
            pass
        elif args.record_interval_field_unit == "millisecond":
            divisor = 1e3
        elif args.record_interval_field_unit == "microsecond":
            divisor = 1e6
        elif args.record_interval_field_unit == "nanosecond":
            divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s" % args.record_interval_field_unit
            )
        logging.info("Ignores --rate option...")
        rate = 1

    INCREMENTAL_IDX = 0
    UNIQUE_ALT_PREV_VALUE = None
    UNIQUE_ALT_IDX = -1

    # For bigfile, load file one by one
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                line = f.readline()
                headers = line.strip().split(",")

            body_start = f.tell()

            while True:
                wait = None
                now = datetime.now(timezone.utc)
                records = []
                for idx in range(rate):
                    epoch = now + timedelta(microseconds=idx * (1000000 / rate))

                    line = f.readline()
                    if not line:
                        f.seek(body_start)
                        line = f.readline()

                    if args.input_type == "csv":
                        values = [
                            float(v) if check_float(v) else v
                            for v in line.strip().split(",")
                        ]
                        values = dict(zip(headers, values))
                    else:
                        values = json.loads(line)

                    if not values and not custom_key_vals:
                        logging.debug("No values to be produced")
                        continue

                    record, wait = create_record(
                        args.incremental_field,
                        args.unique_alt_field,
                        args.record_interval_field,
                        divisor,
                        args.kafka_partition,
                        args.kafka_key,
                        epoch,
                        values,
                    )
                    records.append(record)

                res = requests.post(
                    url=(
                        f"{scheme}://{args.kafka_sasl_username}:{args.kafka_sasl_password}@{args.redpanda_host}:{args.redpanda_port}"
                        f"/topics/{args.kafka_topic}"
                    ),
                    data=encode({"records": records}, args.output_type),
                    headers={
                        "Content-Type": "application/vnd.kafka.json.v2+json",
                        "content-encoding": "gzip",
                    },
                    verify=args.redpanda_verify,
                ).json()

                logging.info(
                    f"Total {len(records)} messages delivered: {json.dumps(res, indent=2)}"
                )

                if wait or args.rate_interval:
                    if args.rate_interval:
                        wait = (
                            args.rate_interval
                            - (datetime.now(timezone.utc) - now).total_seconds()
                        )
                        wait = 0.0 if wait < 0 else wait

                    logging.info("Waiting for %f seconds...", wait)
                    time.sleep(wait)
            logging.info("Finished")

    else:
        values = load_values(filepath, args.input_type)
        if not values and not custom_key_vals:
            logging.warning("No values to be produced")
            exit(0)

        val_idx = 0
        while True:
            wait = None
            now = datetime.now(timezone.utc)
            records = []
            for idx in range(rate):
                epoch = now + timedelta(microseconds=idx * (1000000 / rate))

                record, wait = create_record(
                    args.incremental_field,
                    args.unique_alt_field,
                    args.record_interval_field,
                    divisor,
                    args.kafka_partition,
                    args.kafka_key,
                    epoch,
                    values[val_idx],
                )
                records.append(record)
                val_idx = (val_idx + 1) % len(values)

            res = requests.post(
                url=(
                    f"{scheme}://{args.kafka_sasl_username}:{args.kafka_sasl_password}@{args.redpanda_host}:{args.redpanda_port}"
                    f"/topics/{args.kafka_topic}"
                ),
                data=encode({"records": records}, args.output_type),
                headers={
                    "Content-Type": "application/vnd.kafka.json.v2+json",
                    "content-encoding": "gzip",
                },
                verify=args.redpanda_verify,
            ).json()
            logging.info(
                f"Total {len(records)} messages delivered: {json.dumps(res, indent=2)}"
            )

            if wait or args.rate_interval:
                if args.rate_interval:
                    wait = (
                        args.rate_interval
                        - (datetime.now(timezone.utc) - now).total_seconds()
                    )
                    wait = 0.0 if wait < 0 else wait

                logging.info("Waiting for %f seconds...", wait)
                time.sleep(wait)

        logging.info("Finished")
