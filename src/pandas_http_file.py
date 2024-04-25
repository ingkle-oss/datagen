#!python

# https://docs.redpanda.com/current/develop/http-proxy/?tab=tabs-1-redpanda-yaml

import argparse
import json
import logging
import time

import pendulum
import requests
from fastnumbers import check_float
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from utils.nzfake import NZFaker
from utils.utils import download_s3file, encode, load_values

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
        "--s3endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3accesskey", help="S3 accesskey")
    parser.add_argument("--s3secretkey", help="S3 secretkey")

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

    key_vals = {}
    for kv in args.key_vals:
        key, val = kv.split("=")
        key_vals[key] = val

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    scheme = "https" if args.ssl else "http"

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3accesskey, args.s3secretkey, args.s3endpoint
        )

    # For bigfile, load file one by one
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                header = f.readline()
                header = header.strip().split(",")

            body_start = f.tell()

            while True:
                now = pendulum.now("UTC")
                rows = []
                for idx in range(args.rate):
                    epoch = now + pendulum.duration(
                        microseconds=idx * (1000000 / args.rate)
                    )

                    row = f.readline()
                    if not row:
                        f.seek(body_start)
                        row = f.readline()

                    if args.input_type == "csv":
                        row = row.strip().split(",")
                        row = [float(v) if check_float(v) else v for v in row]
                        row = dict(zip(header, row))
                    else:
                        row = json.loads(row)

                    row = {
                        "timestamp": epoch.timestamp(),
                        **key_vals,
                        **row,
                    }
                    if args.field_date:
                        row["date"] = epoch.format("YYYY-MM-DD")
                    if args.field_hour:
                        row["hour"] = epoch.format("HH")

                    if args.key is None:
                        record = dict(
                            value=encode(row, args.output_type),
                            partition=args.partition,
                        )
                    else:
                        record = dict(
                            key=args.key.encode("utf-8"),
                            value=encode(row, args.output_type),
                            partition=args.partition,
                        )
                    rows.append(record)

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

                wait = 1.0 - (pendulum.now("UTC") - now).total_seconds()
                wait = 0.0 if wait < 0 else wait
                logging.info("Waiting for %f seconds...", wait)
                time.sleep(wait)
            logging.info("Finished")

    values = load_values(filepath, args.input_type)
    val_idx = 0
    while True:
        now = pendulum.now("UTC")
        rows = []
        for idx in range(args.rate):
            epoch = now + pendulum.duration(microseconds=idx * (1000000 / args.rate))

            row = {
                "timestamp": epoch.timestamp(),
                **key_vals,
                **values[val_idx],
            }
            val_idx = (val_idx + 1) % len(values)

            if args.field_date:
                row["date"] = epoch.format("YYYY-MM-DD")
            if args.field_hour:
                row["hour"] = epoch.format("HH")

            if args.key is None:
                record = dict(
                    value=encode(row, args.output_type), partition=args.partition
                )
            else:
                record = dict(
                    key=args.key.encode("utf-8"),
                    value=encode(row, args.output_type),
                    partition=args.partition,
                )
            rows.append(record)

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

        wait = 1.0 - (pendulum.now("UTC") - now).total_seconds()
        wait = 0.0 if wait < 0 else wait
        logging.info("Waiting for %f seconds...", wait)
        time.sleep(wait)

    logging.info("Finished")
