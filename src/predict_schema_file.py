#!python

import io
import csv
import json
import argparse
import logging

from utils.utils import download_s3file, load_rows
from fastnumbers import check_float
from utils.nazare import convert_dict_to_schema, Field

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

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
        "--input-type",
        help="Input file type",
        choices=["csv", "jsonl"],
        default="jsonl",
    )
    parser.add_argument(
        "--output-type",
        help="Output file type",
        choices=["csv", "jsonl"],
        default="jsonl",
    )

    parser.add_argument(
        "--enable-timestamp",
        help="Enable timestamp",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--enable-date",
        help="Enable date",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    with open(filepath, "r", encoding="utf-8") as f:
        headers = f.readline().strip().split(",")

        while line := f.readline():
            row = {}
            if args.input_type == "csv":
                vals = line.strip().split(",")
                load_rows(filepath, args.input_type)

                row = [float(v) if check_float(v) else v for v in vals]
                row = dict(zip(headers, row))
            else:
                row = json.loads(line)

            if not row:
                raise ValueError("Empty row")

            if None in row.values() or "" in row.values():
                continue

            fields = convert_dict_to_schema(row)

            if args.enable_timestamp:
                fields.insert(0, Field(name="timestamp", type="timestamp"))
            if args.enable_date:
                fields.insert(1, Field(name="date", type="date"))

            if args.output_type == "jsonl":
                for field in fields:
                    print(field.model_dump_json())
            else:
                output = io.StringIO()
                writer = csv.DictWriter(
                    output, fieldnames=Field.model_json_schema()["properties"].keys()
                )
                writer.writeheader()
                for field in fields:
                    writer.writerow(field.model_dump())

                print(output.getvalue())
                output.close()

            break
