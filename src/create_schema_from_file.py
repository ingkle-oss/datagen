#!python

import argparse
import csv
import io
import logging

from utils.nazare import Field, nz_predict_field
from utils.utils import LoadRows, download_s3file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # File
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
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
        help="Output file type",
        choices=["csv", "jsonl"],
        default="jsonl",
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

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    filepath = args.input_filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    fields = []
    with LoadRows(filepath, args.input_type) as rows:
        while True:
            try:
                row = next(rows)
            except StopIteration as e:
                raise RuntimeError(
                    "Cannot predict schema before reaching the end of file", e
                )

            if all(k in [f.name for f in fields] for k, v in row.items()):
                break

            for k, v in row.items():
                field = nz_predict_field(k, v)
                if field is None:
                    continue

                prev = [f for f in fields if f.name == k]
                if prev:
                    if prev[0].type != field.type:
                        raise RuntimeError(
                            f"Field type is evolving: key={field.name}, value={prev[0].type} --> {field.type}"
                        )
                    continue

                fields.append(field)

        if args.timestamp_enabled:
            fields.insert(0, Field(name="timestamp", type="timestamp"))
        if args.date_enabled:
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
