#!python

import argparse
import csv
import io
import logging

from utils.nazare import Field, predict_field
from utils.utils import LoadRows, download_s3file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Fake
    parser.add_argument(
        "--fake-integer-count", help="Number of integer field", type=int, default=0
    )
    parser.add_argument(
        "--fake-long-count", help="Number of long field", type=int, default=0
    )
    parser.add_argument(
        "--fake-string-count", help="Number of string field", type=int, default=0
    )
    parser.add_argument(
        "--fake-float-count", help="Number of float field", type=int, default=0
    )
    parser.add_argument(
        "--fake-double-count", help="Number of double field", type=int, default=0
    )
    parser.add_argument(
        "--fake-boolean-count", help="Number of boolean field", type=int, default=0
    )
    parser.add_argument(
        "--fake-binary-count", help="Number of binary field", type=int, default=0
    )
    parser.add_argument(
        "--fake-date-count", help="Number of date field", type=int, default=0
    )
    parser.add_argument(
        "--fake-timestamp-count", help="Number of timestamp field", type=int, default=0
    )
    parser.add_argument(
        "--fake-timestamp_ntz-count",
        help="Number of timestamp_ntz field",
        type=int,
        default=0,
    )

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

    fields = []
    for i in range(args.fake_integer_count):
        fields.append(Field(name=f"integer_{i}", type="integer"))
    for i in range(args.fake_long_count):
        fields.append(Field(name=f"long_{i}", type="long"))
    for i in range(args.fake_string_count):
        fields.append(Field(name=f"string_{i}", type="string"))
    for i in range(args.fake_float_count):
        fields.append(Field(name=f"float_{i}", type="float"))
    for i in range(args.fake_double_count):
        fields.append(Field(name=f"double_{i}", type="double"))
    for i in range(args.fake_boolean_count):
        fields.append(Field(name=f"boolean_{i}", type="boolean"))
    for i in range(args.fake_binary_count):
        fields.append(Field(name=f"binary_{i}", type="binary"))
    for i in range(args.fake_date_count):
        fields.append(Field(name=f"date_{i}", type="date"))
    for i in range(args.fake_timestamp_count):
        fields.append(Field(name=f"timestamp_{i}", type="timestamp"))
    for i in range(args.fake_timestamp_ntz_count):
        fields.append(Field(name=f"timestamp_ntz_{i}", type="timestamp_ntz"))

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
