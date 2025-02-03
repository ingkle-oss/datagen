#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import json
import logging

import psycopg
from fastnumbers import check_float

from utils.utils import download_s3file, load_rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--postgresql-host",
        help="Postgresql host",
        default="postgresql-ha-pgpool.postgresql-ha.svc.cluster.local",
    )
    parser.add_argument(
        "--postgresql-port",
        help="Postgresql port",
        default=5432,
        type=int,
    )
    parser.add_argument(
        "--postgresql-username", help="Postgresql username", required=True
    )
    parser.add_argument(
        "--postgresql-password", help="Postgresql password", required=True
    )
    parser.add_argument(
        "--postgresql-database", help="Postgresql database", default="store"
    )

    parser.add_argument(
        "--meta-table",
        help="Meta table name (e.g. ingkle.datagen.ingest)",
        required=True,
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

    parser.add_argument(
        "--custom-rows",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    custom_rows = {}
    for kv in args.custom_rows:
        key, val = kv.split("=")
        custom_rows[key] = val

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                line = f.readline()
                headers = line.strip().split(",")

            body_start = f.tell()

            line = f.readline()
            if not line:
                f.seek(body_start)
                line = f.readline()

            if args.input_type == "csv":
                values = [
                    float(v) if check_float(v) else v for v in line.strip().split(",")
                ]
                values = dict(zip(headers, values))
            else:
                values = json.loads(line)

            if not values and not custom_rows:
                raise RuntimeError("No values to be used")
    else:
        values = load_rows(filepath, args.input_type)
        if not values and not custom_rows:
            logging.warning("No values to be produced")
            exit(0)

        values = values[0]

    values = {**values, **custom_rows}

    with psycopg.connect(
        host=args.postgresql_host,
        port=args.postgresql_port,
        user=args.postgresql_username,
        password=args.postgresql_password,
        dbname=args.postgresql_database,
    ) as conn:
        with conn.cursor() as cur:
            logging.info("Creating table if not exists")
            cur.execute(
                f"""
                INSERT INTO tables (name, alias, type, storage, location, partitions, options, delete_retention)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (
                    args.meta_table,
                    args.meta_table,
                    "EXTERNAL",
                    "DELTA",
                    f"s3://ingkle-com-ingkle/{args.meta_table}/ingest",
                    '["date"]',
                    "{}",
                    "30,d",
                ),
            )

            logging.info(f"Deleting all fields with table {args.meta_table}")
            cur.execute(
                f"DELETE FROM fields WHERE \"table_name\" = '{args.meta_table}'"
            )

            logging.info(f"Inserting default meta fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO fields (table_name, name, type, nullable, subtype)
                VALUES (%s, %s, %s, %s, %s);
                """,
                [
                    (args.meta_table, "timestamp", "timestamp", True, "microsecond"),
                    (args.meta_table, "date", "date", True, None),
                    (args.meta_table, "__meta__offset", "long", True, None),
                    (args.meta_table, "__meta__partition", "integer", True, None),
                    (args.meta_table, "__meta__topic", "string", True, None),
                    (args.meta_table, "__meta__txn_target", "string", True, None),
                ],
            )

            for key, val in values.items():
                if isinstance(val, int):
                    _type = "integer"
                elif isinstance(val, float):
                    _type = "float"
                elif isinstance(val, str):
                    _type = "string"

                logging.info(
                    f"Inserting {_type} fields({key}, {val}) with table {args.meta_table}"
                )
                cur.executemany(
                    f"""
                    INSERT INTO fields ("table_name", name, type, nullable)
                    VALUES (%s, %s, %s, %s);
                    """,
                    [(args.meta_table, key, _type, True)],
                )

            conn.commit()
