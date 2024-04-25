#!python

# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
# https://github.com/confluentinc/confluent-kafka-python/tree/master/examples

import argparse
import logging

import psycopg

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
        "--postgresql-table", help="Postgresql database", default="fields"
    )

    parser.add_argument(
        "--meta-table",
        help="Meta table name (e.g. ingkle.datagen.ingest)",
        required=True,
    )

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
        "--field-word-count", help="Number of word field", type=int, default=0
    )
    parser.add_argument(
        "--field-text-count", help="Number of text field", type=int, default=0
    )
    parser.add_argument(
        "--field-name-count", help="Number of name field", type=int, default=0
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    with psycopg.connect(
        host=args.postgresql_host,
        port=args.postgresql_port,
        user=args.postgresql_username,
        password=args.postgresql_password,
        dbname=args.postgresql_database,
    ) as conn:
        with conn.cursor() as cur:
            print(f"Deleting all with table {args.meta_table}")
            cur.execute(
                f"DELETE FROM {args.postgresql_table} WHERE \"table\" = '{args.meta_table}'"
            )

            print(f"Inserting meta fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, "timestamp", "timestamp", False),
                    (args.meta_table, "date", "date", False),
                    (args.meta_table, "hour", "string", False),
                    (args.meta_table, "__meta__offset", "integer", True),
                    (args.meta_table, "__meta__partition", "integer", True),
                    (args.meta_table, "__meta__topic", "string", True),
                    (args.meta_table, "__meta__txn_target", "string", True),
                ],
            )

            print(f"Inserting integer fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, name, "integer", True)
                    for name in [f"int_{i}" for i in range(args.field_int_count)]
                ],
            )

            print(f"Inserting float fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, name, "float", True)
                    for name in [f"float_{i}" for i in range(args.field_float_count)]
                ],
            )

            print(f"Inserting string fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, name, "string", True)
                    for name in [f"str_{i}" for i in range(args.field_str_count)]
                ],
            )

            print(f"Inserting word fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, name, "string", True)
                    for name in [f"word_{i}" for i in range(args.field_word_count)]
                ],
            )

            print(f"Inserting text fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %s, %s);
                """,
                [
                    (args.meta_table, name, "string", True)
                    for name in [f"text_{i}" for i in range(args.field_text_count)]
                ],
            )

            print(f"Inserting name fields with table {args.meta_table}")
            cur.executemany(
                f"""
                INSERT INTO {args.postgresql_table} ("table_name", name, type, nullable)
                VALUES (%s, %s, %S);
                """,
                [
                    (args.meta_table, name, "string", True)
                    for name in [f"name_{i}" for i in range(args.field_name_count)]
                ],
            )
