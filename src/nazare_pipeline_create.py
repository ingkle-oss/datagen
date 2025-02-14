#!python

import argparse
import logging

from utils.nazare import pipeline_create, load_schema_file
from utils.utils import download_s3file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # NZStore REST API
    parser.add_argument(
        "--store-api-url",
        help="Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument(
        "--store-api-username", help="Store API username", required=True
    )
    parser.add_argument(
        "--store-api-password", help="Store API password", required=True
    )

    # NZStore pipeline
    parser.add_argument("--pipeline-name", help="Pipeline name", required=True)
    parser.add_argument(
        "--pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--pipeline-deltasync-enabled",
        help="Enable deltasync",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    # File
    parser.add_argument("--schema-file", help="Schema file", required=True)
    parser.add_argument(
        "--schema-file-type",
        help="Schema file type",
        choices=["csv", "jsonl", "bsonl"],
        default="json",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

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

    pipeline_create(
        args.store_api_url,
        args.store_api_username,
        args.store_api_password,
        args.pipeline_name,
        load_schema_file(schema_file, args.schema_file_type),
        args.pipeline_deltasync_enabled,
        args.pipeline_retention,
        logger=logging,
    )
