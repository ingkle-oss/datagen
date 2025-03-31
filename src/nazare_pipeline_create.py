#!python

import argparse
import logging

from utils.nazare import nz_pipeline_create
from utils.utils import download_s3file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    # Nazare Specific Options
    parser.add_argument("--nz-pipeline-name", help="Pipeline name", required=True)
    parser.add_argument(
        "--nz-pipeline-type",
        help="Pipeline type",
        choices=["EDGE", "KAFKA"],
        default="KAFKA",
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

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    schema_file = args.nz_schema_file
    if schema_file.startswith("s3a://"):
        schema_file = download_s3file(
            schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    nz_pipeline_create(
        args.nz_api_url,
        args.nz_api_username,
        args.nz_api_password,
        args.nz_pipeline_name,
        args.nz_schema_file_type,
        schema_file,
        args.nz_pipeline_type,
        args.nz_pipeline_deltasync_enabled,
        args.nz_pipeline_retention,
        logger=logging,
    )
