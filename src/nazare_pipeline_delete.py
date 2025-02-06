#!python

import argparse
import logging

from utils.nazare import pipeline_delete

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # NZStore REST API
    parser.add_argument(
        "--store-api-url",
        help="Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument(
        "--store-api-username",
        help="Store API username",
        required=True,
    )
    parser.add_argument(
        "--store-api-password",
        help="Store API password",
        required=True,
    )

    parser.add_argument(
        "--pipeline-name",
        help="Pipeline name",
        required=True,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    pipeline_delete(
        args.store_api_url,
        args.store_api_username,
        args.store_api_password,
        args.pipeline_name,
        logger=logging,
    )
