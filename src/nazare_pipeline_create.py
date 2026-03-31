#!python

import argparse
import logging

from utils.k8s_config import add_k8s_pipeline_args, build_pipeline_config
from utils.k8s_deploy import create_unified_pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    parser.add_argument("--nz-pipeline-name", help="Pipeline name", required=True)
    parser.add_argument(
        "--nz-pipeline-type",
        help="Pipeline type",
        choices=["EDGE", "KAFKA", "MQTT"],
        default="KAFKA",
    )

    add_k8s_pipeline_args(parser)

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    config = build_pipeline_config(args, args.nz_pipeline_name, args.nz_pipeline_type)
    create_unified_pipeline(config)
