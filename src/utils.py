import csv
import json
import logging
import os
from urllib.parse import urlparse

import boto3
import bson
from fastnumbers import check_float


def encode(value: dict | str, type: str):
    if type == "bson":
        return bson.dumps(value)
    elif type == "json":
        return json.dumps(value, default=str)
    elif type == "csv":
        return ",".join([str(val) for val in value.values()])

    return value.encode("utf-8")


def load_values(filepath: str, filetype: str) -> list[dict] | list[str]:
    values = []
    if filetype == "bson":
        with open(filepath, "rb") as f:
            values.append(bson.loads(f.read()))
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            if filetype == "json":
                for line in f:
                    values.append(json.loads(line))
            elif filetype == "csv":
                values = list(csv.DictReader(f))
                for value in values:
                    for k, v in value.items():
                        if check_float(v):
                            value[k] = float(v)
            else:
                values = f.readlines()

    return values


def download_s3file(filepath: str, s3accesskey: str, s3secretkey: str, s3endpoint: str):
    tok = urlparse(filepath)

    src_bucket = tok.netloc
    src_key = tok.path.lstrip("/")
    _filepath = os.path.basename(tok.path)

    session = boto3.Session(
        aws_access_key_id=s3accesskey,
        aws_secret_access_key=s3secretkey,
        aws_session_token=None,
        region_name="ap-northeast-2",
    )

    s3 = session.client(service_name="s3", endpoint_url=s3endpoint)
    meta_data = s3.head_object(Bucket=src_bucket, Key=src_key)
    total_length = int(meta_data.get("ContentLength", 0))

    PROGRESS = 0

    def _download_status(chunk):
        nonlocal PROGRESS
        PROGRESS += chunk
        done = int((PROGRESS / total_length) * 100000)
        if done % 1000 == 0:
            logging.info(
                "Processing... %s%% (%s/%s bytes)", done / 1000, PROGRESS, total_length
            )

    s3.download_file(src_bucket, src_key, _filepath, Callback=_download_status)
    logging.info("Download completed... %s", _filepath)

    return _filepath
