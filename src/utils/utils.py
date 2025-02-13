import ast
import json
import logging
import os
from collections.abc import Callable
from typing import Literal
from urllib.parse import urlparse

import boto3
import bson
from fastnumbers import check_float


def encode(value: dict | str, type: str):
    if type == "bson":
        return bson.encode(value)
    elif type == "json":
        return json.dumps(value, default=str)
    elif type == "csv":
        return ",".join([str(val) for val in value.values()])

    return value.encode("utf-8")


def csv_loads(value: str, headers: list[str]):
    row = []
    for v in value.strip().split(","):
        if v == "":
            row.append(None)
        elif check_float(v):
            row.append(float(v))
        else:
            row.append(v)

    return dict(zip(headers, row))


def load_rows(filepath: str, filetype: str) -> list[dict] | list[str]:
    rows = []
    if filetype == "bsonl":
        with open(filepath, "rb") as f:
            for line in bson.decode_file_iter(f):
                rows.append(line)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            if filetype == "jsonl":
                for line in f:
                    rows.append(json.loads(line))
            elif filetype == "csv":
                headers = f.readline().strip().split(",")
                while line := f.readline():
                    if not line:
                        break

                    row = csv_loads(line, headers)
                    rows.append(dict(zip(headers, row)))

    return rows


class LoadRows(object):
    def __init__(self, filepath: str, filetype: str = Literal["csv", "jsonl", "bsonl"]):
        self.filepath = filepath
        self.filetype = filetype
        self.bson_iter = None

        if self.filetype == "bsonl":
            self.fo = open(self.filepath, "rb")
        else:
            self.fo = open(self.filepath, "r", encoding="utf-8")

    def __enter__(self):
        if self.filetype == "bsonl":
            self.bson_iter = bson.decode_file_iter(self.fo)
        elif self.filetype == "csv":
            self.headers = self.fo.readline().strip().split(",")

        return self

    def __exit__(self, type, value, trace_back):
        self.fo.close()

    def seek(self, pos):
        self.fo.seek(pos)
        if self.filetype == "bsonl":
            self.bson_iter = bson.decode_file_iter(self.fo)
        elif self.filetype == "csv":
            self.headers = self.fo.readline().strip().split(",")

    def __iter__(self):
        return self

    def __next__(self):
        if self.filetype == "bsonl":
            return next(self.bson_iter)
        else:
            line = self.fo.readline()
            if not line:
                raise StopIteration

            if self.filetype == "csv":
                row = csv_loads(line, self.headers)
            elif self.filetype == "jsonl":
                row = json.loads(line)

        return row


def download_s3file(filepath: str, accesskey: str, secretkey: str, endpoint: str):
    logging.info("Downloading file %s from S3... %s", filepath, endpoint)
    tok = urlparse(filepath)

    src_bucket = tok.netloc
    src_key = tok.path.lstrip("/")
    _filepath = os.path.basename(tok.path)

    session = boto3.Session(
        aws_access_key_id=accesskey,
        aws_secret_access_key=secretkey,
        aws_session_token=None,
        region_name="ap-northeast-2",
    )

    s3 = session.client(service_name="s3", endpoint_url=endpoint)
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


def eval_create_func(eval_field_expr: str) -> Callable:
    fields = [
        node.id
        for node in ast.walk(ast.parse(eval_field_expr))
        if isinstance(node, ast.Name)
    ]
    return eval("lambda " + ",".join(fields) + ",**kwargs" + ": " + eval_field_expr)
