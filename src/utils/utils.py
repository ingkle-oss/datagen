import ast
import csv
import logging
import os
from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Literal
from urllib.parse import urlparse

import boto3
import bson
import orjson
import pyarrow.dataset as ds
from botocore.client import Config
from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
from fastnumbers import check_float

s3_config = Config(connect_timeout=30, read_timeout=30, retries={"max_attempts": 0})


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

    s3 = session.client(service_name="s3", endpoint_url=endpoint, config=s3_config)
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


class DateCodec(TypeCodec):
    python_type = date
    bson_type = str

    def transform_python(self, value: date):
        return str(value)

    def transform_bson(self, value: str):
        return date.fromisoformat(value)


codec_options = CodecOptions(type_registry=TypeRegistry([DateCodec()]))


def encode(value: dict | str, type: str) -> bytes:
    if type == "csv":
        return ",".join([str(val) for val in value.values()]).encode("utf-8")
    elif type == "json":
        return orjson.dumps(value, default=str)
    elif type == "bson" or type == "edge":
        return bson.encode(value, codec_options=codec_options)

    return value.encode("utf-8")


def decode(val: str | bytes, type: str):
    if type == "csv":
        f = StringIO(val.decode("utf-8"))
        for line in csv.reader(f):
            value = line
            break
    elif type == "json":
        value = orjson.loads(val.decode("utf-8"))
    elif type == "bson" or type == "edge":
        value = bson.decode(val)
    elif type == "txt":
        value = val.decode("utf-8")
    else:
        value = val

    return value


def csv_loads(value: str, headers: list[str]) -> dict:
    vals = []
    for v in value.strip().split(","):
        if v == "":
            vals.append(None)
        elif check_float(v):
            vals.append(float(v))
        else:
            # 따옴표 제거
            v = v.strip('"')
            vals.append(v)

    return {k: v for k, v in dict(zip(headers, vals)).items() if v is not None}


def load_rows(
    filepath: str, filetype: str = Literal["csv", "json", "jsonl", "bson"]
) -> list[dict]:
    rows = []
    if filetype == "bson":
        with open(filepath, "rb") as f:
            for line in bson.decode_file_iter(f):
                rows.append(line)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            if filetype == "jsonl":
                for line in f:
                    rows.append(orjson.loads(line))
            elif filetype == "json":
                obj = orjson.loads(f.read())
                if isinstance(obj, list):
                    rows.extend(obj)
                else:
                    rows.append(obj)

            elif filetype == "csv":
                headers = f.readline().strip().split(",")
                while line := f.readline():
                    if not line.strip():
                        break

                    row = csv_loads(line, headers)
                    if not row:
                        continue

                    rows.append(row)
            else:
                raise RuntimeError(f"Unsupported file type: {filetype}")

    return rows


class LoadRows(object):
    def __init__(
        self,
        filepath: str,
        filetype: str = Literal["csv", "json", "jsonl", "bson", "parquet"],
    ):
        self.filepath = filepath
        self.filetype = filetype
        self.bson_iter = None
        self.json_iter = None
        self.json_obj = None
        self.pq_batches = None
        self.pq_batch = None
        self.pq_batch_idx = 0
        self.pq_batch_size = 1000
        self.pq_table = None

        if self.filetype == "bson":
            self.fo = open(self.filepath, "rb")
        elif self.filetype == "parquet":
            self.pq_batches = ds.dataset(self.filepath, format="parquet").to_batches(
                batch_size=self.pq_batch_size
            )
        else:
            self.fo = open(self.filepath, "r", encoding="utf-8")

    def __enter__(self):
        if self.filetype == "bson":
            self.bson_iter = bson.decode_file_iter(self.fo)
        elif self.filetype == "json":
            self.json_obj = orjson.loads(self.fo.read())
            if isinstance(self.json_obj, list):
                self.json_iter = iter(self.json_obj)
            else:
                self.json_iter = iter([self.json_obj])
        elif self.filetype == "csv":
            # CSV 헤더에서 따옴표 제거
            headers_line = self.fo.readline().strip()
            self.headers = [h.strip('"') for h in headers_line.split(",")]

        return self

    def __exit__(self, type, value, trace_back):
        if hasattr(self, 'fo') and self.fo is not None:
            self.fo.close()

    def rewind(self):
        if self.filetype == "bson":
            self.fo.seek(0)
            self.bson_iter = bson.decode_file_iter(self.fo)
        elif self.filetype == "json":
            if isinstance(self.json_obj, list):
                self.json_iter = iter(self.json_obj)
            else:
                self.json_iter = iter([self.json_obj])
        elif self.filetype == "jsonl":
            self.fo.seek(0)
        elif self.filetype == "csv":
            self.fo.seek(0)
            self.headers = self.fo.readline().strip().split(",")
        elif self.filetype == "parquet":
            self.pq_batches = ds.dataset(self.filepath, format="parquet").to_batches(
                batch_size=self.pq_batch_size
            )

    def __iter__(self):
        return self

    def __next__(self):
        if self.filetype == "bson":
            return next(self.bson_iter)
        elif self.filetype == "json":
            return next(self.json_iter)
        elif self.filetype == "jsonl":
            line = self.fo.readline()
            if not line:
                raise StopIteration

            row = orjson.loads(line)
        elif self.filetype == "parquet":
            if self.pq_batch is None or self.pq_batch_idx >= self.pq_batch.num_rows:
                self.pq_batch = next(self.pq_batches)
                self.pq_batch_idx = 0
            row = self.pq_batch.slice(self.pq_batch_idx, 1).to_pylist()[0]
            self.pq_batch_idx += 1
        else:
            line = self.fo.readline()
            if not line:
                raise StopIteration

            if self.filetype == "csv":
                row = csv_loads(line, self.headers)
            elif self.filetype == "jsonl":
                row = orjson.loads(line)

        return row


def eval_create_func(eval_field_expr: str) -> Callable:
    fields = [
        node.id
        for node in ast.walk(ast.parse(eval_field_expr))
        if isinstance(node, ast.Name)
    ]
    return eval(
        "lambda " + ",".join(fields) + ",**kwargs" + ": " + eval_field_expr, {}, {}
    )
