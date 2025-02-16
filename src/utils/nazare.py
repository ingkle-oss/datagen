#!python

import logging
import re
from datetime import datetime
from typing import Callable, Literal

import requests
from pydantic import BaseModel, TypeAdapter

from utils.utils import load_rows


class Field(BaseModel):
    name: str
    type: Literal[
        "integer",
        "long",
        "string",
        "float",
        "double",
        "boolean",
        "binary",
        "date",
        "timestamp",
        "timestamp_ntz",
    ]
    subtype: str | None = None
    nullable: bool = True
    comment: str | None = None
    alias: str | None = None


def _pipeline_check(
    store_url: str,
    store_username: str,
    password: str,
    name: str,
    logger: logging.Logger = logging,
):
    logger.info("Checking pipeline existence: %s", name)
    session = requests.Session()
    session.auth = (store_username, password)
    response = session.get(
        store_url + f"/{name}",
        headers={"Content-Type": "application/json"},
    )
    logger.debug("%s, %s", response.status_code, response.text)
    if response.status_code == 200:
        if response.json().get("is_deleting", False):
            raise RuntimeError(f"Pipeline is being deleted: {name}")

        return True

    if response.status_code == 404:
        return False

    raise RuntimeError(
        f"Failed to get table: {response.status_code}, {response.reason}"
    )


def pipeline_create(
    store_api_url: str,
    store_api_username: str,
    store_api_password: str,
    pipeline_name: str,
    fields: list[Field],
    enable_deltasync: bool = False,
    delete_retention: str = "",
    logger: logging.Logger = logging,
) -> bool:
    if _pipeline_check(
        store_api_url,
        store_api_username,
        store_api_password,
        pipeline_name,
        logger=logger,
    ):
        logger.warning("Pipeline already exists: %s", pipeline_name)
        return

    logger.info("Creating new pipeline: %s", pipeline_name)

    if re.match(r"^[a-z0-9_]+$", pipeline_name) is None:
        raise RuntimeError(f"Invalid table name: {pipeline_name}")

    fields_create = []
    for field in fields:
        fields_create.append(field.model_dump(exclude_none=True))

    if logger.root.level <= logging.DEBUG:
        logger.debug("Fields:")
        for field in fields_create:
            logger.debug(field)

    data = {
        "name": pipeline_name,
        "alias": pipeline_name,
        "ingest_type": "KAFKA",
        "deltasync_enabled": enable_deltasync,
        "table_create": {
            "name": pipeline_name,
            "alias": pipeline_name,
            "partitions": ["date"],
            "delete_retention": delete_retention,
            "fields_create": fields_create,
        },
    }

    session = requests.Session()
    session.auth = (store_api_username, store_api_password)
    response = session.post(
        store_api_url,
        headers={"Content-Type": "application/json"},
        json=data,
    )
    logger.debug("%s, %s", response.status_code, response.text)
    response.raise_for_status()

    logger.info("Pipeline is created: %s", pipeline_name)


def pipeline_delete(
    store_api_url: str,
    store_api_username: str,
    store_api_password: str,
    pipeline_name: str,
    logger: logging.Logger = logging,
):
    if not _pipeline_check(
        store_api_url,
        store_api_username,
        store_api_password,
        pipeline_name,
    ):
        logger.info("Pipeline does not exist: %s", pipeline_name)
        return

    logger.info("Deleting pipeline: %s", pipeline_name)

    session = requests.Session()
    session.auth = (store_api_username, store_api_password)
    response = session.delete(
        store_api_url + f"/{pipeline_name}",
        headers={"Content-Type": "application/json"},
    )
    logger.debug("%s, %s", response.status_code, response.text)
    response.raise_for_status()

    logger.info("Pipeline is deleted: %s", pipeline_name)


def predict_field(key: str, val: any) -> Field:
    mapping = {
        # int: "integer",
        int: "double",  # if lack of sample data, it is hard to predict integer
        str: "string",
        float: "double",
        bool: "boolean",
        bytes: "binary",
    }

    if not isinstance(key, str) or not key:
        raise RuntimeError("Cannot predict schema because of empty key")

    if val is None or val == "":
        return None

    try:
        return Field(name=key, type=mapping[type(val)])
    except Exception as e:
        raise RuntimeError("Cannot predict schema because it has unrecognized value", e)


def load_schema_file(schema_file: str, schema_file_type: str) -> list[Field]:
    rows = load_rows(schema_file, schema_file_type)
    if len(rows) > 1000:
        raise RuntimeError(f"Too many rows in the schema file: {len(rows)}")

    fields = TypeAdapter(list[Field]).validate_python(
        load_rows(schema_file, schema_file_type)
    )
    if "timestamp" not in [field.name for field in fields] or "date" not in [
        field.name for field in fields
    ]:
        raise RuntimeError("Schema file must have 'timestamp' and 'date' fields")

    return fields


class RowTransformer:
    incremental_idx = 0
    interval_field_prev = None

    interval_field = None
    interval_field_divisor = 1.0
    interval_field_diff = None
    interval_field_diff_format = "%Y-%m-%d %H:%M:%S"
    incremental_field = None
    incremental_field_step = None
    datetime_field = None
    datetime_field_format = None
    eval_field = None
    eval_func = None

    def __init__(
        self,
        incremental_field_from=0,
        interval_field: str = None,
        interval_field_unit: str = None,
        interval_field_diff: str = None,
        interval_field_diff_format: str = None,
        incremental_field: str = None,
        incremental_field_step: int = None,
        datetime_field: str = None,
        datetime_field_format: str = None,
        eval_field: str = None,
        eval_func: Callable = None,
    ):
        self.incremental_idx = incremental_field_from
        self.interval_field = interval_field
        self.interval_field_diff = interval_field_diff
        self.interval_field_diff_format = interval_field_diff_format
        self.incremental_field = incremental_field
        self.incremental_field_step = incremental_field_step
        self.datetime_field = datetime_field
        self.datetime_field_format = datetime_field_format
        self.eval_field = eval_field
        self.eval_func = eval_func

        if interval_field:
            if interval_field_unit == "second":
                self.interval_field_divisor = 1.0
            elif interval_field_unit == "millisecond":
                self.interval_field_divisor = 1e3
            elif self.interval_field_unit == "microsecond":
                self.interval_field_divisor = 1e6
            elif interval_field_unit == "nanosecond":
                self.interval_field_divisor = 1e9
            else:
                raise RuntimeError(
                    "Invalid interval field unit: %s", interval_field_unit
                )

    def transform(
        self, row: dict, epoch: datetime, interval: float
    ) -> tuple[dict, float]:
        row = {k: v for k, v in row.items() if k is not None and v is not None}

        if self.interval_field in row:
            interval = row[self.interval_field] / self.interval_field_divisor
        elif self.interval_field_diff in row:
            interval_diff = datetime.strptime(
                row[self.interval_field_diff], self.interval_field_diff_format
            )
            if self.interval_field_prev:
                print(interval_diff, self.interval_field_prev)
                if interval_diff >= self.interval_field_prev:
                    interval = (
                        interval_diff - self.interval_field_prev
                    ).total_seconds()
            self.interval_field_prev = interval_diff
        print(interval)

        if self.incremental_field:
            row[self.incremental_field] = self.incremental_idx
            self.incremental_idx += self.incremental_field_step

        if self.datetime_field and self.datetime_field_format:
            row[self.datetime_field] = epoch.strftime(self.datetime_field_format)

        if self.eval_field and self.eval_func:
            row[self.eval_field] = self.eval_func(**row)

        return row, interval
