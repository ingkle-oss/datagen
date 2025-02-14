#!python

import logging
import re
from typing import Literal

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
