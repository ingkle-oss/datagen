#!python

import re
import logging
import requests

from pydantic import BaseModel, TypeAdapter

from typing import Literal


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
    subtype: str = None
    nullable: bool = True
    comment: str = None
    alias: str = None


def _pipeline_check(
    store_url: str,
    store_username: str,
    password: str,
    name: str,
    logger: logging.Logger = logging,
):
    session = requests.Session()
    session.auth = (store_username, password)
    response = session.get(
        store_url + f"/{name}",
        headers={"Content-Type": "application/json"},
    )
    if not response.ok:
        return False

    if response.json().get("is_deleting", False):
        raise Exception(f"Pipeline is being deleted: {name}")

    return True


def pipeline_create(
    store_api_url: str,
    store_api_username: str,
    store_api_password: str,
    pipeline_name: str,
    fields: dict,
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
        logger.info("Pipeline already exists: %s", pipeline_name)
        return

    logger.info("Creating new pipeline: %s", pipeline_name)

    if re.match(r"^[a-z0-9_]+$", pipeline_name) is None:
        raise Exception(f"Invalid table name: {pipeline_name}")

    fields_create = []
    for field in TypeAdapter(list[Field]).validate_python(fields):
        fields_create.append(
            {
                "name": field.name,
                "type": field.type,
                "subtype": field.subtype,
                "nullable": field.nullable,
                "comment": field.comment,
                "alias": field.alias,
                "" "table_name": pipeline_name,
            }
        )

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
    if not response.ok:
        raise Exception(
            f"Failed to create table: {response.status_code}, {response.reason}"
        )
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
    if not response.ok:
        raise Exception(
            f"Failed to delete table: {response.status_code}, {response.reason}"
        )
    logger.info("Pipeline is deleted: %s", pipeline_name)


def convert_dict_to_schema(org: dict) -> list[Field]:
    mapping = {
        int: "long",
        str: "string",
        float: "double",
        bool: "boolean",
        bytes: "binary",
    }
    fields = []
    for k, v in org.items():
        fields.append(Field(name=k, type=mapping[type(v)]))

    return fields
