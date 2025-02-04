#!python

import re
import logging
import requests

from pydantic import BaseModel


class Field(BaseModel):
    name: str
    type: str


def _pipeline_check(store_url: str, store_username: str, password: str, name: str):
    session = requests.Session()
    session.auth = (store_username, password)
    response = session.get(
        store_url + f"/{name}",
        headers={"Content-Type": "application/json"},
    )
    if not response.ok:
        return False

    return True


def pipeline_create(
    store_api_url: str,
    store_api_username: str,
    store_api_password: str,
    table_name: str,
    fields: list[Field],
    location: str,
    logger: logging.Logger,
):
    if _pipeline_check(
        store_api_url,
        store_api_username,
        store_api_password,
        table_name,
    ):
        logger.info("Pipeline already exists: %s", table_name)
        return

    logger.info("Creating new table: %s", table_name)

    if re.match(r"^[a-z0-9_]+$", table_name) is None:
        raise Exception(f"Invalid table name: {table_name}")

    fields_create = []
    for field in fields:
        fields_create.append(
            {
                "name": field.name,
                "type": field.type,
                "table_name": table_name,
            }
        )

    data = {
        "name": table_name,
        "alias": table_name,
        "ingest_type": "KAFKA",
        "deltasync_enabled": False,
        "table_create": {
            "name": table_name,
            "alias": table_name,
            "location": location,
            "partitions": ["date"],
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
    logger.info("Pipeline is created: %s", table_name)
