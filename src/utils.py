import json

import bson


def encode(value: dict | str, type: str):
    if type == "csv" or type == "txt":
        return ",".join([str(val) for val in value.values()])
    elif type == "bson":
        return bson.dumps(value)

    if isinstance(value, dict):
        return json.dumps(value, default=str)

    return value.encode("utf-8")
