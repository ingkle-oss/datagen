#!python

import ast
import enum
import logging
import re
import struct
from datetime import datetime
from typing import Callable, Literal

import requests
from pydantic import BaseModel, BeforeValidator, TypeAdapter
from typing_extensions import Annotated

from utils.utils import load_rows

TRIAGE_PREFIX = "__triage__@"


# * Edge
STRUCT_FMT = {
    "c": str,
    "b": int,
    "B": int,
    "?": bool,
    "h": int,
    "H": int,
    "i": int,
    "I": int,
    "l": int,
    "L": int,
    "q": int,
    "Q": int,
    "f": float,
    "d": float,
    "s": str,
    "p": str,
}


def _validate_format(val: str) -> str:
    fmt = val[-1:]
    if fmt not in STRUCT_FMT:
        raise ValueError(f"Invalid format: {val}")

    return val


def _validate_obj(val: str | dict) -> dict:
    if isinstance(val, dict):
        return val
    return ast.literal_eval(val)


class EdgeDataSpecType(str, enum.Enum):
    ANALOG = "ANALOG"
    DIGITAL = "DIGITAL"
    TEXT = "TEXT"


class EdgeDataSpec(BaseModel):
    edgeId: str
    edgeDataSourceId: str
    edgeDataSpecId: str

    type: EdgeDataSpecType | None = None
    format: Annotated[str, BeforeValidator(_validate_format)]
    size: int | None = None
    index: int
    is_null: bool = False
    bits: list[str | None] = []


class DataSourceType(str, enum.Enum):
    OPC_UA = "OPC_UA"
    ModbusEdgeDataSource = "ModbusEdgeDataSource"
    SocketEdgeDataSource = "SocketEdgeDataSource"
    RevpiEdgeDataSource = "RevpiEdgeDataSource"
    FastSocketEdgeDataSource = "FastSocketEdgeDataSource"
    MelsecEdgeDataSource = "MelsecEdgeDataSource"
    FanucEdgeDataSource = "FanucEdgeDataSource"


class DataSourceEndianType(str, enum.Enum):
    BIG = "BIG"
    LITTLE = "LITTLE"


class DataSourcePlcType(str, enum.Enum):
    Q = "Q"
    L = "L"
    QnA = "QnA"
    iQ_L = "iQ_L"
    iQ_R = "iQ_R"
    Type4E = "Type4E"


class EdgeDataSource(BaseModel):
    edgeId: str | None = None
    edgeDataSourceId: str
    type: DataSourceType
    payload: Annotated[dict, BeforeValidator(_validate_obj)] | None = None

    array2dHeader: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    binderChannel: str | None = None
    controlAttributes: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    controlListener: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    controlSets: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    directPass: bool | None = None
    endian: DataSourceEndianType | None = None
    isServer: bool | None = None
    name: str | None = None
    period: int | None = None
    portNames: list | None = None
    protocol: str | None = None
    unitNumber: int | None = None
    registerType: str | None = None
    server: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    usedByteIndexes: Annotated[list, BeforeValidator(_validate_obj)] | None = None
    wordOrder: DataSourceEndianType | None = DataSourceEndianType.BIG
    readBlock: Annotated[list, BeforeValidator(_validate_obj)] | None = None
    orderIndex: int | None = None
    charEncoding: str | None = None
    serialPort: Annotated[dict, BeforeValidator(_validate_obj)] | None = None
    plcType: DataSourcePlcType | None = None
    path: int | None = None
    opcuaUser: str | None = None
    opcuaPassword: str | None = None


def _split(formats, format, prefix=None):
    for fmt in re.findall(r"\d+|\D+", format):
        if fmt.isdigit():
            prefix = int(fmt)
            continue

        val = fmt[0]
        if prefix is not None:
            if val == "s" or val == "p":
                formats.append(f"{prefix}{val}")
            else:
                formats.extend([val] * prefix)
            prefix = None
        else:
            formats.append(val)

        if len(fmt[1:]) > 0:
            _split(formats, fmt[1:], prefix)

    return formats


def _split_payload_format(format):
    if not format:
        return []

    if format[0] in ("@", "=", "<", ">", "!"):
        format = format[1:]

    formats = []
    return _split(formats, format)


def _format_to_spec_type(format) -> EdgeDataSpecType:
    return {
        "s": EdgeDataSpecType.TEXT,
        "p": EdgeDataSpecType.TEXT,
    }.get(format[-1:], EdgeDataSpecType.ANALOG)


def _datasource_to_dataspecs(datasource: EdgeDataSource) -> list[EdgeDataSpec]:
    # ! Legacy format, fields, digitalFields
    formats = _split_payload_format(datasource.payload.get("format", []))
    fields = datasource.payload.get("fields", [])
    digital_fields = datasource.payload.get("digitalFields", [])

    data_specs = []
    index = 0
    for format, field in zip(formats, fields):
        bits = None
        if not field:
            field = f"v{index}"
            spec_type = None
            is_null = True
        else:
            for digital_field in digital_fields:
                if digital_field["field"] == field:
                    bits = digital_field["bits"]
                    break

            if bits:
                spec_type = EdgeDataSpecType.DIGITAL
            else:
                spec_type = _format_to_spec_type(format)

            is_null = False

        data_specs.append(
            EdgeDataSpec(
                edgeId=datasource.edgeId,
                edgeDataSourceId=datasource.edgeDataSourceId,
                edgeDataSpecId=f"{datasource.edgeDataSourceId}@{field}",
                type=spec_type,
                format=format,
                size=struct.calcsize(format),
                bits=bits,
                index=index,
                is_null=is_null,
            )
        )
        index += 1

    return data_specs


def _load_sources_from_file(file: str, file_type: str) -> list[EdgeDataSource]:
    rows = load_rows(file, file_type)
    if len(rows) > 1000:
        raise RuntimeError(f"Too many rows in the edge schema file: {len(rows)}")

    sources = []
    for row in rows:
        sources.append(EdgeDataSource.model_validate(row))

    return sources


def edge_load_datasources(
    file: str, file_type: str, logger: logging.Logger = logging
) -> list[tuple[str, str, list[EdgeDataSpec]]]:
    sources = _load_sources_from_file(file, file_type)

    datasources = []
    for source in sources:
        datasources.append(
            (
                source.edgeDataSourceId,
                source.payload["format"],
                _datasource_to_dataspecs(source),
            )
        )
    logging.info(datasources)

    return datasources


def _edge_encode(spec: EdgeDataSpec, row: dict) -> list[int]:
    if spec.is_null:
        if spec.format == "c":
            return [b"0"]
        return [0]

    values = []
    if spec.type == EdgeDataSpecType.DIGITAL and spec.bits:
        bits = 0
        for bit in reversed(spec.bits):
            if bit and row.get(spec.edgeDataSpecId + "@" + bit, 0) in [
                "true",
                "True",
                "1",
                1,
            ]:
                bit = 1
            else:
                bit = 0

            bits = (bits << 1) | bit

        # Convert to Signed integer value
        if spec.format == "b":
            bits = bits | (-(bits & 0x80))
        elif spec.format == "h":
            bits = bits | (-(bits & 0x8000))
        elif spec.format == "i" or spec.format == "l":
            bits = bits | (-(bits & 0x80000000))
        elif spec.format == "q":
            bits = bits | (-(bits & 0x8000000000000000))
        elif (
            spec.format != "B"
            and spec.format != "H"
            and spec.format != "I"
            and spec.format != "L"
            and spec.format != "Q"
        ):
            logging.error(
                "Unsupported format for bits, use 0 value, spec_id: %s, format: %s, bit: %s",
                spec.edgeDataSpecId,
                spec.format,
                bit,
            )
            return [0]

        values.append(bits)
    else:
        value = row.get(spec.edgeDataSpecId, None)
        if value is None:
            return [0]

        value = STRUCT_FMT.get(spec.format[-1:], float)(value)
        if (
            spec.format.endswith("c")
            or spec.format.endswith("s")
            or spec.format.endswith("p")
        ):
            try:
                value = str(value).encode("utf-8")
            except UnicodeEncodeError:
                logging.error(
                    "Encoding error, spec_id: %s, format: %s, value: %s",
                    spec.edgeDataSpecId,
                    spec.format,
                    value,
                )
                value = ""

        values.append(value)

    return values


def edge_row_encode(
    row: dict,
    datasources: list[tuple[str, str, list[EdgeDataSpec]]],
) -> bytes:
    values: dict[str, bytes] = {}

    for src_id, format, specs in datasources:
        vals = []
        for spec in sorted(specs, key=lambda x: x.index):
            vals.extend(_edge_encode(spec, row))
        values[src_id] = struct.pack(format, *vals)

    return values


def _edge_decode(
    spec: EdgeDataSpec, value: any, logger: logging.Logger = logging
) -> dict:
    if value is None or spec.is_null:
        return {}

    values = {}
    if spec.type == EdgeDataSpecType.DIGITAL and spec.bits:
        # Convert signed to unsigned integer
        if spec.format == "b":
            value = value + (1 << 8)
        elif spec.format == "h":
            value = value + (1 << 16)
        elif spec.format == "i":
            value = value + (1 << 32)
        elif spec.format == "q":
            value = value + (1 << 64)
        elif (
            spec.format != "B"
            and spec.format != "H"
            and spec.format != "I"
            and spec.format != "L"
            and spec.format != "Q"
        ):
            logger.error(
                "Unsupported format for bits, send it to triage, spec_id: %s, format: %s, value: %s",
                spec.edgeDataSpecId,
                spec.format,
                value,
            )
            return {TRIAGE_PREFIX + spec.edgeDataSpecId: value}

        for bit in spec.bits:
            if bit:
                values[spec.edgeDataSpecId + "@" + bit] = bool(value & 0b1)
            value = value >> 1
    else:
        if (
            spec.format.endswith("c")
            or spec.format.endswith("s")
            or spec.format.endswith("p")
        ):
            try:
                values[spec.edgeDataSpecId] = STRUCT_FMT.get(spec.format[-1:], float)(
                    value.decode("utf-8")
                )
            except UnicodeDecodeError:
                logger.error(
                    "Decoding error, send it to triage, spec_id: %s, format: %s, value: %s",
                    spec.edgeDataSpecId,
                    spec.format,
                    value,
                )
                return {TRIAGE_PREFIX + spec.edgeDataSpecId: value}
        else:
            values[spec.edgeDataSpecId] = STRUCT_FMT.get(spec.format[-1:], float)(value)

    return values


def edge_row_decode(
    row: dict,
    datasources: list[tuple[str, str, list[EdgeDataSpec]]],
    logger: logging.Logger = logging,
) -> dict:
    values = {}
    for src_id, packed in row.items():
        if packed is None:
            continue

        _src_id, format, specs = next(
            (d for d in datasources if d[0] == src_id), (None, None, None)
        )
        if _src_id is None:
            values[src_id] = packed
            continue

        unpacked = struct.unpack(format, packed)
        logger.debug("unpacked: %s", unpacked)

        for spec in sorted(specs, key=lambda x: x.index):
            values.update(_edge_decode(spec, unpacked[spec.index], logger))

    return values


# * Field
FIELD_TYPES = [
    "string",
    "long",
    "integer",
    "short",
    "byte",
    "float",
    "double",
    "boolean",
    "binary",
    "date",
    "timestamp",
    "timestamp_ntz",
    # backward compatibility
    "tinyint",
    "smallint",
    "int",
    "bigint",
]


def _validate_field_type(val: str) -> str:
    val = val.lower()
    if val in FIELD_TYPES or (val.startswith("decimal(") and val.endswith(")")):
        return val
    raise ValueError(f"Field type must be one of {FIELD_TYPES} or decimal(u8, u8): not {val}")


class Field(BaseModel):
    name: str
    type: Annotated[str, BeforeValidator(_validate_field_type)]
    subtype: str | None = None
    nullable: bool = True
    comment: str | None = None
    alias: str | None = None
    units: str | None = None


def nz_predict_field(key: str, val: any) -> Field:
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


def nz_load_fields(file: str, file_type: str) -> list[Field]:
    rows = load_rows(file, file_type)
    if len(rows) > 1000:
        raise RuntimeError(f"Too many rows in the schema file: {len(rows)}")

    fields = TypeAdapter(list[Field]).validate_python(rows)
    if "timestamp" not in [field.name for field in fields] or "date" not in [
        field.name for field in fields
    ]:
        raise RuntimeError("Schema file must have 'timestamp' and 'date' fields")

    return fields


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
        f"Failed to get pipeline: {response.status_code}, {response.reason}"
    )


def nz_pipeline_create(
    store_api_url: str,
    store_api_username: str,
    store_api_password: str,
    pipeline_name: str,
    schema_file_type,
    schema_file,
    ingest_type: str = Literal["KAFKA", "MQTT", "EDGE"],
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

    data = {
        "name": pipeline_name,
        "alias": pipeline_name,
        "ingest_type": ingest_type,
        "deltasync_enabled": enable_deltasync,
        "table_create": {
            "name": pipeline_name,
            "alias": pipeline_name,
            "partitions": ["date"],
            "delete_retention": delete_retention,
        },
    }

    if ingest_type == "EDGE":
        data["edge_create"] = {
            "edgeId": pipeline_name,
            "type": "EXTERNAL",
        }

        datasources: list[EdgeDataSource] = []
        for src in _load_sources_from_file(schema_file, schema_file_type):
            src.edgeId = None  # Remove for creating
            datasources.append(src.model_dump(exclude_none=True))

        data["edge_create"]["datasources_create"] = datasources
    elif ingest_type == "KAFKA" or ingest_type == "MQTT":
        fields = []
        for field in nz_load_fields(schema_file, schema_file_type):
            fields.append(field.model_dump(exclude_none=True))

        data["table_create"]["fields_create"] = fields
    else:
        raise RuntimeError(f"Invalid ingest type: {ingest_type}")

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


def nz_pipeline_delete(
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


class NzRowTransformer:
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
                if interval_diff >= self.interval_field_prev:
                    interval = (
                        interval_diff - self.interval_field_prev
                    ).total_seconds()
            self.interval_field_prev = interval_diff

        if self.incremental_field:
            row[self.incremental_field] = self.incremental_idx
            self.incremental_idx += self.incremental_field_step

        if self.datetime_field and self.datetime_field_format:
            row[self.datetime_field] = epoch.strftime(self.datetime_field_format)

        if self.eval_field and self.eval_func:
            row[self.eval_field] = self.eval_func(**row)

        return row, interval
