import enum
import logging
import random
import string
import struct
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import (
    ARRAY,
    Boolean,
    DateTime,
    Enum,
    String,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def _struct_to_value(format: str, size: int):
    value = None
    if format == "c" or format == "b":
        value = random.randint(-(2**7), (2**7) - 1)
    elif format == "B":
        value = random.randint(0, (2**8) - 1)
    elif format == "?":
        value = random.getrandbits(1)
    elif format == "h":
        value = random.randint(-(2**15), (2**15) - 1)
    elif format == "H":
        value = random.randint(0, (2**16) - 1)
    elif format == "i" or format == "l":
        value = random.randint(-(2**31), (2**31) - 1)
    elif format == "I" or format == "L":
        value = random.randint(0, (2**32) - 1)
    elif format == "q":
        value = random.randint(-(2**63), (2**63) - 1)
    elif format == "Q":
        value = random.randint(0, (2**64) - 1)
    elif format == "f":
        value = random.uniform(-(2**31), (2**31) - 1)
    elif format == "d":
        value = random.uniform(-(2**63), (2**63) - 1)
    elif format.endswith("s") or format.endswith("p"):
        str_length = size
        value = "".join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(str_length)
        ).encode("utf-8")

    else:
        raise ValueError(f"Unknown field type: {format}")

    return value


def _datatype_to_value(
    type: str, str_cardinality: int, str_choice: list[str], str_length: int
):
    value = None
    if type == "tinyint":
        value = random.randint(-(2**7), (2**7) - 1)
    elif type == "boolean":
        value = random.choice([True, False])
    elif type == "smallint":
        value = random.randint(-(2**15), (2**15) - 1)
    elif type == "int" or type == "integer":
        value = random.randint(-(2**31), (2**31) - 1)
    elif type == "bigint" or type == "long":
        value = random.randint(-(2**63), (2**63) - 1)
    elif type == "float":
        value = random.uniform(-(2**31), (2**31) - 1)
    elif type == "double":
        value = random.uniform(-(2**63), (2**63) - 1)
    elif type == "timestamp":
        value = int(
            (datetime.now() + timedelta(hours=random.randint(-720, 0))).timestamp()
            * 1e6
        )
    elif type == "date":
        value = datetime.now().date()
    elif type == "string":
        if str_cardinality > 0:
            value = random.choice(str_choice)
        else:
            value = "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(str_length)
            )
    else:
        raise ValueError(f"Unknown field type: {type}")

    return value


class NZFakerBase(ABC):
    @abstractmethod
    def get_schema(self) -> list[dict[str, any]]:
        pass

    @abstractmethod
    def update_schema(self):
        pass

    @abstractmethod
    def values(self) -> dict[str, any]:
        pass


class NZFaker(NZFakerBase):
    fake = None
    fields = []
    str_choice: list[str]

    bool_count: int
    int_count: int
    float_count: int
    word_count: int
    text_count: int
    name_count: int
    str_count: int
    str_length: int
    str_cardinality: int

    def __init__(
        self,
        bool_count=0,
        int_count=0,
        float_count=0,
        word_count=0,
        text_count=0,
        name_count=0,
        str_count=0,
        str_length=10,
        str_cardinality=0,
    ):
        self.fake = Faker(use_weighting=False)

        self.fields = (
            [f"bool_{i}" for i in range(bool_count)]
            + [f"int_{i}" for i in range(int_count)]
            + [f"float_{i}" for i in range(float_count)]
            + [f"word_{i}" for i in range(word_count)]
            + [f"text_{i}" for i in range(text_count)]
            + [f"name_{i}" for i in range(name_count)]
            + [f"str_{i}" for i in range(str_count)]
        )

        self.bool_count = bool_count if bool_count and bool_count > 0 else 0
        self.int_count = int_count if int_count and int_count > 0 else 0
        self.float_count = float_count if float_count and float_count > 0 else 0
        self.word_count = word_count if word_count and word_count > 0 else 0
        self.text_count = text_count if text_count and text_count > 0 else 0
        self.name_count = name_count if name_count and name_count > 0 else 0
        self.str_count = str_count if str_count and str_count > 0 else 0
        self.str_length = str_length if str_length and str_length > 0 else 0
        self.str_cardinality = (
            str_cardinality if str_cardinality and str_cardinality > 0 else 0
        )

        if self.str_cardinality > 0:
            self.str_choice = [
                self.fake.unique.pystr(max_chars=str_length)
                for _ in range(str_cardinality)
            ]

    def get_schema(self) -> list[dict[str, any]]:
        return self.fields

    def update_schema(self):
        return super().update_schema()

    def values(self):
        values = (
            [random.choice([True, False]) for _ in range(self.bool_count)]
            + [random.randint(-(2**31), (2**31) - 1) for _ in range(self.int_count)]
            + [random.uniform(-(2**31), (2**31) - 1) for _ in range(self.float_count)]
            + self.fake.words(self.word_count)
            + self.fake.texts(self.text_count)
            + [self.fake.name() for _ in range(self.name_count)]
        )
        if self.str_cardinality > 0:
            values += [random.choice(self.str_choice) for _ in range(self.str_count)]
        else:
            values += [
                "".join(
                    random.choice(string.ascii_letters + string.digits)
                    for _ in range(self.str_length)
                )
                for _ in range(self.str_count)
            ]
        return dict(zip(self.fields, values))


class Base(DeclarativeBase):
    created_at: Mapped[str] = mapped_column(
        DateTime, server_default=func.now(), nullable=True
    )
    updated_at: Mapped[str] = mapped_column(
        DateTime, server_default=func.now(), nullable=True
    )

    def __repr__(self) -> str:
        return str(self.__dict__)


def field_model(tablename):
    class Field(Base):
        __tablename__ = tablename

        name: Mapped[str] = mapped_column(String, primary_key=True)
        table_name: Mapped[str] = mapped_column(String, primary_key=True)
        alias: Mapped[str] = mapped_column(String, nullable=True)

        type: Mapped[str] = mapped_column(String, nullable=False)
        subtype: Mapped[str] = mapped_column(String, nullable=True)
        nullable: Mapped[str] = mapped_column(Boolean, nullable=False)
        comment: Mapped[str] = mapped_column(String, nullable=True)

        def __init__(
            self,
            name,
            table_name,
            alias,
            type,
            subtype,
            nullable,
            comment,
            created_at,
            updated_at,
        ):
            self.name = name
            self.table_name = table_name
            self.alias = alias
            self.type = type
            self.subtype = subtype
            self.nullable = nullable
            self.comment = comment
            self.created_at = created_at
            self.updated_at = updated_at

    return Field


class NZFakerStore(NZFakerBase):
    engine = None
    Field = None
    fake = None
    fields = []
    str_choice: list[str]
    str_length: int
    str_cardinality: int

    def __init__(
        self,
        host,
        port,
        username,
        password,
        database,
        table,
        table_name,
        loglevel="INFO",
        str_length=10,
        str_cardinality=0,
    ):
        self.fake = Faker(use_weighting=False)

        self.engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}",
            echo=True if loglevel == "DEBUG" else False,
        )
        self.Field = field_model(table)
        self.table_name = table_name
        self.update_schema()

        self.str_length = str_length if str_length and str_length > 0 else 0
        self.str_cardinality = (
            str_cardinality if str_cardinality and str_cardinality > 0 else 0
        )

        self.str_choice = []
        if self.str_cardinality > 0:
            self.str_choice = [
                self.fake.unique.pystr(max_chars=str_length)
                for _ in range(str_cardinality)
            ]

    def get_schema(self) -> list[dict[str, any]]:
        return self.fields

    def update_schema(self):
        with Session(self.engine, expire_on_commit=False) as session:
            logging.info("Updating schema...")
            self.fields = sorted(
                [
                    {"name": s.name, "type": s.type, "subtype": s.subtype}
                    for s in session.scalars(
                        select(self.Field).where(
                            self.Field.table_name == self.table_name
                        )
                    ).all()
                ],
                key=lambda x: x["name"],
            )
            logging.info("Schema length: %d", len(self.fields))
            logging.info("Schema: %s", self.fields)

    def values(self) -> dict[str, any]:
        values = {}
        for field in self.fields:
            if (
                field["name"].startswith("__")
                or field["name"] == "timestamp"
                or field["name"] == "date"
            ):
                # 'timestamp' and 'date' is a reserved field for deltasync
                continue

            values[field["name"]] = _datatype_to_value(
                field["type"],
                self.str_cardinality,
                self.str_choice,
                self.str_length,
            )

        return values


def edgedataspec_model(tablename):
    class EdgeDataSpecType(str, enum.Enum):
        ANALOG = "ANALOG"
        DIGITAL = "DIGITAL"
        TEXT = "TEXT"

    class EdgeDataSpec(Base):
        __tablename__ = tablename

        edgeId: Mapped[str] = mapped_column(String, primary_key=True)
        edgeDataSourceId: Mapped[str] = mapped_column(String, primary_key=True)
        edgeDataSpecId: Mapped[str] = mapped_column(String, primary_key=True)

        type: Mapped[EdgeDataSpecType] = mapped_column(
            Enum(EdgeDataSpecType), nullable=False
        )
        format: Mapped[str] = mapped_column(String, nullable=False)
        size: Mapped[int] = mapped_column(String, nullable=True)
        index: Mapped[int] = mapped_column(String, nullable=False)
        is_null: Mapped[bool] = mapped_column(Boolean, default=False)
        bits: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=True)

        def __init__(
            self,
            edgeId,
            edgeDataSourceId,
            edgeDataSpecId,
            type,
            format,
            size,
            index,
            is_null,
            bits,
            created_at,
            updated_at,
        ):
            self.edgeId = edgeId
            self.edgeDataSourceId = edgeDataSourceId
            self.edgeDataSpecId = edgeDataSpecId
            self.type = type
            self.format = format
            self.size = size
            self.index = index
            self.is_null = is_null
            self.bits = bits
            self.created_at = created_at
            self.updated_at = updated_at

    return EdgeDataSpec


class NZFakerEdge(NZFakerBase):
    engine = None
    EdgeDataSpec = None
    fake = None
    dataspecs = []
    str_choice: list[str]
    str_length: int
    str_cardinality: int

    def __init__(
        self,
        host,
        port,
        username,
        password,
        database,
        table,
        edge_id,
        loglevel="INFO",
    ):
        self.fake = Faker(use_weighting=False)

        self.engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}",
            echo=True if loglevel == "DEBUG" else False,
        )
        self.EdgeDataSpec = edgedataspec_model(table)
        self.edge_id = edge_id
        self.update_schema()

    def get_schema(self) -> list[dict[str, any]]:
        return self.dataspecs

    def update_schema(self):
        with Session(self.engine, expire_on_commit=False) as session:
            logging.info("Updating schema...")
            self.dataspecs = sorted(
                [
                    {
                        "edgeDataSourceId": s.edgeDataSourceId,
                        "edgeDataSpecId": s.edgeDataSpecId,
                        "type": s.type,
                        "format": s.format,
                        "size": s.size,
                        "index": s.index,
                        "is_null": s.is_null,
                        "bits": s.bits,
                    }
                    for s in session.scalars(
                        select(self.EdgeDataSpec).where(
                            self.EdgeDataSpec.edgeId == self.edge_id
                        )
                    ).all()
                ],
                key=lambda x: (x["edgeDataSourceId"], x["index"]),
            )
            logging.info("Schema length: %d", len(self.dataspecs))
            logging.info("Schema: %s", self.dataspecs)

    @staticmethod
    def _dataspec_to_values(dataspec: dict) -> list[int]:
        if dataspec["is_null"]:
            return [0]

        values = []
        if dataspec["bits"]:
            bits = 0
            for bit in reversed(dataspec["bits"]):
                if bit:
                    bit = random.getrandbits(1)
                else:
                    bit = 0

                bits = (bits << 1) | bit

            # Convert to Signed integer value
            if dataspec["format"] == "c":
                bits = bits | (-(bits & 0x80))
            elif dataspec["format"] == "h":
                bits = bits | (-(bits & 0x8000))
            elif dataspec["format"] == "i":
                bits = bits | (-(bits & 0x80000000))
            elif dataspec["format"] == "q":
                bits = bits | (-(bits & 0x8000000000000000))
            else:
                logging.error("Unsupported format: %s for bits", dataspec["format"])

            values.append(bits)
        else:
            values.append(_struct_to_value(dataspec["format"], dataspec["size"]))

        return values

    def values(self):
        values = {}
        datasources = {}
        for spec in self.dataspecs:
            if spec["edgeDataSourceId"] not in datasources:
                datasources[spec["edgeDataSourceId"]] = []

            datasources[spec["edgeDataSourceId"]].append(spec)

        for source in datasources:
            _formats = ""
            _values = []
            for spec in datasources[source]:
                _formats += spec["format"]
                _values.extend(NZFakerEdge._dataspec_to_values(spec))

            values[source] = struct.pack(_formats, *_values)

        return values
