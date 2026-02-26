import logging
import random
import string
import struct
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from faker import Faker
from sqlalchemy import Boolean, DateTime, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from utils.nazare import EdgeDataSpec, EdgeDataSpecType, Field


def _gen_struct_to_value(format: str, size: int):
    value = None
    if format == "c":
        value = random.choice(string.ascii_letters + string.digits).encode("utf-8")
    elif format == "b":
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


def _gen_datatype_values(
    type: str, str_cardinality: int, str_choice: list[str], str_length: int
):
    value = None
    if type == "tinyint" or type == "byte":
        value = random.randint(-(2**7), (2**7) - 1)
    elif type == "boolean":
        value = random.choice([True, False])
    elif type == "smallint" or type == "short":
        value = random.randint(-(2**15), (2**15) - 1)
    elif type == "int" or type == "integer":
        value = random.randint(-(2**31), (2**31) - 1)
    elif type == "bigint" or type == "long":
        value = random.randint(-(2**63), (2**63) - 1)
    elif type == "float":
        value = random.uniform(-(2**31), (2**31) - 1)
    elif type == "double" or type.startswith("decimal("):
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


def _gen_spec_values(spec: EdgeDataSpec) -> list[int]:
    if spec.is_null:
        return [0]

    values = []
    if spec.type == EdgeDataSpecType.DIGITAL and spec.bits:
        bits = 0
        for bit in reversed(spec.bits):
            if bit:
                bit = random.getrandbits(1)
            else:
                bit = 0

            bits = (bits << 1) | bit

        # convert unsigned to signed
        if spec.format == "b":
            bits = 0b11111111
            bits = bits | (-(bits & 0x80))
        elif spec.format == "h":
            bits = 0b1111111111111111
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
        values.append(_gen_struct_to_value(spec.format, spec.size))

    return values


class NZFakerDB(ABC):
    @abstractmethod
    def get_schema(self) -> list[dict[str, any]]:
        pass

    @abstractmethod
    def update_schema(self):
        pass

    @abstractmethod
    def values(self) -> dict[str, any]:
        pass


class NZFaker(NZFakerDB):
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
    class TableField(Base):
        __tablename__ = tablename

        name: Mapped[str] = mapped_column(String, primary_key=True)
        table_name: Mapped[str] = mapped_column(String, primary_key=True)
        alias: Mapped[str] = mapped_column(String, nullable=True)

        type: Mapped[str] = mapped_column(String, nullable=False)
        subtype: Mapped[str] = mapped_column(String, nullable=True)
        nullable: Mapped[str] = mapped_column(Boolean, nullable=False)
        comment: Mapped[str] = mapped_column(String, nullable=True)
        units: Mapped[str] = mapped_column(String, nullable=True)

        def __init__(
            self,
            name,
            table_name,
            alias,
            type,
            subtype,
            nullable,
            comment,
            units,
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
            self.units = units
            self.created_at = created_at
            self.updated_at = updated_at

    return TableField


class NZFakerStore(NZFakerDB):
    engine = None
    TableField = None
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
        self.TableField = field_model(table)
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
                        select(self.TableField).where(
                            self.TableField.table_name == self.table_name
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

            values[field["name"]] = _gen_datatype_values(
                field["type"],
                self.str_cardinality,
                self.str_choice,
                self.str_length,
            )

        return values


class NZFakerEdgeSource(NZFakerDB):
    engine = None
    fake = None
    dataspecs: list[EdgeDataSpec] = []
    str_choice: list[str]
    str_length: int
    str_cardinality: int

    def __init__(
        self,
        host,
        port,
        username,
        password,
        db_name,
        edge_id,
        loglevel="INFO",
    ):
        self.fake = Faker(use_weighting=False)

        self.engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{db_name}",
            echo=True if loglevel == "DEBUG" else False,
        )
        self.edge_id = edge_id
        self.update_schema()

    def get_schema(self) -> dict[str, list[EdgeDataSpec]]:
        return self.dataspecs

    def update_schema(self):
        with Session(self.engine, expire_on_commit=False) as session:
            logging.info("Updating schema...")
            dataspecs = sorted(
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
                        select(EdgeDataSpec).where(EdgeDataSpec.edgeId == self.edge_id)
                    ).all()
                ],
                key=lambda x: (x["edgeDataSourceId"], x["index"]),
            )
            dataspecs = EdgeDataSpec.model_validate(dataspecs)
            self.dataspecs = {}
            for spec in dataspecs:
                if spec.edgeDataSourceId not in self.dataspecs:
                    self.dataspecs[spec.edgeDataSourceId] = []
                self.dataspecs[spec.edgeDataSourceId].append(spec)
            logging.info("Schema length: %d", len(self.dataspecs))
            logging.info("Schema: %s", self.dataspecs)

    def values(self):
        values: dict[str, bytes] = {}

        for src, specs in self.dataspecs.items():
            format = ""
            _vals = []
            for spec in sorted(specs, key=lambda x: x.index):
                format += spec.format
                _vals.extend(_gen_spec_values(spec))
            values[src] = struct.pack(format, *_vals)

        return values


class NaFaker(ABC):
    @abstractmethod
    def values(self) -> dict[str, any]:
        pass

    @abstractmethod
    def get_schema(self) -> list[any]:
        pass


class NZFakerField:
    fake: Faker
    fields: list[str]

    integers: list[str]
    longs: list[str]
    strings: list[str]
    floats: list[str]
    doubles: list[str]
    booleans: list[str]
    binaries: list[str]
    dates: list[str]
    timestamps: list[str]
    timestamp_tzinfo: timezone
    timestamp_ntz_s: list[str]

    string_choice: list[str]
    string_length: int
    string_cardinality: int
    binary_length: int

    def __init__(
        self,
        fields: list[Field],
        str_length: int,
        str_cardinality: int,
        binary_length: int,
        timestamp_tzinfo=timezone.utc,
    ):
        self.fake = Faker(use_weighting=False)
        self.fields = []

        self.integers = []
        self.longs = []
        self.strings = []
        self.floats = []
        self.doubles = []
        self.booleans = []
        self.binaries = []
        self.dates = []
        self.timestamps = []
        self.timestamp_ntz_s = []

        self.string_length = 0 if str_length < 0 else str_length
        self.string_cardinality = 0 if str_cardinality < 0 else str_cardinality
        self.string_choice = []
        self.binary_length = 0 if binary_length < 0 else binary_length
        self.timestamp_tzinfo = timestamp_tzinfo

        if self.string_cardinality > 0:
            self.string_choice = [
                self.fake.unique.pystr(max_chars=str_length)
                for _ in range(str_cardinality)
            ]

        maps = {
            "integer": self.integers,
            "int": self.integers,
            "short": self.integers,
            "smallint": self.integers,
            "byte": self.integers,
            "tinyint": self.integers,
            "long": self.longs,
            "bigint": self.longs,
            "string": self.strings,
            "float": self.floats,
            "double": self.doubles,
            "boolean": self.booleans,
            "binary": self.binaries,
            "date": self.dates,
            "timestamp": self.timestamps,
            "timestamp_ntz": self.timestamp_ntz_s,
        }
        for field in fields:
            target = maps.get(field.type)
            if target is None and field.type.startswith("decimal("):
                target = self.doubles
            if target is not None:
                target.append(field.name)
            else:
                raise ValueError(f"Unknown field type: {field.type}")

    def get_schema(self) -> list[Field]:
        return self.fields

    def values(self) -> dict[str, any]:
        values = {}
        values |= dict(
            zip(
                self.integers + self.longs,
                [
                    random.randint(-(2**31), (2**31) - 1)
                    for _ in range(len(self.integers) + len(self.longs))
                ],
            )
        )
        if self.string_choice:
            values |= dict(
                zip(
                    self.strings,
                    [
                        random.choice(self.string_choice)
                        for _ in range(len(self.strings))
                    ],
                )
            )
        else:
            values |= dict(zip(self.strings, self.fake.words(len(self.strings))))
        values |= dict(
            zip(
                self.floats + self.doubles,
                [
                    random.uniform(-(2**31), (2**31) - 1)
                    for _ in range(len(self.floats) + len(self.doubles))
                ],
            )
        )

        values |= dict(
            zip(
                self.booleans,
                [random.choice([True, False]) for _ in range(len(self.booleans))],
            )
        )
        values |= dict(
            zip(
                self.binaries,
                [
                    self.fake.binary(length=self.binary_length)
                    for _ in range(len(self.binaries))
                ],
            )
        )
        values |= dict(
            zip(
                self.dates,
                [self.fake.date_object() for _ in range(len(self.dates))],
            )
        )
        values |= dict(
            zip(
                self.timestamps,
                [
                    self.fake.date_time(self.timestamp_tzinfo)
                    for _ in range(len(self.timestamps))
                ],
            )
        )
        values |= dict(
            zip(
                self.timestamp_ntz_s,
                [self.fake.date_time() for _ in range(len(self.timestamp_ntz_s))],
            )
        )

        return values


class NZFakerEdge:
    datasources: list[tuple[str, str, list[EdgeDataSpec]]] = []

    def __init__(self, datasources: list[tuple[str, str, list[EdgeDataSpec]]]):
        self.fake = Faker(use_weighting=False)
        self.datasources = datasources

    def get_schema(self) -> list[tuple[str, str, list[EdgeDataSpec]]]:
        return self.datasources

    def values(self) -> dict[str, any]:
        values: dict[str, bytes] = {}

        for src_id, format, specs in self.datasources:
            vals = []
            for spec in sorted(specs, key=lambda x: x.index):
                vals.extend(_gen_spec_values(spec))
            values[src_id] = struct.pack(format, *vals)

        return values
