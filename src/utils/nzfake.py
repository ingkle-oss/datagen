import random
import string
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import Boolean, DateTime, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class NZFaker:
    fake = None
    fields = []
    str_choice: list[str]

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
            [f"int_{i}" for i in range(int_count)]
            + [f"float_{i}" for i in range(float_count)]
            + [f"word_{i}" for i in range(word_count)]
            + [f"text_{i}" for i in range(text_count)]
            + [f"name_{i}" for i in range(name_count)]
            + [f"str_{i}" for i in range(str_count)]
        )

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

    def values(self):
        values = (
            [random.randint(-(2**31), (2**31) - 1) for _ in range(self.int_count)]
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
    def __repr__(self) -> str:
        return str(self.__dict__)


def field_model(tablename):

    class Field(Base):

        __tablename__ = tablename

        type: Mapped[str] = mapped_column(String, nullable=True)
        subtype: Mapped[str] = mapped_column(String, nullable=True)
        name: Mapped[str] = mapped_column(String, primary_key=True)
        table_name: Mapped[str] = mapped_column(String, primary_key=True)
        nullable: Mapped[str] = mapped_column(Boolean, nullable=True)
        comment: Mapped[str] = mapped_column(String, nullable=True)
        data_source: Mapped[str] = mapped_column(String, nullable=True)
        data_order: Mapped[str] = mapped_column(String, nullable=True)
        created_at: Mapped[str] = mapped_column(
            DateTime, server_default=func.now(), nullable=True
        )
        updated_at: Mapped[str] = mapped_column(
            DateTime, server_default=func.now(), nullable=True
        )

        def __init__(
            self,
            type,
            subtype,
            name,
            table,
            nullable,
            comment,
            data_source,
            data_order,
            created_at,
            updated_at,
        ):
            self.type = type
            self.subtype = subtype
            self.name = name
            self.table_name = table
            self.nullable = nullable
            self.comment = comment
            self.data_source = data_source
            self.data_order = data_order
            self.created_at = created_at
            self.updated_at = updated_at

    return Field


class NZFakerStore:
    engine = None
    Field = None
    fake = None
    fields = []
    str_choice: list[str]
    str_length: int
    str_cardinality: int

    def update_fields(self, table_name):
        with Session(self.engine, expire_on_commit=False) as session:
            self.fields = sorted(
                [
                    {"name": s.name, "type": s.type, "subtype": s.subtype}
                    for s in session.scalars(
                        select(self.Field).where(self.Field.table_name == table_name)
                    ).all()
                ],
                key=lambda x: x["name"],
            )

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
        self.update_fields(table_name)

        self.str_length = str_length if str_length and str_length > 0 else 0
        self.str_cardinality = (
            str_cardinality if str_cardinality and str_cardinality > 0 else 0
        )

        if self.str_cardinality > 0:
            self.str_choice = [
                self.fake.unique.pystr(max_chars=str_length)
                for _ in range(str_cardinality)
            ]

    def values(self):
        values = {}
        for field in self.fields:
            if field["name"].startswith("__"):
                continue

            if field["type"] == "integer":
                values[field["name"]] = random.randint(-(2**31), (2**31) - 1)
            elif field["type"] == "long":
                values[field["name"]] = random.randint(-(2**63), (2**63) - 1)
            elif field["type"] == "float":
                values[field["name"]] = random.uniform(-(2**31), (2**31) - 1)
            elif field["type"] == "double":
                values[field["name"]] = random.uniform(-(2**63), (2**63) - 1)
            elif field["type"] == "timestamp":
                if field["name"] != "timestamp":  # 'timestamp' is a reserved field
                    values[field["name"]] = int(
                        (
                            datetime.now() + timedelta(hours=random.randint(-720, 0))
                        ).timestamp()
                        * 1e6
                    )
            elif field["type"] == "date":
                if field["name"] != "date":  # 'date' is a reserved/hidden field
                    values[field["name"]] = datetime.now().date()
            elif field["type"] == "string":
                if self.str_cardinality > 0:
                    values[field["name"]] = random.choice(self.str_choice)
                else:
                    values[field["name"]] = "".join(
                        random.choice(string.ascii_letters + string.digits)
                        for _ in range(self.str_length)
                    )
            else:
                raise ValueError(f"Unknown field type: {field['type']}")

        return values
