import argparse
import math
import logging
import random
import string
from datetime import timezone
from zoneinfo import ZoneInfo

from faker import Faker
from pydantic import BaseModel, field_validator, model_validator

from utils.nazare import FIELD_TYPES, Field
from utils.nzfake import NaFaker

# --- System defaults (lowest priority in 3-level merge) ---
SYSTEM_DEFAULTS: dict[str, dict] = {
    "integer": {"min": -(2**31), "max": (2**31) - 1},
    "int": {"min": -(2**31), "max": (2**31) - 1},
    "short": {"min": -(2**15), "max": (2**15) - 1},
    "smallint": {"min": -(2**15), "max": (2**15) - 1},
    "byte": {"min": -(2**7), "max": (2**7) - 1},
    "tinyint": {"min": -(2**7), "max": (2**7) - 1},
    "long": {"min": -(2**63), "max": (2**63) - 1},
    "bigint": {"min": -(2**63), "max": (2**63) - 1},
    "float": {"min": -(2**31), "max": (2**31) - 1},
    "double": {"min": -(2**63), "max": (2**63) - 1},
    "string": {"length": 10, "cardinality": 0},
    "boolean": {},
    "binary": {"length": 10},
    "date": {},
    "timestamp": {"tzinfo": "UTC"},
    "timestamp_ntz": {},
}

# Types that are integer-family (for dispatch in value generation)
_INT_TYPES = frozenset(
    ["integer", "int", "short", "smallint", "byte", "tinyint", "long", "bigint"]
)
_FLOAT_TYPES = frozenset(["float", "double"])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class ColumnGenerator(BaseModel):
    """Time-aware generator configuration for a column."""

    kind: str
    value: object | None = None
    values: list[object] | None = None
    amplitude: float | None = None
    period_sec: float | None = None
    offset: float | None = None
    phase_sec: float | None = None
    noise: float | None = None
    start: float | int | None = None
    slope_per_sec: float | None = None
    wrap: bool = False
    dwell_sec: float | None = None
    interval_sec: float | None = None
    step_stddev: float | None = None
    min: int | float | None = None
    max: int | float | None = None

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        kind = value.lower()
        supported = {
            "constant",
            "sine",
            "ramp",
            "step",
            "random_walk",
            "choice_cycle",
        }
        if kind not in supported:
            raise ValueError(f"Unsupported generator kind: {value}")
        return kind

    @model_validator(mode="after")
    def validate_shape(self) -> "ColumnGenerator":
        if self.kind == "constant" and self.value is None:
            raise ValueError("constant generator requires value")
        if self.kind == "sine":
            if self.amplitude is None or self.period_sec in (None, 0):
                raise ValueError("sine generator requires amplitude and period_sec")
        if self.kind == "ramp":
            if self.start is None or self.slope_per_sec is None:
                raise ValueError("ramp generator requires start and slope_per_sec")
        if self.kind == "step":
            if not self.values or self.dwell_sec in (None, 0):
                raise ValueError("step generator requires values and dwell_sec")
        if self.kind == "random_walk":
            if self.start is None or self.step_stddev is None:
                raise ValueError("random_walk generator requires start and step_stddev")
        if self.kind == "choice_cycle":
            if not self.values or self.interval_sec in (None, 0):
                raise ValueError("choice_cycle generator requires values and interval_sec")
        return self


class ColumnDef(BaseModel):
    """Per-column fake data generation configuration."""

    name: str
    type: str
    min: int | float | None = None
    max: int | float | None = None
    precision: int | None = None
    values: list | None = None
    null_ratio: float = 0.0
    length: int | None = None
    cardinality: int | None = None
    pattern: str | None = None
    tzinfo: str | None = None
    generator: ColumnGenerator | None = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        v = v.lower()
        if v in FIELD_TYPES or (v.startswith("decimal(") and v.endswith(")")):
            return v
        raise ValueError(f"Invalid column type: {v}")

    @field_validator("null_ratio")
    @classmethod
    def validate_null_ratio(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("null_ratio must be between 0.0 and 1.0")
        return v

    @model_validator(mode="after")
    def validate_min_max(self) -> "ColumnDef":
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"min ({self.min}) must be <= max ({self.max})")
        return self


class FakeConfig(BaseModel):
    """Top-level config loaded from --fake-config JSON file."""

    defaults: dict = {}
    columns: list[ColumnDef] = []
    total_columns: int | None = None
    type_ratio: dict[str, float] = {}

    @model_validator(mode="after")
    def validate_config(self) -> "FakeConfig":
        if self.type_ratio and not self.total_columns:
            raise ValueError("total_columns is required when type_ratio is specified")
        if not self.columns and not self.type_ratio:
            raise ValueError("At least one of 'columns' or 'type_ratio' is required")
        return self


# ---------------------------------------------------------------------------
# Config loading and 3-level merge
# ---------------------------------------------------------------------------


def load_fake_config(filepath: str) -> FakeConfig:
    """Load and validate a FakeConfig from a JSON file."""
    import orjson

    with open(filepath, "rb") as f:
        obj = orjson.loads(f.read())
    return load_fake_config_obj(obj)


def load_fake_config_obj(obj: dict) -> FakeConfig:
    """Load and validate a FakeConfig from a Python object."""
    return FakeConfig.model_validate(obj)


def _resolve_column(col: ColumnDef, defaults: dict) -> ColumnDef:
    """
    3-level merge for a single column.
    Priority: column explicit > defaults[type] > defaults global > SYSTEM_DEFAULTS
    """
    sys_defaults = SYSTEM_DEFAULTS.get(col.type, {})

    type_defaults = defaults.get(col.type, {})
    if isinstance(type_defaults, dict):
        type_defaults = dict(type_defaults)
    else:
        type_defaults = {}

    # Global defaults: keys that are not type names
    global_defaults = {
        k: v
        for k, v in defaults.items()
        if k not in FIELD_TYPES and not k.startswith("decimal(")
    }

    # Merge chain: system < global < type-specific < column explicit
    merged = {}
    merged.update(sys_defaults)
    merged.update(global_defaults)
    merged.update(type_defaults)

    col_explicit = col.model_dump(exclude_none=True, exclude={"name", "type"})
    merged.update(col_explicit)

    # Filter to only ColumnDef fields
    valid_fields = set(ColumnDef.model_fields.keys()) - {"name", "type"}
    filtered = {k: v for k, v in merged.items() if k in valid_fields}

    payload = {"name": col.name, "type": col.type, **filtered}
    return ColumnDef.model_validate(payload)


def _expand_type_ratio(
    type_ratio: dict[str, float], total_columns: int, existing_count: int
) -> list[ColumnDef]:
    """
    Expand type_ratio into auto-generated ColumnDef list.
    Uses largest-remainder method for fair rounding.
    """
    auto_count = total_columns - existing_count
    if auto_count <= 0:
        return []

    # Normalize ratios
    ratio_sum = sum(type_ratio.values())
    if ratio_sum <= 0:
        return []

    if abs(ratio_sum - 100.0) > 0.01:
        logging.warning(
            "type_ratio sum is %.1f (expected ~100), normalizing", ratio_sum
        )

    # Calculate raw counts and remainders
    types = list(type_ratio.keys())
    raw = [(t, auto_count * type_ratio[t] / ratio_sum) for t in types]
    floors = [(t, int(v), v - int(v)) for t, v in raw]

    allocated = sum(f for _, f, _ in floors)
    remaining = auto_count - allocated

    # Largest remainder: give extra slots to types with biggest fractional parts
    sorted_by_remainder = sorted(floors, key=lambda x: -x[2])
    counts = {}
    for i, (t, floor, _) in enumerate(sorted_by_remainder):
        counts[t] = floor + (1 if i < remaining else 0)

    # Generate columns
    columns = []
    for t in types:
        for j in range(counts.get(t, 0)):
            columns.append(ColumnDef(name=f"{t}_{j}", type=t))

    return columns


def resolve_config(config: FakeConfig) -> list[ColumnDef]:
    """
    Resolve full column list:
    1. Start with explicit columns
    2. Expand type_ratio for remaining slots
    3. Apply 3-level merge to each column
    4. Log resolved config
    """
    columns = list(config.columns)

    if config.type_ratio and config.total_columns:
        auto_columns = _expand_type_ratio(
            config.type_ratio, config.total_columns, len(columns)
        )
        columns.extend(auto_columns)

    # Check for duplicate column names
    names = [c.name for c in columns]
    dupes = [n for n in names if names.count(n) > 1]
    if dupes:
        raise ValueError(f"Duplicate column names: {set(dupes)}")

    resolved = [_resolve_column(col, config.defaults) for col in columns]

    logging.info("Resolved fake config: %d columns", len(resolved))
    for col in resolved:
        logging.debug("  %s (%s): %s", col.name, col.type, col.model_dump(exclude={"name", "type"}))

    return resolved


# ---------------------------------------------------------------------------
# NZFakerConfig — per-column fake data generator
# ---------------------------------------------------------------------------


class NZFakerConfig(NaFaker):
    """
    Per-column fake data generator driven by FakeConfig.
    Each column has its own ColumnDef with independent generation parameters.
    """

    def __init__(self, columns: list[ColumnDef]):
        self.fake = Faker(use_weighting=False)
        self.columns = columns
        self._generator_state: dict[str, float | int | object] = {}

        # Pre-compute string cardinality pools per column
        self._string_pools: dict[str, list[str]] = {}
        for col in columns:
            if col.type == "string" and col.cardinality and col.cardinality > 0:
                length = col.length or 10
                if col.pattern:
                    provider = getattr(self.fake, col.pattern, None)
                    if provider and callable(provider):
                        self._string_pools[col.name] = list(
                            {provider() for _ in range(col.cardinality * 2)}
                        )[: col.cardinality]
                        continue
                self._string_pools[col.name] = [
                    self.fake.unique.pystr(max_chars=length)
                    for _ in range(col.cardinality)
                ]

    def get_schema(self) -> list[Field]:
        return [
            Field(name=col.name, type=col.type, nullable=(col.null_ratio > 0))
            for col in self.columns
        ]

    def _coerce_value(self, col: ColumnDef, value):
        if value is None:
            return None

        if col.type in _INT_TYPES:
            return int(round(float(value)))

        if col.type in _FLOAT_TYPES or col.type.startswith("decimal("):
            value = float(value)
            if col.precision is not None:
                return round(value, col.precision)
            return value

        if col.type == "string":
            return str(value)

        if col.type == "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes", "y", "on"}:
                    return True
                if lowered in {"false", "0", "no", "n", "off"}:
                    return False
            return bool(value)

        return value

    def _generate_from_generator(
        self,
        col: ColumnDef,
        elapsed_seconds: float,
    ):
        generator = col.generator
        if generator is None:
            return None

        if generator.kind == "constant":
            return self._coerce_value(col, generator.value)

        if generator.kind == "sine":
            offset = generator.offset or 0.0
            phase = generator.phase_sec or 0.0
            noise = generator.noise or 0.0
            value = offset + generator.amplitude * math.sin(
                (2 * math.pi * (elapsed_seconds + phase)) / generator.period_sec
            )
            if noise > 0:
                value += random.gauss(0, noise)
            return self._coerce_value(col, value)

        if generator.kind == "ramp":
            value = generator.start + (generator.slope_per_sec * elapsed_seconds)
            if generator.wrap and generator.min is not None and generator.max is not None:
                span = generator.max - generator.min
                if span > 0:
                    value = generator.min + ((value - generator.min) % span)
            if generator.min is not None:
                value = max(generator.min, value)
            if generator.max is not None:
                value = min(generator.max, value)
            return self._coerce_value(col, value)

        if generator.kind == "step":
            index = int(elapsed_seconds // generator.dwell_sec) % len(generator.values)
            return self._coerce_value(col, generator.values[index])

        if generator.kind == "choice_cycle":
            index = int(elapsed_seconds // generator.interval_sec) % len(generator.values)
            return self._coerce_value(col, generator.values[index])

        if generator.kind == "random_walk":
            current = self._generator_state.get(col.name, generator.start)
            current = float(current) + random.gauss(0, generator.step_stddev)
            if generator.min is not None:
                current = max(generator.min, current)
            if generator.max is not None:
                current = min(generator.max, current)
            self._generator_state[col.name] = current
            return self._coerce_value(col, current)

        raise ValueError(f"Unsupported generator kind: {generator.kind}")

    def _generate_one(
        self,
        col: ColumnDef,
        elapsed_seconds: float = 0.0,
    ):
        if col.null_ratio > 0 and random.random() < col.null_ratio:
            return None

        if col.generator:
            return self._generate_from_generator(col, elapsed_seconds)

        if col.values:
            return random.choice(col.values)

        return self._generate_by_type(col)

    def _generate_by_type(self, col: ColumnDef):
        t = col.type

        # Integer family
        if t in _INT_TYPES:
            return random.randint(int(col.min), int(col.max))

        # Float family (including decimal)
        if t in _FLOAT_TYPES or t.startswith("decimal("):
            val = random.uniform(col.min, col.max)
            if col.precision is not None:
                return round(val, col.precision)
            return val

        # String
        if t == "string":
            if col.name in self._string_pools:
                return random.choice(self._string_pools[col.name])
            if col.pattern:
                provider = getattr(self.fake, col.pattern, None)
                if provider and callable(provider):
                    return provider()
                raise ValueError(f"Unknown faker pattern: {col.pattern}")
            length = col.length or 10
            return "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(length)
            )

        # Boolean
        if t == "boolean":
            return random.choice([True, False])

        # Binary
        if t == "binary":
            return self.fake.binary(length=col.length or 10)

        # Date
        if t == "date":
            return self.fake.date_object()

        # Timestamp
        if t == "timestamp":
            tz = ZoneInfo(col.tzinfo) if col.tzinfo else timezone.utc
            return self.fake.date_time(tz)

        # Timestamp without timezone
        if t == "timestamp_ntz":
            return self.fake.date_time()

        raise ValueError(f"Unsupported column type: {t}")

    def values(self, elapsed_seconds: float = 0.0) -> dict[str, any]:
        return {
            col.name: self._generate_one(col, elapsed_seconds=elapsed_seconds)
            for col in self.columns
        }


# ---------------------------------------------------------------------------
# CLI helpers — shared across produce_fake, publish_fake, redpanda_http_fake
# ---------------------------------------------------------------------------


def export_schema_file(faker: "NZFakerConfig", filepath: str = None) -> str:
    """
    Export NZFakerConfig schema to a JSON file compatible with nz_load_fields().
    Returns the file path. If filepath is None, creates a temp file.
    """
    import tempfile

    import orjson

    fields = faker.get_schema()
    data = [f.model_dump(exclude_none=True) for f in fields]

    if filepath is None:
        fd, filepath = tempfile.mkstemp(suffix=".json", prefix="fake_schema_")
        with open(fd, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    else:
        with open(filepath, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    logging.info("Exported schema to %s (%d fields)", filepath, len(fields))
    return filepath


def add_fake_args(parser: argparse.ArgumentParser) -> None:
    """Add all faker-related CLI arguments (legacy + new --fake-config)."""
    parser.add_argument(
        "--fake-config",
        help="Path to JSON fake config file for per-column generation",
    )
    parser.add_argument(
        "--fake-string-length", help="Length of string field", type=int, default=10
    )
    parser.add_argument(
        "--fake-string-cardinality",
        help="Number of string field cardinality",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--fake-binary-length", help="Length of binary field", type=int, default=10
    )
    parser.add_argument(
        "--fake-timestamp-tzinfo", help="Datetime timezone", default="UTC"
    )


def create_faker(args) -> NaFaker:
    """
    Factory: create the appropriate faker instance based on CLI args.

    Priority:
      1. edge output type → NZFakerEdge (unchanged)
      2. --fake-config → NZFakerConfig (new)
      3. --nz-schema-file → NZFakerField (legacy)
    """
    from utils.nazare import edge_load_datasources, nz_load_fields
    from utils.nzfake import NZFakerEdge, NZFakerField
    from utils.utils import download_s3file

    schema_file = getattr(args, "nz_schema_file", None)
    schema_file_type = getattr(args, "nz_schema_file_type", "json")
    fake_config_file = getattr(args, "fake_config", None)

    if schema_file and schema_file.startswith("s3a://"):
        schema_file = download_s3file(
            schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    # Edge output type — separate path, not affected by --fake-config
    output_type = getattr(args, "output_type", "json")
    if output_type == "edge":
        if not schema_file:
            raise RuntimeError(
                "Please provide --nz-schema-file for edge output type"
            )
        return NZFakerEdge(edge_load_datasources(schema_file, schema_file_type))

    # --fake-config takes precedence
    if fake_config_file:
        if schema_file:
            logging.warning(
                "Both --fake-config and --nz-schema-file specified. "
                "Using --fake-config; --nz-schema-file will be ignored."
            )
        config = load_fake_config(fake_config_file)
        columns = resolve_config(config)
        return NZFakerConfig(columns)

    # Legacy path
    if not schema_file:
        raise RuntimeError(
            "Please provide either --fake-config or --nz-schema-file"
        )
    return NZFakerField(
        nz_load_fields(schema_file, schema_file_type),
        args.fake_string_length,
        args.fake_string_cardinality,
        args.fake_binary_length,
        ZoneInfo(args.fake_timestamp_tzinfo),
    )
