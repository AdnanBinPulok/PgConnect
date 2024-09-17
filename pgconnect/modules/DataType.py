from typing import Any, Dict, List, Optional, Tuple, Union


# unchangable class DataType
class DataType:
    """
    A class to represent PostgreSQL data types.
    """
    # Numeric types
    INT = "INTEGER"
    SMALLINT = "SMALLINT"
    BIGINT = "BIGINT"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"
    MONEY = "MONEY"

    # Character types
    TEXT = "TEXT"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"

    # Binary types
    BYTEA = "BYTEA"

    # Date/time types
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"  # Timestamp with timezone
    DATE = "DATE"
    TIME = "TIME"
    TIMETZ = "TIMETZ"  # Time with timezone
    INTERVAL = "INTERVAL"

    # Boolean type
    BOOLEAN = "BOOLEAN"

    # UUID type
    UUID = "UUID"

    # JSON types
    JSON = "JSON"
    JSONB = "JSONB"

    # Network address types
    CIDR = "CIDR"
    INET = "INET"
    MACADDR = "MACADDR"

    # Geometric types
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"

    # Arrays
    ARRAY = "ARRAY"

    # Range types
    INT4RANGE = "INT4RANGE"
    INT8RANGE = "INT8RANGE"
    NUMRANGE = "NUMRANGE"
    TSRANGE = "TSRANGE"  # Timestamp range
    TSTZRANGE = "TSTZRANGE"  # Timestamp with time zone range
    DATERANGE = "DATERANGE"

    # Composite and special types
    HSTORE = "HSTORE"  # Key-value store
    XML = "XML"
    TSQUERY = "TSQUERY"
    TSVECTOR = "TSVECTOR"