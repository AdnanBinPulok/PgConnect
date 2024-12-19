from typing import Any
import json

class DataType:
    """
    A class to represent PostgreSQL data types.
    """
    def __init__(self, type_name: str):
        self.type_name = type_name
        self.constraints = []
        self.length = None

    def primary_key(self):
        if self.type_name in ["TEXT", "BYTEA", "JSON", "JSONB", "ARRAY"]:
            raise ValueError(f"{self.type_name} cannot be a primary key")
        self.constraints.append("PRIMARY KEY")
        return self

    def not_null(self):
        self.constraints.append("NOT NULL")
        return self

    def unique(self):
        if self.type_name in ["TEXT", "BYTEA", "JSON", "JSONB", "ARRAY"]:
            raise ValueError(f"{self.type_name} cannot be unique")
        self.constraints.append("UNIQUE")
        return self

    def default(self, value: Any):
        if self.type_name in ["BYTEA", "ARRAY"]:
            raise ValueError(f"{self.type_name} cannot have a default value")
        if self.type_name in ["JSON", "JSONB"]:
            value = json.dumps(value)
        self.constraints.append(f"DEFAULT '{value}'")
        return self
    
    def check(self, condition: str):
        self.constraints.append(f"CHECK ({condition})")
        return self

    def references(self, table: str, column: str):
        if self.type_name in ["TEXT", "BYTEA", "JSON", "JSONB", "ARRAY"]:
            raise ValueError(f"{self.type_name} cannot reference another table")
        self.constraints.append(f"REFERENCES {table}({column})")
        return self

    def length(self, length: int):
        if self.type_name not in ["VARCHAR", "CHAR"]:
            raise ValueError(f"Length can only be set for VARCHAR and CHAR types")
        self.type_name = f"{self.type_name}({length})"
        return self

    def __str__(self):
        return f"{self.type_name} {' '.join(self.constraints)}"

    @classmethod
    def INT(cls):
        return cls("INTEGER")

    @classmethod
    def SMALLINT(cls):
        return cls("SMALLINT")

    @classmethod
    def BIGINT(cls):
        return cls("BIGINT")

    @classmethod
    def SERIAL(cls):
        return cls("SERIAL")

    @classmethod
    def BIGSERIAL(cls):
        return cls("BIGSERIAL")

    @classmethod
    def REAL(cls):
        return cls("REAL")

    @classmethod
    def DOUBLE_PRECISION(cls):
        return cls("DOUBLE PRECISION")

    @classmethod
    def NUMERIC(cls, precision: int = 10, scale: int = 0):
        return cls(f"NUMERIC({precision}, {scale})")

    @classmethod
    def DECIMAL(cls, precision: int = 10, scale: int = 0):
        return cls(f"DECIMAL({precision}, {scale})")

    @classmethod
    def MONEY(cls):
        return cls("MONEY")

    @classmethod
    def TEXT(cls):
        return cls("TEXT")

    @classmethod
    def VARCHAR(cls, length: int = 255):
        return cls(f"VARCHAR({length})")

    @classmethod
    def CHAR(cls, length: int = 1):
        return cls(f"CHAR({length})")

    @classmethod
    def BYTEA(cls):
        return cls("BYTEA")

    @classmethod
    def TIMESTAMP(cls):
        return cls("TIMESTAMP")

    @classmethod
    def TIMESTAMPTZ(cls):
        return cls("TIMESTAMPTZ")

    @classmethod
    def DATE(cls):
        return cls("DATE")

    @classmethod
    def TIME(cls):
        return cls("TIME")

    @classmethod
    def TIMETZ(cls):
        return cls("TIMETZ")

    @classmethod
    def INTERVAL(cls):
        return cls("INTERVAL")

    @classmethod
    def BOOLEAN(cls):
        return cls("BOOLEAN")

    @classmethod
    def UUID(cls):
        return cls("UUID")

    @classmethod
    def JSON(cls):
        return cls("JSON")

    @classmethod
    def JSONB(cls):
        return cls("JSONB")

    @classmethod
    def CIDR(cls):
        return cls("CIDR")

    @classmethod
    def INET(cls):
        return cls("INET")

    @classmethod
    def MACADDR(cls):
        return cls("MACADDR")

    @classmethod
    def POINT(cls):
        return cls("POINT")

    @classmethod
    def LINE(cls):
        return cls("LINE")

    @classmethod
    def LSEG(cls):
        return cls("LSEG")

    @classmethod
    def BOX(cls):
        return cls("BOX")

    @classmethod
    def PATH(cls):
        return cls("PATH")

    @classmethod
    def POLYGON(cls):
        return cls("POLYGON")

    @classmethod
    def CIRCLE(cls):
        return cls("CIRCLE")

    @classmethod
    def ARRAY(cls, base_type: str):
        return cls(f"{base_type}[]")

    @classmethod
    def INT4RANGE(cls):
        return cls("INT4RANGE")

    @classmethod
    def INT8RANGE(cls):
        return cls("INT8RANGE")

    @classmethod
    def NUMRANGE(cls):
        return cls("NUMRANGE")

    @classmethod
    def TSRANGE(cls):
        return cls("TSRANGE")

    @classmethod
    def TSTZRANGE(cls):
        return cls("TSTZRANGE")

    @classmethod
    def DATERANGE(cls):
        return cls("DATERANGE")

    @classmethod
    def HSTORE(cls):
        return cls("HSTORE")

    @classmethod
    def XML(cls):
        return cls("XML")

    @classmethod
    def TSQUERY(cls):
        return cls("TSQUERY")

    @classmethod
    def TSVECTOR(cls):
        return cls("TSVECTOR")