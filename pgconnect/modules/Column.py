import asyncpg
import pgconnect


class Column:
    def __init__(
            self,
            name: str,
            type: pgconnect.DataType
    ) -> None:
        """
        Initializes the column with specified properties.
        
        Notes:
        - Length should only be set for VARCHAR and CHAR types.
        - Default values will be applied if provided.
        """
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return f"<Column {self.name}>"

    def __str__(self) -> str:
        return f"<Column {self.name}>"