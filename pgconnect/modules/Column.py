import asyncpg
import pgconnect


class Column:
    def __init__(
            self,
            type: pgconnect.DataType,
            length: int = None,
            primary_key: bool = False,
            not_null: bool = False,
            unique: bool = False,
            default: any = None
    ) -> None:
        """
        Initializes the column with specified properties.
        
        Notes:
        - Length should only be set for VARCHAR and CHAR types.
        - Default values will be applied if provided.
        """
        if length and type not in [pgconnect.DataType.VARCHAR, pgconnect.DataType.CHAR]:
            raise ValueError("Length can only be set for VARCHAR and CHAR types")
        self.type = type
        self.length = length
        self.primary_key = primary_key
        self.not_null = not_null
        self.unique = unique
        self.default = default
        self.name = None
        self.table = None
    


    def __repr__(self) -> str:
        return f"<Column {self.name}>"

    def __str__(self) -> str:
        return f"<Column {self.name}>"