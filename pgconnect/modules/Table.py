import asyncpg
import traceback

import pgconnect

class Table:
    def __init__(
            self,
            name: str,
            connection: pgconnect.Connection,
            columns: dict
    ) -> None:
        self.name = name
        self.connection = connection
        self.columns = columns

    async def create(self):
        try:
            connection = await self.connection.get_connection()
            query = f"CREATE TABLE IF NOT EXISTS {self.name} ("
            for column_name, column in self.columns.items():
                column: pgconnect.Column
                query += f"{column_name} {column.type}"
                if column.length:
                    query += f"({column.length})"
                if column.primary_key:
                    query += " PRIMARY KEY"
                if column.not_null:
                    query += " NOT NULL"
                if column.unique:
                    query += " UNIQUE"
                if column.default:
                    query += f" DEFAULT {column.default}"
                query += ", "
            query = query[:-2] + ")"
            print(query)
            await connection.execute(query)
        except Exception as e:
            traceback.print_exc()
            raise e