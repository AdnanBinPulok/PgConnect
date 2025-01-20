import asyncpg
import traceback
from .Column import Column
from typing import Optional, List, Any, Dict
from pgconnect import Connection
from cachetools import TTLCache
import asyncio

class Table:
    def __init__(
            self,
            name: str,
            connection: Connection,
            columns: List[Column],
            cache: bool = False,
            cache_key: Optional[str] = None,
            cache_ttl: Optional[int] = None,  # Change to Optional[int]
            cache_maxsize: int = 1000
    ) -> None:
        """
        Initializes the Table object.

        :param name: The name of the table.
        :param connection: The connection object to the PostgreSQL database.
        :param columns: A list of Column objects defining the table schema.
        :param cache: Whether to enable caching.
        :param cache_key: The key to use for caching.
        :param cache_ttl: The time-to-live for cache entries in seconds.
        :param cache_maxsize: The maximum size of the cache.
        """
        self.name = name
        self.connection: Connection = connection
        self.columns = columns
        self.cache = cache
        self.cache_key = cache_key
        self.cache_ttl = cache_ttl if cache_ttl is not None else 0  # Ensure cache_ttl is a valid number
        self.cache_maxsize = cache_maxsize
        self._conn = None  # Initialize the connection attribute
        if cache and not cache_key:
            raise ValueError("cache_key must be provided if cache is enabled")
        
        self.caches = TTLCache(maxsize=cache_maxsize, ttl=self.cache_ttl) if cache else None
        self.timeout = 5  # Set the timeout to 5 seconds
    def clear_cache(self):
        """
        Clears the cache for the table.
        """
        if not self.cache:
            raise ValueError("Cache is not enabled")
        self.caches.clear()

    async def _get_connection(self):
        return await self.connection.get_connection()

    def _get_cache_key(self, **kwargs):
        """
        Generates a string cache key from the provided keyword arguments.
        """
        if self.cache_key:
            if self.cache_key in [column.name for column in self.columns]:
                return str(kwargs.get(self.cache_key)) if self.cache_key in kwargs else None
        return None
    
    async def ensure_connection_available(self, connection):
        """
        Ensure the connection is available by checking if it is in a transaction.
        If the connection is busy, wait for a short period and retry.
        """
        if not isinstance(self.connection.connection, asyncpg.pool.Pool):
            for i in range(5):
                if connection.is_in_transaction():
                    await asyncio.sleep(1)  # Wait for 1 second before retrying
                else:
                    break
            if connection.is_in_transaction():
                raise Exception("Connection is still busy after multiple retries")

    async def create(self):
        """
        Creates the table in the PostgreSQL database. If the table already exists,
        it will add new columns and drop removed columns based on the current schema.
        """
        try:
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{self.name}'
            );
            """
            table_exists = await connection.fetchval(table_exists_query, timeout=self.timeout)
            
            if table_exists:
                existing_columns_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{self.name}';
                """
                existing_columns = await connection.fetch(existing_columns_query, timeout=self.timeout)
                existing_column_names = {row['column_name'] for row in existing_columns}
                
                alter_table_queries = []
                new_column_names = {column.name for column in self.columns}
                
                for column in self.columns:
                    if column.name not in existing_column_names:
                        alter_table_queries.append(f"ALTER TABLE {self.name} ADD COLUMN {column.name} {column.type};")
                
                for existing_column in existing_column_names:
                    if existing_column not in new_column_names:
                        alter_table_queries.append(f"ALTER TABLE {self.name} DROP COLUMN {existing_column};")
                
                for query in alter_table_queries:
                    await connection.execute(query, timeout=self.timeout)
                return

            query = f"CREATE TABLE IF NOT EXISTS {self.name} (\n"
            column_definitions = []
            for column in self.columns:
                column: Column
                column_definitions.append(f"{column.name} {column.type}")
            query += ",\n".join(column_definitions) + "\n);"
            await connection.execute(query, timeout=self.timeout)
        except asyncpg.PostgresError as e:
            print(f"Failed to create table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()
        
    async def insert(self, **kwargs):
        """
        Inserts a row into the table.

        :param kwargs: The column values to insert.
        :raises ValueError: If no valid columns are provided.
        :raises RuntimeError: If there is a database error.
        """
        try:
            filtered_columns = [column for column in self.columns if column.name in kwargs]
            if not filtered_columns:
                raise ValueError("No valid columns provided")

            columns = ", ".join(column.name for column in filtered_columns)
            values = ", ".join(f"${i+1}" for i in range(len(filtered_columns)))
            query = f"INSERT INTO {self.name} ({columns}) VALUES ({values}) RETURNING *"
            
            query_values = [kwargs[column.name] for column in filtered_columns]

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)

            row = await connection.fetchrow(query, *query_values, timeout=self.timeout)

            if self.cache:
                cache_key = self._get_cache_key(**row)
                if cache_key:
                    self.caches[cache_key] = row

            return row
        except asyncpg.PostgresError as e:
            print(f"Failed to insert into table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()


    async def update(self, where: Dict[str, Any], **kwargs):
        """
        Updates rows in the table.

        :param where: A dictionary specifying the conditions for the rows to update.
        :param kwargs: The column values to update.
        :raises ValueError: If no valid columns are provided.
        :raises RuntimeError: If there is a database error.
        """
        try:
            filtered_columns = [column for column in self.columns if column.name in kwargs]
            if not filtered_columns:
                raise ValueError("No valid columns provided")

            set_clause = ", ".join(f"{column.name} = ${i+1}" for i, column in enumerate(filtered_columns))
            where_clause = " AND ".join(f"{key} = ${len(filtered_columns) + i + 1}" for i, key in enumerate(where.keys()))
            query = f"UPDATE {self.name} SET {set_clause} WHERE {where_clause} RETURNING *"
            
            query_values = [kwargs[column.name] for column in filtered_columns] + list(where.values())

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)

            if self.cache:
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        self.caches[cache_key] = row

            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to update table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def delete(self, **where):
        """
        Deletes rows from the table.

        :param where: A dictionary specifying the conditions for the rows to delete.
        :raises ValueError: If no conditions are provided.
        :raises RuntimeError: If there is a database error.
        """
        try:
            if not where:
                raise ValueError("No conditions provided for delete")

            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys()))
            query = f"DELETE FROM {self.name} WHERE {where_clause} RETURNING *"
            
            query_values = list(where.values())

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)

            if self.cache:
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key and cache_key in self.caches:
                        del self.caches[cache_key]

            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to delete from table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def select(self, *columns, **where):
        """
        Selects rows from the table.

        :param columns: The columns to select.
        :param where: A dictionary specifying the conditions for the rows to select.
        :raises RuntimeError: If there is a database error.
        :return: The selected rows.
        """
        try:

            connection = await self._get_connection()
            cache_key = self._get_cache_key(**where)
            if self.cache and cache_key and cache_key in self.caches:
                return [self.caches[cache_key]]

            columns_clause = ", ".join(columns) if columns else "*"
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            query = f"SELECT {columns_clause} FROM {self.name} WHERE {where_clause}"
            
            query_values = list(where.values())
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)

            if self.cache:
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        self.caches[cache_key] = row
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to select from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def get(self, **where):
        """
        Gets a single row from the table.

        :param where: A dictionary specifying the conditions for the row to get.
        :raises RuntimeError: If there is a database error.
        :return: The selected row.
        """
        try:

            # Validate and convert types
            for column in self.columns:
                if column.name in where:
                    if isinstance(where[column.name], str) and column.type in ["INTEGER", "BIGINT"]:
                        where[column.name] = int(where[column.name])
                    elif isinstance(where[column.name], str) and column.type == "BOOLEAN":
                        where[column.name] = where[column.name].lower() in ["true", "1", "yes"]

            connection = await self._get_connection()

            cache_key = self._get_cache_key(**where)
            if self.cache and cache_key and cache_key in self.caches:
                return self.caches[cache_key]
            
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            query = f"SELECT * FROM {self.name} WHERE {where_clause}"
            query_values = list(where.values())
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            row = await connection.fetchrow(query, *query_values, timeout=self.timeout)

            if self.cache and row:
                cache_key = self._get_cache_key(**row)
                if cache_key:
                    self.caches[cache_key] = row
            return row
        except asyncpg.PostgresError as e:
            print(f"Failed to get row from table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def gets(self, **where):
        """
        Gets multiple rows from the table.

        :param where: A dictionary specifying the conditions for the rows to get.
        :raises RuntimeError: If there is a database error.
        :return: The selected rows.
        """
        try:
            connection = await self._get_connection()
            cache_key = self._get_cache_key(**where)
            if self.cache and cache_key and cache_key in self.caches:
                return [self.caches[cache_key]]
            
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            query = f"SELECT * FROM {self.name} WHERE {where_clause}"

            query_values = list(where.values())

            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)

            if self.cache:
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        self.caches[cache_key] = row
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to get rows from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def get_all(self):
        """
        Gets all rows from the table.

        :raises RuntimeError: If there is a database error.
        :return: The selected rows.
        """
        try:
            query = f"SELECT * FROM {self.name}"
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, timeout=self.timeout)
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to get all rows from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def count(self, **where):
        """
        Counts the number of rows in the table.

        :param where: A dictionary specifying the conditions for the rows to count.
        :raises RuntimeError: If there is a database error.
        :return: The count of rows.
        """
        try:
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            query = f"SELECT COUNT(*) FROM {self.name} WHERE {where_clause}"
            
            query_values = list(where.values())

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            count = await connection.fetchval(query, *query_values, timeout=self.timeout)
            return count
        except asyncpg.PostgresError as e:
            print(f"Failed to count rows in table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def exists(self, **where):
        """
        Checks if any rows exist in the table that match the conditions.

        :param where: A dictionary specifying the conditions for the rows to check.
        :raises RuntimeError: If there is a database error.
        :return: True if any rows exist, False otherwise.
        """
        try:
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            query = f"SELECT EXISTS (SELECT 1 FROM {self.name} WHERE {where_clause})"
            
            query_values = list(where.values())

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            exists = await connection.fetchval(query, *query_values, timeout=self.timeout)
            return exists
        except asyncpg.PostgresError as e:
            print(f"Failed to check existence in table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    
    async def get_page(self, page: int = 1, limit: int = 10, where: Dict[str, Any] = None, order_by: str = None, order: str = 'ASC'):
        """
        Gets a paginated set of rows from the table.
    
        :param page: The page number to retrieve.
        :param limit: The number of rows per page.
        :param where: A dictionary specifying the conditions for the rows to get.
        :param order_by: The column to order the results by.
        :param order: The order direction ('ASC' or 'DESC').
        :raises RuntimeError: If there is a database error.
        :return: The selected rows.
        """
        try:
            offset = (page - 1) * limit
            where_clause = " AND ".join(f"{key} = ${i+1}" for i, key in enumerate(where.keys())) if where else "1=1"
            order_clause = f"ORDER BY {order_by} {order}" if order_by else ""
            query = f"SELECT * FROM {self.name} WHERE {where_clause} {order_clause} LIMIT {limit} OFFSET {offset}"
    
            query_values = list(where.values()) if where else []
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)
    
            if self.cache:
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        self.caches[cache_key] = row
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to get paginated rows from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

    
    async def search(self, by: Optional[list], keyword: str, limit: int = 10, where: Dict[str, Any] = None, order_by: str = 'id', order: str = 'ASC'):
        """
        Searches the table for a keyword in the specified columns.
    
        :param by: The columns to search.
        :param keyword: The keyword to search for.
        :param limit: The maximum number of rows to return.
        :param where: Additional conditions for the search.
        :param order_by: The column to order the results by.
        :param order: The order direction (ASC or DESC).
        """
        try:
            if not by:
                raise ValueError("No columns provided for search")
            
            where_clause = " OR ".join(f"{column} ILIKE $1" for column in by)
            
            query_values = [f"%{keyword}%"]
            
            if where:
                additional_conditions = " AND ".join(f"{key} = ${i+2}" for i, key in enumerate(where.keys()))
                where_clause = f"({where_clause}) AND {additional_conditions}"
                query_values.extend(where.values())
            
            query = f"SELECT * FROM {self.name} WHERE {where_clause} ORDER BY {order_by} {order} LIMIT {limit}"
            
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            
            rows = await connection.fetch(query, *query_values, timeout=self.timeout)
            return rows
    
        except asyncpg.PostgresError as e:
            print(f"Failed to search table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def query(self, query: str, *args):
        """
        Executes a custom query on the table.

        :param query: The query to execute.
        :param args: The query arguments.
        :raises RuntimeError: If there is a database error.
        :return: The query result.
        """
        try:
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            result = await connection.fetch(query, *args, timeout=self.timeout)
            return result
        except asyncpg.PostgresError as e:
            print(f"Failed to execute query on table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                

        
    async def get_columns(self):
        """
        Retrieves the column names and types for the table.

        :raises RuntimeError: If there is a database error.
        :return: A list of dictionaries containing column names and types.
        """
        try:
            query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{self.name}';
            """
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            columns = await connection.fetch(query, timeout=self.timeout)
            return [{"name": column["column_name"], "type": column["data_type"]} for column in columns]
        except asyncpg.PostgresError as e:
            print(f"Failed to get columns for table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    def __repr__(self) -> str:
        return f"<Table {self.name}>"

    def __str__(self) -> str:
        return f"Table(name={self.name}, columns={self.columns})"
    
    def __getitem__(self, key: str) -> Optional[Column]:
        """
        Gets a column by its name.

        :param key: The name of the column.
        :return: The Column object if found, otherwise None.
        """
        for column in self.columns:
            if column.name == key:
                return column
        return None
    
    def __setitem__(self, key: str, value: Column):
        """
        Sets a column by its name.

        :param key: The name of the column.
        :param value: The Column object to set.
        """
        for i, column in enumerate(self.columns):
            if column.name == key:
                self.columns[i] = value
                return
        self.columns.append(value)

    def __delitem__(self, key: str):
        """
        Deletes a column by its name.

        :param key: The name of the column.
        :raises KeyError: If the column is not found.
        """
        for i, column in enumerate(self.columns):
            if column.name == key:
                del self.columns[i]
                return
        raise KeyError(f"Column {key} not found")
    
    async def drop(self):
        """
        Drops the table from the PostgreSQL database.

        :raises RuntimeError: If there is a database error.
        """
        try:
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            query = f"DROP TABLE IF EXISTS {self.name};"
            await connection.execute(query, timeout=self.timeout)
        except asyncpg.PostgresError as e:
            print(f"Failed to drop table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                
    async def truncate(self):
        """
        Truncates the table to remove all rows.

        :raises RuntimeError: If there is a database error.
        """
        try:
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            query = f"TRUNCATE TABLE {self.name};"
            await connection.execute(query, timeout=self.timeout)
        except asyncpg.PostgresError as e:
            print(f"Failed to truncate table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.close()

                