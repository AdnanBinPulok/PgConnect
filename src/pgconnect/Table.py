import time
import asyncpg
import traceback
from .Column import Column
from typing import Optional, List, Any, Dict
from . import Connection, RedisConnection
from cachetools import TTLCache
import asyncio
from .Filters import Between, Like, In


class Table:
    def __init__(
            self,
            name: str,
            connection: Connection,
            columns: List[Column],
            cache: bool = False,
            redis_cache: bool = False,
            redis_connection: Optional[RedisConnection] = None,
            cache_key: Optional[str] = None,
            cache_ttl: Optional[int] = None,  # Change to Optional[int]
            cache_maxsize: int = 1000,
            indexes: Optional[List[Dict[str, Any]]] = None,
            fetch_timeout: int = 5
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
        self.redis_cache = redis_cache
        self.redis_connection: Optional[RedisConnection] = redis_connection
        self.cache_key = cache_key
        self.cache_ttl = cache_ttl if cache_ttl is not None else 0  # Ensure cache_ttl is a valid number
        self.cache_maxsize = cache_maxsize
        self._conn = None  # Initialize the connection attribute

        if cache and not cache_key:
            raise ValueError("cache_key must be provided if cache is enabled")
        
        if redis_cache and not redis_connection:
            raise ValueError("redis_connection must be provided if redis_cache is enabled")

        if redis_cache and not cache_key:
            raise ValueError("cache_key must be provided if redis_cache is enabled")
        
        if cache and redis_cache:
            raise ValueError("cache (memcache) and redis_cache cannot be enabled at the same time")

        self.caches = TTLCache(maxsize=cache_maxsize, ttl=self.cache_ttl) if cache else None
        self.timeout = fetch_timeout  # Set the timeout to the provided fetch_timeout
        self.indexes = indexes if indexes is not None else []

    def clear_cache(self):
        """
        Clears the cache for the table.
        """
        if not self.cache:
            raise ValueError("Cache is not enabled")
        self.caches.clear()

    def cacheEnabled(self) -> bool:
        """Checks if caching is enabled."""
        return self.cache or (self.redis_cache and self.redis_connection)
    
    def redisCacheEnabled(self) -> bool:
        """Checks if Redis caching is enabled."""
        return self.redis_cache and self.redis_connection is not None

    async def setCache(self, key: str, value: Any):
        """Sets a value in the cache."""
        if not self.cacheEnabled():
            return None
        if self.cache:
            self.caches[key] = value
            return True
        elif self.redis_cache and self.redis_connection:
            try:
                await self.redis_connection.set(table_name=self.name, key=key, value=value, ttl=self.cache_ttl)
                return True
            except Exception as e:
                print(f"Error setting cache in Redis: {e}")
                return None
        else:
            return None
    
    async def getCache(self, key: str) -> Any:
        """Gets a value from the cache."""
        if not self.cacheEnabled():
            return None
        if self.cache:
            return self.caches.get(key)
        elif self.redis_cache and self.redis_connection:
            try:
                return await self.redis_connection.get(table_name=self.name, key=key)
            except Exception as e:
                print(f"Error getting cache from Redis: {e}")
                return None
        else:
            return None
    
    async def getCacheAsRecord(self, key: str) -> Any:
        """Gets a cached record (alias for getCache - now always returns RecordLike for records)."""
        return await self.getCache(key)
    
    async def cacheExists(self, key: str) -> bool:
        """Checks if a value exists in the cache."""
        if not self.cacheEnabled():
            return False
        if self.cache:
            return key in self.caches
        elif self.redis_cache and self.redis_connection:
            try:
                return await self.redis_connection.exists(table_name=self.name, key=key)
            except Exception as e:
                print(f"Error checking cache existence in Redis: {e}")
                return False
        else:
            return False

    async def clear_redis_cache(self):
        """
        Clears the Redis cache for the table.
        """
        if not self.redisCacheEnabled():
            return
        await self.redis_connection.clear_cache(self.name)

    async def pingDatabase(self) -> float:
        """
        Pings the database to check if the connection is alive.
        """
        try:
            start_time = time.time_ns()
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            await connection.execute("SELECT 1")
            return round((time.time_ns() - start_time) / 1_000_000, 4)  # Return ping time in milliseconds
        except Exception as e:
            print(f"Error pinging database: {e}")
            return -1
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

    async def pingRedis(self) -> float:
        """
        Pings the Redis cache to check if the connection is alive.
        """
        try:
            start_time = time.time_ns()
            if not self.redisCacheEnabled():
                return -1
            result = await self.redis_connection.ping()
            if result > 0:
                return result
            return round((time.time_ns() - start_time) / 1_000_000, 4)  # Return ping time in milliseconds
        except Exception as e:
            print(f"Error pinging Redis: {e}")
            return -1

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

    async def check_if_index_schema_correct(self):
        """
        Checks if the indexs schema is correct by comparing it with existing indexes.
        Returns True if the index is correct, False otherwise.
        """
        try:
            if not self.indexes:
                return True
            # if connection is busy wait 1 second and try again
            for index in self.indexes:
                # make sure nothing more than in that index is defined
                for key in index:
                    if key not in ["name", "columns", "unique"]:
                        print(f"Index {index['name']} has invalid key {key}. Skipping index schema check.\nExpected keys are: ['name', 'columns', 'unique']")
                        return False
            return True
        except Exception as e:
            print(f"Failed to check index schema for table {self.name}: {e}")
            return False

    async def delete_existing_non_defined_indexes_and_create_indexes(self):
        """
        Deletes existing indexes that are not defined in the current table schema.
        This is useful to clean up indexes that may have been created in previous versions of the table.
        """
        try:
            if not await self.check_if_index_schema_correct():
                self.indexes = []
                print(f"Index schema for table {self.name} is not correct. Skipping index deletion and creation.")

            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            existing_indexes_query = f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{self.name}';
            """
            existing_indexes = await connection.fetch(existing_indexes_query, timeout=self.timeout)
            # drop indexes that are not defined in the table
            for index in existing_indexes:
                index_name = index['indexname']
                if index_name == f"{self.name}_pkey":
                    continue
                all_columns_indexes_possibles = [f"{self.name}_{column.name}_key" for column in self.columns]
                if index_name in all_columns_indexes_possibles:
                    continue
                if self.indexes:
                    if not any(index_name == idx.get("name", None) for idx in self.indexes):
                        drop_index_query = f"DROP INDEX IF EXISTS {index_name};"
                        await connection.execute(drop_index_query, timeout=self.timeout)
                else:
                    # If no indexes are defined, drop all existing indexes except the primary key
                    drop_index_query = f"DROP INDEX IF EXISTS {index_name};"
                    await connection.execute(drop_index_query, timeout=self.timeout)

            # Now create indexes defined in the schema
            if self.indexes:
                await self.create_indexes(existing_indexes)

        except asyncpg.PostgresError as e:
            print(f"Failed to delete existing non-defined indexes for table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

    async def create_indexes(self, already_existing_indexes: List):
        """
        Creates indexes for the table based on the defined indexes in the schema.
        If an index already exists, it will skip creating that index.
        """
        try:
            connection = await self._get_connection()
            # if connection is busy wait 1 second and try again
            await self.ensure_connection_available(connection)
            for index in self.indexes:
                index_name = index.get("name", f"idx_{self.name}_{'_'.join(index.get('columns', []))}")
                if any(index_name == idx.get("name", None) for idx in already_existing_indexes):
                    print(f"Index {index_name} already exists, skipping creation.")
                    continue
                columns = ", ".join(index['columns'])
                unique = "UNIQUE" if index.get("unique", False) else ""
                create_index_query = f"CREATE {unique} INDEX IF NOT EXISTS {index_name} ON {self.name} ({columns});"
                await connection.execute(create_index_query, timeout=self.timeout)
        except asyncpg.PostgresError as e:
            print(f"Failed to create indexes for table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

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

                # After altering the table, create indexes if defined
                await self.delete_existing_non_defined_indexes_and_create_indexes()
                return

            query = f"CREATE TABLE IF NOT EXISTS {self.name} (\n"
            column_definitions = []
            for column in self.columns:
                column: Column
                column_definitions.append(f"{column.name} {column.type}")
            query += ",\n".join(column_definitions) + "\n);"

            await connection.execute(query, timeout=self.timeout)

            # create table is done, now create indexes if defined

            

            # if the table has indexed not defined here delete them
            # get all indexes for the table
            await self.delete_existing_non_defined_indexes_and_create_indexes()

            
        except asyncpg.PostgresError as e:
            print(f"Failed to create table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()
        
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

            if self.cacheEnabled():
                cache_key = self._get_cache_key(**row)
                if cache_key:
                    await self.setCache(cache_key, row)

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
                await connection.release_connection()

    async def insert_many(self, columns: List[str], values: List[Dict[str, Any]]):
        """
        Inserts multiple rows into the table.

        :param columns: The list of column names to insert.
        :param values: A list of dictionaries containing the column values for each row.
        :raises ValueError: If no valid columns are provided or if values do not match columns.
        :raises RuntimeError: If there is a database error.
        """
        try:
            if not columns or not values:
                raise ValueError("No valid columns or values provided")

            filtered_columns = [column for column in self.columns if column.name in columns]
            if not filtered_columns:
                raise ValueError("No valid columns provided")

            columns_clause = ", ".join(column.name for column in filtered_columns)
            
            # Build multi-row VALUES clause
            value_rows = []
            query_values = []
            param_index = 1
            
            for value in values:
                row_placeholders = []
                for index, column in enumerate(filtered_columns):
                    row_placeholders.append(f"${param_index}")
                    query_values.append(value[index])
                    param_index += 1
                value_rows.append(f"({', '.join(row_placeholders)})")
            
            values_clause = ", ".join(value_rows)
            query = f"INSERT INTO {self.name} ({columns_clause}) VALUES {values_clause} RETURNING *"

            connection = await self._get_connection()
            await self.ensure_connection_available(connection)

            rows = await connection.fetch(query, *query_values, timeout=self.timeout)

            if self.cacheEnabled():
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        await self.setCache(cache_key, row)

            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to insert many into table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()


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

            if self.cacheEnabled():
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        await self.setCache(cache_key, row)

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
                await connection.release_connection()

                
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

            if self.cacheEnabled():
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key and await self.cacheExists(cache_key):
                        await self.deleteCache(cache_key)

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
                await connection.release_connection()

    async def _build_where_clause(self, where: Dict[str, Any]) -> tuple[str, list]:
        """
        Build WHERE clause from filters dictionary.
        Returns tuple of (where_clause, parameters)
        """
        if not where:
            return "1=1", []

        conditions = []
        params = []

        for key, value in where.items():
            if isinstance(value, (Between, Like, In)):
                conditions.append(value.to_sql(key, params))
            else:
                params.append(value)
                conditions.append(f"{key} = ${len(params)}")

        return " AND ".join(conditions), params

    async def select(self, *columns, **where):
        """
        Selects rows from the table with advanced filtering.

        :param columns: The columns to select.
        :param where: A dictionary with column names as keys and values/filters as values.
        Example:
            table.select('name', 'age', age=Filters.Between(18, 30), name=Filters.Like('John'))
        Warning:
            This function does not support caching.
        """
        try:
            connection = await self._get_connection()
            columns_clause = ", ".join(columns) if columns else "*"
            where_clause, params = await self._build_where_clause(where)
            query = f"SELECT {columns_clause} FROM {self.name} WHERE {where_clause}"

            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *params, timeout=self.timeout)
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to select from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

    async def get(self, **where):
        """
        Gets a single row from the table with advanced filtering.

        :param where: A dictionary with column names as keys and values/filters as values.
        Example:
            table.get(id=1, age=Filters.Between(18, 30))
        """
        try:
            connection = await self._get_connection()

            # Check cache first if enabled
            cache_key = self._get_cache_key(**where)
            if self.cacheEnabled() and cache_key and await self.cacheExists(cache_key):
                return await self.getCache(cache_key)

            where_clause, params = await self._build_where_clause(where)
            query = f"SELECT * FROM {self.name} WHERE {where_clause} LIMIT 1"

            await self.ensure_connection_available(connection)
            row = await connection.fetchrow(query, *params, timeout=self.timeout)

            if self.cacheEnabled() and row:
                cache_key = self._get_cache_key(**row)
                if cache_key:
                    await self.setCache(cache_key, row)
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
                await connection.release_connection()

    async def gets(self, **where):
        """
        Gets multiple rows from the table with advanced filtering.

        :param where: A dictionary with column names as keys and values/filters as values.
        Example:
            table.gets(status='active', age=Filters.Between(18, 30))
        """
        try:
            connection = await self._get_connection()

            where_clause, params = await self._build_where_clause(where)
            query = f"SELECT * FROM {self.name} WHERE {where_clause}"

            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *params, timeout=self.timeout)

            if self.cacheEnabled():
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        await self.setCache(cache_key, row)
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to get rows from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

    async def get_page(self, page: int = 1, limit: int = 10, where: Dict[str, Any] = None, 
                    order_by: str = None, order: str = 'ASC'):
        """
        Gets a paginated set of rows with advanced filtering.

        :param page: The page number to retrieve.
        :param limit: The number of rows per page.
        :param where: A dictionary with column names as keys and values/filters as values.
        :param order_by: The column to order by.
        :param order: The sort order ('ASC' or 'DESC').
        Example:
            table.get_page(1, 10, where={'status': 'active', 'age': Filters.Between(18, 30)})
        """
        try:
            offset = (page - 1) * limit
            where = where or {}
            where_clause, params = await self._build_where_clause(where)
            order_clause = f"ORDER BY {order_by} {order}" if order_by else ""
            
            query = f"""
                SELECT * FROM {self.name} 
                WHERE {where_clause} 
                {order_clause} 
                LIMIT {limit} OFFSET {offset}
            """

            connection = await self._get_connection()
            await self.ensure_connection_available(connection)
            rows = await connection.fetch(query, *params, timeout=self.timeout)

            if self.cacheEnabled():
                for row in rows:
                    cache_key = self._get_cache_key(**row)
                    if cache_key:
                        await self.setCache(cache_key, row)
            return rows
        except asyncpg.PostgresError as e:
            print(f"Failed to get paginated rows from table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

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
                await connection.release_connection()

    async def count(self, **where):
        """
        Counts the number of rows in the table.

        :param where: A dictionary with column names as keys and values/filters as values.
        Example:
            table.count(status='active', age=Filters.Between(18, 30))
        """
        try:
            where_clause, params = await self._build_where_clause(where)
            query = f"SELECT COUNT(*) FROM {self.name} WHERE {where_clause}"
            
            connection = await self._get_connection()
            await self.ensure_connection_available(connection)
            count = await connection.fetchval(query, *params, timeout=self.timeout)
            return count
        except asyncpg.PostgresError as e:
            print(f"Failed to count rows in table {self.name}: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

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
                await connection.release_connection()

    async def search(self, by: Optional[list], keyword: str, page: int = 1, limit: int = 10, 
                    where: Dict[str, Any] = None, order_by: str = 'id', order: str = 'ASC'):
        """
        Searches the table for a keyword in the specified columns with pagination.

        :param by: The columns to search.
        :param keyword: The keyword to search for.
        :param page: The page number (starting from 1).
        :param limit: The number of rows per page.
        :param where: Additional conditions using regular filters.
        :param order_by: The column to order the results by.
        :param order: The order direction (ASC or DESC).
        Example:
            table.search(
                by=['name', 'email'],
                keyword='john',
                where={'status': 'active', 'age': Filters.Between(18, 30)}
            )
        """
        try:
            if not by:
                raise ValueError("No columns provided for search")
            
            offset = (page - 1) * limit
            
            # Start parameter index at 1
            param_index = 1
            
            # Create the WHERE clause for the search columns with proper parameter index
            search_clause = " OR ".join(f"{column}::text ILIKE ${param_index}" for column in by)
            query_values = [f"%{keyword}%"]
            
            # Handle additional where conditions
            if where:
                where_clause, where_params = await self._build_where_clause(where)
                
                # Replace the generic parameter placeholders with properly indexed ones
                param_parts = []
                for i, part in enumerate(where_clause.split('$')):
                    if i == 0:  # First part has no $ prefix
                        param_parts.append(part)
                    elif part.strip():  # For parts that have content
                        # Extract the parameter number and the rest of the string
                        num_end = 0
                        while num_end < len(part) and part[num_end].isdigit():
                            num_end += 1
                        if num_end > 0:
                            new_index = param_index + int(part[:num_end])
                            param_parts.append(f"{new_index}{part[num_end:]}")
                
                # Rebuild the where clause with adjusted parameter indices
                adjusted_where_clause = '$'.join(param_parts)
                search_clause = f"({search_clause}) AND ({adjusted_where_clause})"
                query_values.extend(where_params)
            
            query = f"""
                SELECT * FROM {self.name} 
                WHERE {search_clause} 
                ORDER BY {order_by} {order} 
                LIMIT {limit} OFFSET {offset}
            """
            
            connection = await self._get_connection()
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
                await connection.release_connection()
    
    async def count_search(self, by: Optional[list], keyword: str, where: Dict[str, Any] = None) -> Optional[int]:
        """
        Counts the number of rows that match the search criteria.

        :param by: The columns to search.
        :param keyword: The keyword to search for.
        :param where: Additional conditions using regular filters.
        Example:
            table.count_search(
                by=['name', 'email'],
                keyword='john',
                where={'status': 'active', 'age': Filters.Between(18, 30)}
            )
        """
        try:
            if not by:
                raise ValueError("No columns provided for search")
            
            # Start parameter index at 1
            param_index = 1
            
            # Create the WHERE clause for the search columns with proper parameter index
            search_clause = " OR ".join(f"{column}::text ILIKE ${param_index}" for column in by)
            query_values = [f"%{keyword}%"]
            
            # Handle additional where conditions
            if where:
                where_clause, where_params = await self._build_where_clause(where)
                
                # Replace the generic parameter placeholders with properly indexed ones
                param_parts = []
                for i, part in enumerate(where_clause.split('$')):
                    if i == 0:  # First part has no $ prefix
                        param_parts.append(part)
                    elif part.strip():  # For parts that have content
                        # Extract the parameter number and the rest of the string
                        num_end = 0
                        while num_end < len(part) and part[num_end].isdigit():
                            num_end += 1
                        if num_end > 0:
                            new_index = param_index + int(part[:num_end])
                            param_parts.append(f"{new_index}{part[num_end:]}")
                
                # Rebuild the where clause with adjusted parameter indices
                adjusted_where_clause = '$'.join(param_parts)
                search_clause = f"({search_clause}) AND ({adjusted_where_clause})"
                query_values.extend(where_params)

            query = f"SELECT COUNT(*) FROM {self.name} WHERE {search_clause}"

            connection = await self._get_connection()
            await self.ensure_connection_available(connection)
            count = await connection.fetchval(query, *query_values, timeout=self.timeout)
            return count or 0

        except asyncpg.PostgresError as e:
            print(f"Failed to count search results in table {self.name}: {e}")
            return None
        except ValueError as e:
            print(f"ValueError: {e}")
            return None
        except Exception as e:
            print(traceback.format_exc())
            return None
        finally:
            if connection and isinstance(self.connection.connection, asyncpg.pool.Pool):
                await connection.release_connection()

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
                await connection.release_connection()

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
                await connection.release_connection()

    def __repr__(self) -> str:
        return f"<Table {self.name}"

    def __str__(self) -> str:
        return f"Table(name={self.name}, columns={self.columns})"

    def __getitem__(self, key: str) -> Optional[Column]:
        """
        Gets a column by its name.

        :param key: The name of the column.
        :return: The Column object if found, otherwise None.
        """
        for column in self.columns:
            if (column.name == key):
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
                await connection.release_connection()

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
                await connection.release_connection()

