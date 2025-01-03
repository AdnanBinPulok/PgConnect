import asyncpg
import time


class Connection:
    connection:asyncpg.Connection = None
    def __init__(
            self,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
            ssl: bool = False,
            pool: int = None,
            reconnect: bool = False
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.ssl = ssl
        self.pool = pool
        self.reconnect = reconnect


    async def get_connection(self):
        if not self.connection:
            await self.connect()
        if isinstance(self.connection, asyncpg.pool.Pool):
            return await self.connection.acquire()
        else:
            return self.connection
        

    async def ping(self) -> float:
        """
        Check the connection to the database
        :return: The time taken to ping the database in milliseconds
        """
        start_time = time.time_ns()
        connection = await self.get_connection()
        await connection.fetchval("SELECT 1")
        if isinstance(self.connection, asyncpg.pool.Pool):
            await connection.close()
        end_time = time.time_ns()
        return (end_time - start_time) / 1000000

    async def connect(self):
        try:
            if self.pool:
                connection = await asyncpg.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    ssl=self.ssl,
                    max_size=self.pool
                )
            else:
                connection = await asyncpg.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    ssl=self.ssl
                )
            self.connection = connection
        except Exception as e:
            raise ConnectionError("Could not connect to the database") from e
    
    async def acquire(self):
        if isinstance(self.connection, asyncpg.pool.Pool):
            return self.connection.acquire()
        else:
            return self.connection
    
    async def release(self, connection):
        """
        Release a connection back to the pool
        """
        if isinstance(self.connection, asyncpg.pool.Pool):
            await connection.close()

    async def close(self):
        """
        Close the connection to the database
        """
        if isinstance(self.connection, asyncpg.pool.Pool):
            await self.connection.close()
        else:
            await self.connection.close()
        self.connection = None
        return True
    