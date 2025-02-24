import asyncpg
import time
import asyncio
from typing import Optional, Any

class Connection:
    connection: Optional[asyncpg.Connection] = None
    
    def __init__(
            self,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
            ssl: bool = False,
            pool: int = None,
            reconnect: bool = False,
            max_retries: int = 3,
            retry_delay: float = 1.0
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.ssl = ssl
        self.pool = pool
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._is_connected = False
        self._last_used = time.time()

    class ModifiedConnection:
        def __init__(self, connection, pool=None):
            self._connection = connection
            self._pool: Optional[asyncpg.pool.Pool] = pool

        def __getattr__(self, attr):
            return getattr(self._connection, attr)

        async def release_connection(self):
            """Release the connection back to the pool if using pooled connections"""
            if self._pool:
                await self._pool.release(self._connection)

    async def get_connection(self):
        """Get a connection from the pool or return the single connection"""
        if not self.connection:
            await self.connect()
        if isinstance(self.connection, asyncpg.pool.Pool):
            conn = await self.connection.acquire()
            return self.ModifiedConnection(conn, self.connection)
        return self.ModifiedConnection(self.connection)
    
    async def ping(self) -> float:
        """
        Check the connection to the database
        :return: The time taken to ping the database in milliseconds
        """
        start_time = time.time_ns()
        connection = await self.get_connection()
        await connection.fetchval("SELECT 1")
        if isinstance(self.connection, asyncpg.pool.Pool):
            await connection.release_connection()
        end_time = time.time_ns()
        return (end_time - start_time) / 1000000

    async def connect(self):
        for attempt in range(self.max_retries):
            try:
                if self.pool:
                    connection = await asyncpg.create_pool(
                        host=self.host,
                        port=self.port,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        ssl=self.ssl,
                        max_size=self.pool,
                        command_timeout=60
                    )
                else:
                    connection = await asyncpg.connect(
                        host=self.host,
                        port=self.port,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        ssl=self.ssl,
                        command_timeout=60
                    )
                self.connection = connection
                self._is_connected = True
                self._last_used = time.time()
                return
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise ConnectionError(f"Failed to connect after {self.max_retries} attempts: {str(e)}") from e
                await asyncio.sleep(self.retry_delay)

    async def ensure_connected(self):
        """Ensure the connection is active and reconnect if necessary."""
        if not self._is_connected or not self.connection:
            await self.connect()
        elif time.time() - self._last_used > 300:  # 5 minutes timeout
            try:
                await self.ping()
            except:
                await self.connect()

    async def transaction(self):
        """Start a new transaction."""
        connection = await self.get_connection()
        return connection.transaction()

    async def execute_transaction(self, queries: list[tuple[str, tuple]]) -> bool:
        """
        Execute multiple queries in a transaction.
        
        :param queries: List of tuples containing (query_string, parameters)
        :return: True if successful, False if failed
        """
        async with self.transaction() as tx:
            try:
                for query, params in queries:
                    await self.connection.execute(query, *params)
                return True
            except Exception as e:
                await tx.rollback()
                print(f"Transaction failed: {str(e)}")
                return False

    async def is_connected(self) -> bool:
        """Check if the connection is still valid."""
        try:
            await self.ping()
            return True
        except:
            return False

    def get_status(self) -> dict[str, Any]:
        """Get the current connection status."""
        return {
            "connected": self._is_connected,
            "pooled": isinstance(self.connection, asyncpg.pool.Pool),
            "last_used": self._last_used,
            "host": self.host,
            "database": self.database
        }

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
            await connection.release_connection()

    async def close(self):
        """Close the connection to the database"""
        try:
            if self.connection:
                    await self.connection.close()
            else:
                await self.connection.close()
            self.connection = None
            self._is_connected = False
            return True
        except Exception as e:
            print(f"Error closing connection: {str(e)}")
            print(f"Error closing connection: {str(e)}")
            return False
