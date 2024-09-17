import asyncpg
import time

SavedConnection = None

class connection_class:
    def __init__(self):
        self.connection = SavedConnection

    def get_connection(self):
        if type(self.connection) == asyncpg.pool.Pool:
            return self.connection.acquire()
        else:
            return self.connection

    async def ping(self) -> float:
        start_time = time.time_ns()
        connection = self.get_connection()
        await connection.fetchval("SELECT 1")
        if isinstance(self.connection, asyncpg.pool.Pool):
            await connection.close()
        end_time = time.time_ns()
        return (end_time - start_time) / 1000000



async def connect(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    ssl: bool = False,
    pool: int = None,
    reconnect: bool = False
):
    try:
        if pool:
            connection = await asyncpg.create_pool(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                ssl=ssl,
                max_size=pool
            )
        else:
            connection = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                ssl=ssl
            )
    except Exception as e:
        raise ConnectionError("Could not connect to the database") from e

    global SavedConnection
    SavedConnection = connection

    return connection_class()