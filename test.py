import pgconnect
import asyncio
import time

async def main():
    connection = pgconnect.Connection(
        host="192.168.0.103",
        port=5432,
        user="postgres",
        password="Pulok23@#",
        database="pgconnect"
    )

    users = pgconnect.Table(
        name="users",
        connection=connection,
        columns=[
            pgconnect.Column(
                name="id",
                type=pgconnect.DataType.SERIAL().primary_key().not_null()
            ),
            pgconnect.Column(
                name="email",
                type=pgconnect.DataType.VARCHAR().unique().not_null()
            ),
            pgconnect.Column(
                name="username",
                type=pgconnect.DataType.VARCHAR()
            ),
            pgconnect.Column(
                name="password",
                type=pgconnect.DataType.TEXT(),
            ),
            pgconnect.Column(
                name="created_at",
                type=pgconnect.DataType.TIMESTAMP().default("NOW()")
            )
        ],
        cache=True,
        cache_key="id",
    )
    
    await users.create()
    print(users)
    
    start_time = time.time_ns()
    user = await users.get(id=8)
    print(f"User: {user}")
    print(f"Without cache: {time.time_ns() - start_time} nanoseconds")

    # Measure time with cache
    start2_time = time.time_ns()
    user = await users.get(id=8)
    print(f"User: {user}")
    print(f"With cache: {time.time_ns() - start2_time} nanoseconds")

    print(users.caches)

if __name__ == "__main__":
    asyncio.run(main())