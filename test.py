import pgconnect
import asyncio




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
        columns={
            "id": pgconnect.Column(
                type=pgconnect.DataType.SERIAL,
                primary_key=True
            ),
            "username": pgconnect.Column(
                type=pgconnect.DataType.VARCHAR,
                not_null=True
            ),
            "password": pgconnect.Column(
                type=pgconnect.DataType.VARCHAR,
                length=255,
                not_null=True
            )
        }
    )
    await users.create()
    print(users)

if __name__ == "__main__":
    asyncio.run(main())