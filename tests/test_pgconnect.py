import pytest
import pgconnect
import asyncio
import os

@pytest.mark.asyncio
async def test_insert():
    connection = pgconnect.Connection(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
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

    await users.delete({"email": "test@example.com"})
    
    await users.create()
    result = await users.insert(
        email="test@example.com",
        username="testuser",
        password="password"
    )
    assert result is not None
    assert result['email'] == "test@example.com"
    assert result['username'] == "testuser"
    assert result['password'] == "password"