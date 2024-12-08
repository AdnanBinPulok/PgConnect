import pytest
import pgconnect
import asyncio

@pytest.mark.asyncio
async def test_insert():
    connection = pgconnect.Connection(
        host="your_host",
        port=5432,
        user="your_user",
        password="your_password",
        database="your_database"
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
    result = await users.insert(
        email="test@example.com",
        username="testuser",
        password="password"
    )
    assert result is not None
    assert result['email'] == "test@example.com"
    assert result['username'] == "testuser"
    assert result['password'] == "password"