# PgConnect

PgConnect is a PostgreSQL connection and ORM library for Python. It provides an easy-to-use interface for connecting to PostgreSQL databases and performing common database operations.

## Features

- Easy connection to PostgreSQL databases
- ORM-like interface for defining and interacting with database tables
- Support for various PostgreSQL data types
- Caching mechanism for improved performance

## Installation

You can install PgConnect using pip:

```bash
pip install git+https://github.com/AdnanBinPulok/PgConnect.git
```

## Usage

### Connecting to the Database

```python
import pgconnect
import asyncio

async def main():
    connection = pgconnect.Connection(
        host="your_host",
        port=5432,
        user="your_user",
        password="your_password",
        database="your_database"
    )

    # Define your table schema
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

if __name__ == "__main__":
    asyncio.run(main())
```

### Inserting Data

```python
await users.insert(
    email="example@gmail.com",
    username="example",
    password="password"
)
```

Here, this is inserting a new row into the `users` table with the `email` as `example@gmail.com`, `username` as `example`, and `password` as `password`.

### Selecting Data

```python
user = await users.select("id", "username", email="example@gmail.com")
print(user)
```

Here, this is selecting the `id` and `username` columns for the row where the `email` is `example@gmail.com`.

### Updating Data

```python
await users.update({"id": 1}, username="new_username")
```

Here, this is updating the `username` column to `new_username` for the row where the `id` is `1`.

### Deleting Data

```python
await users.delete(id=1)
```

Here, this is deleting the row from the `users` table where the `id` is `1`.

### Counting Rows

```python
user_count = await users.count()
print(user_count)
```

Here, this is counting the total number of rows in the `users` table.

### Checking Existence

```python
user_exists = await users.exists(id=1)
print(user_exists)
```

Here, this is checking if there is any row in the `users` table where the `id` is `1`.

### Getting Columns

```python
columns = await users.get_columns()
print(columns)
```

Here, this is retrieving the names and data types of all columns in the `users` table.

### Dropping the Table

```python
await users.drop()
```

Here, this is dropping the `users` table from the PostgreSQL database.

### Truncating the Table

```python
await users.truncate()
```

Here, this is truncating the `users` table, which removes all rows from the table.

### Example Usage in a Script

Here is a complete example script demonstrating how to use these methods:

```python
import pgconnect
import asyncio

async def main():
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
    print("Table created")

    # Insert data
    await users.insert(
        email="example@gmail.com",
        username="example",
        password="password"
    )
    print("Data inserted")

    # Select data
    user = await users.select("id", "username", email="example@gmail.com")
    print("Selected user:", user)

    # Update data
    await users.update({"id": 1}, username="new_username")
    print("Data updated")

    # Delete data
    await users.delete(id=1)
    print("Data deleted")

    # Count rows
    user_count = await users.count()
    print("User count:", user_count)

    # Check existence
    user_exists = await users.exists(id=1)
    print("User exists:", user_exists)

    # Get columns
    columns = await users.get_columns()
    print("Table columns:", columns)

    # Drop table
    await users.drop()
    print("Table dropped")

    # Truncate table
    await users.truncate()
    print("Table truncated")

if __name__ == "__main__":
    asyncio.run(main())
```

This script demonstrates how to use the `Table` class to perform various database operations, including creating a table, inserting, updating, deleting, selecting data, counting rows, checking existence, getting columns, dropping the table, and truncating the table.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

AdnanBinPulok - [GitHub](https://github.com/AdnanBinPulok)
