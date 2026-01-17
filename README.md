# ezpg

Simple PostgreSQL connection pool library with matching async and sync interfaces.

## Installation

```bash
pip install ezpg
```

## Usage

### Async (asyncpg)

```python
from ezpg import init_async_database, get_async_db_pool, close_async_database

# Initialize
await init_async_database(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
)

# Use
pool = get_async_db_pool()
rows = await pool.fetch("SELECT * FROM users WHERE id = $1", user_id)

# Or use the pool directly
async with pool.acquire() as conn:
    await conn.execute("INSERT INTO users (name) VALUES ($1)", "alice")

# Transactions
async with pool.transaction() as conn:
    await conn.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, from_id)
    await conn.execute("UPDATE accounts SET balance = balance + $1 WHERE id = $2", 100, to_id)

# Cleanup
await close_async_database()
```

### Sync (psycopg3)

```python
from ezpg import init_sync_database, get_sync_db_pool, close_sync_database

# Initialize
init_sync_database(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
)

# Use
pool = get_sync_db_pool()
rows = pool.fetch("SELECT * FROM users WHERE id = %s", user_id)

# Cleanup
close_sync_database()
```

### Direct pool usage

```python
from ezpg import AsyncDatabasePool, SyncDatabasePool

# Create your own pool instance
pool = AsyncDatabasePool(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
    min_connections=2,
    max_connections=20,
)
await pool.initialize()

# Use it
rows = await pool.fetch("SELECT 1")

# Close when done
await pool.close()
```

## API

Both `AsyncDatabasePool` and `SyncDatabasePool` have the same interface:

- `initialize()` - Initialize the connection pool
- `close()` - Close all connections
- `acquire()` - Context manager to get a connection
- `transaction()` - Context manager for transactions
- `execute(query, *args)` - Execute a command
- `executemany(query, args)` - Execute a command multiple times
- `fetch(query, *args)` - Fetch all rows
- `fetchrow(query, *args)` - Fetch single row
- `fetchval(query, *args)` - Fetch single value

Async pool also has:
- `create_listener()` - Create a LISTEN/NOTIFY listener
