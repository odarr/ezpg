"""Async database connection and pool management using asyncpg."""

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import asyncpg_listen

logger = logging.getLogger(__name__)


class AsyncDatabasePool:
    """Manages an async connection pool to PostgreSQL database using asyncpg."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_connections: int = 1,
        max_connections: int = 10,
    ):
        """Initialize async database connection pool.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            min_connections: Minimum pool size
            max_connections: Maximum pool size
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_connections = min_connections
        self.max_connections = max_connections
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        """Initialize the async connection pool."""
        try:
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=self.min_connections,
                max_size=self.max_connections,
                command_timeout=60,
                init=self._setup_connection,
            )
            logger.info(
                f"Async database pool initialized for {self.database} "
                f"at {self.host}:{self.port}"
            )
        except asyncpg.PostgresError as e:
            logger.error(f"Failed to initialize async database pool: {e}")
            raise

    async def _setup_connection(self, conn: asyncpg.Connection) -> None:
        """Set up each connection with custom type codecs.

        This is called for each connection in the pool when it's created.
        """
        # Set up JSON codec for automatic JSON/JSONB handling
        await conn.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    async def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Async database pool closed")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a connection from the pool.

        Yields:
            An async database connection that will be returned to the pool

        Raises:
            RuntimeError: If pool is not initialized
            asyncpg.PostgresError: On database errors
        """
        if not self._pool:
            raise RuntimeError("Async database pool not initialized")

        async with self._pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        """Execute database operations in a transaction.

        Yields:
            An async database connection within a transaction context

        Example:
            async with db_pool.transaction() as conn:
                await conn.execute("INSERT INTO ...")
                await conn.execute("UPDATE ...")
                # Automatically commits on success, rolls back on exception
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def execute(
        self, query: str, *args, timeout: float | None = None
    ) -> str:
        """Execute a command (INSERT/UPDATE/DELETE).

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            Status string from the query
        """
        async with self.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout or 60.0)

    async def executemany(
        self, query: str, args: list[tuple], timeout: float | None = None
    ) -> None:
        """Execute a command multiple times.

        Args:
            query: SQL query to execute
            args: List of parameter tuples
            timeout: Query timeout in seconds
        """
        async with self.acquire() as conn:
            await conn.executemany(query, args, timeout=timeout or 60.0)

    async def fetch(
        self, query: str, *args, timeout: float | None = None
    ) -> list[asyncpg.Record]:
        """Execute a query and return all rows.

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            List of records
        """
        async with self.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)

    async def fetchrow(
        self, query: str, *args, timeout: float | None = None
    ) -> asyncpg.Record | None:
        """Execute a query and return a single row.

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            Single record or None
        """
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)

    async def fetchval(
        self, query: str, *args, column: int = 0, timeout: float | None = None
    ) -> Any:
        """Execute a query and return a single value.

        Args:
            query: SQL query to execute
            *args: Query parameters
            column: Column index to return
            timeout: Query timeout in seconds

        Returns:
            Single value from the query result
        """
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args, column=column, timeout=timeout)

    def create_listener(self) -> asyncpg_listen.NotificationListener:
        """Create a notification listener for LISTEN/NOTIFY.

        Returns:
            A configured NotificationListener instance
        """
        return asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        )


# Global async database pool instance
_async_db_pool: AsyncDatabasePool | None = None


def get_async_db_pool() -> AsyncDatabasePool:
    """Get the global async database pool instance.

    Returns:
        The async database pool instance

    Raises:
        RuntimeError: If async database pool is not initialized
    """
    if not _async_db_pool:
        raise RuntimeError(
            "Async database pool not initialized. Call init_async_database first."
        )
    return _async_db_pool


async def init_async_database(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    min_connections: int = 1,
    max_connections: int = 10,
) -> None:
    """Initialize the global async database pool.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        min_connections: Minimum pool size
        max_connections: Maximum pool size
    """
    global _async_db_pool
    _async_db_pool = AsyncDatabasePool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_connections=min_connections,
        max_connections=max_connections,
    )
    await _async_db_pool.initialize()


async def close_async_database() -> None:
    """Close the global async database pool."""
    global _async_db_pool
    if _async_db_pool:
        await _async_db_pool.close()
        _async_db_pool = None
