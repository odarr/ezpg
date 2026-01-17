"""Sync database connection and pool management using psycopg3."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg import Connection
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class SyncDatabasePool:
    """Manages a sync connection pool to PostgreSQL database using psycopg3."""

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
        """Initialize sync database connection pool.

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
        self._pool: ConnectionPool | None = None

    def initialize(self) -> None:
        """Initialize the sync connection pool."""
        try:
            conninfo = (
                f"host={self.host} port={self.port} dbname={self.database} "
                f"user={self.user} password={self.password}"
            )
            self._pool = ConnectionPool(
                conninfo=conninfo,
                min_size=self.min_connections,
                max_size=self.max_connections,
                open=True,
            )
            logger.info(
                f"Sync database pool initialized for {self.database} "
                f"at {self.host}:{self.port}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize sync database pool: {e}")
            raise

    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.close()
            logger.info("Sync database pool closed")

    @contextmanager
    def acquire(self) -> Iterator[Connection]:
        """Acquire a connection from the pool.

        Yields:
            A database connection that will be returned to the pool

        Raises:
            RuntimeError: If pool is not initialized
        """
        if not self._pool:
            raise RuntimeError("Sync database pool not initialized")

        with self._pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Execute database operations in a transaction.

        Yields:
            A database connection within a transaction context

        Example:
            with db_pool.transaction() as conn:
                conn.execute("INSERT INTO ...")
                conn.execute("UPDATE ...")
                # Automatically commits on success, rolls back on exception
        """
        with self.acquire() as conn:
            with conn.transaction():
                yield conn

    def execute(self, query: str, *args, timeout: float | None = None) -> str:
        """Execute a command (INSERT/UPDATE/DELETE).

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds (not supported in psycopg3 pool)

        Returns:
            Status string from the query
        """
        with self.acquire() as conn:
            cur = conn.execute(query, args if args else None)
            return cur.statusmessage or ""

    def executemany(
        self, query: str, args: list[tuple], timeout: float | None = None
    ) -> None:
        """Execute a command multiple times.

        Args:
            query: SQL query to execute
            args: List of parameter tuples
            timeout: Query timeout in seconds (not supported in psycopg3 pool)
        """
        with self.acquire() as conn:
            cur = conn.cursor()
            cur.executemany(query, args)

    def fetch(
        self, query: str, *args, timeout: float | None = None
    ) -> list[tuple]:
        """Execute a query and return all rows.

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds (not supported in psycopg3 pool)

        Returns:
            List of rows as tuples
        """
        with self.acquire() as conn:
            cur = conn.execute(query, args if args else None)
            return cur.fetchall()

    def fetchrow(
        self, query: str, *args, timeout: float | None = None
    ) -> tuple | None:
        """Execute a query and return a single row.

        Args:
            query: SQL query to execute
            *args: Query parameters
            timeout: Query timeout in seconds (not supported in psycopg3 pool)

        Returns:
            Single row or None
        """
        with self.acquire() as conn:
            cur = conn.execute(query, args if args else None)
            return cur.fetchone()

    def fetchval(
        self, query: str, *args, column: int = 0, timeout: float | None = None
    ) -> Any:
        """Execute a query and return a single value.

        Args:
            query: SQL query to execute
            *args: Query parameters
            column: Column index to return
            timeout: Query timeout in seconds (not supported in psycopg3 pool)

        Returns:
            Single value from the query result
        """
        with self.acquire() as conn:
            cur = conn.execute(query, args if args else None)
            row = cur.fetchone()
            if row is None:
                return None
            return row[column]


# Global sync database pool instance
_sync_db_pool: SyncDatabasePool | None = None


def get_sync_db_pool() -> SyncDatabasePool:
    """Get the global sync database pool instance.

    Returns:
        The sync database pool instance

    Raises:
        RuntimeError: If sync database pool is not initialized
    """
    if not _sync_db_pool:
        raise RuntimeError(
            "Sync database pool not initialized. Call init_sync_database first."
        )
    return _sync_db_pool


def init_sync_database(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    min_connections: int = 1,
    max_connections: int = 10,
) -> None:
    """Initialize the global sync database pool.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        min_connections: Minimum pool size
        max_connections: Maximum pool size
    """
    global _sync_db_pool
    _sync_db_pool = SyncDatabasePool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_connections=min_connections,
        max_connections=max_connections,
    )
    _sync_db_pool.initialize()


def close_sync_database() -> None:
    """Close the global sync database pool."""
    global _sync_db_pool
    if _sync_db_pool:
        _sync_db_pool.close()
        _sync_db_pool = None
