"""Simple PostgreSQL connection pool library."""

from ezpg.async_pool import (
    AsyncDatabasePool,
    close_async_database,
    get_async_db_pool,
    init_async_database,
)
from ezpg.sync_pool import (
    SyncDatabasePool,
    close_sync_database,
    get_sync_db_pool,
    init_sync_database,
)

__all__ = [
    # Async
    "AsyncDatabasePool",
    "get_async_db_pool",
    "init_async_database",
    "close_async_database",
    # Sync
    "SyncDatabasePool",
    "get_sync_db_pool",
    "init_sync_database",
    "close_sync_database",
]
