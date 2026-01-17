"""Test that AsyncDatabasePool and SyncDatabasePool have matching interfaces."""

import inspect

import pytest
from ezpg.pool import AsyncDatabasePool
from ezpg.sync_pool import SyncDatabasePool

# Methods that should exist on both classes with matching signatures
SHARED_METHODS = [
    "__init__",
    "initialize",
    "close",
    "acquire",
    "transaction",
    "execute",
    "executemany",
    "fetch",
    "fetchrow",
    "fetchval",
]

# Methods that are async-only (no sync equivalent expected)
ASYNC_ONLY_METHODS = [
    "create_listener",  # LISTEN/NOTIFY is inherently async
]


class TestInterfaceParity:
    """Verify that async and sync pool classes have matching interfaces."""

    def test_shared_methods_exist_on_both(self):
        """Both classes should have all shared methods."""
        async_methods = set(dir(AsyncDatabasePool))
        sync_methods = set(dir(SyncDatabasePool))

        for method in SHARED_METHODS:
            assert method in async_methods, f"AsyncDatabasePool missing {method}"
            assert method in sync_methods, f"SyncDatabasePool missing {method}"

    def test_init_parameters_match(self):
        """__init__ should have the same parameters."""
        async_sig = inspect.signature(AsyncDatabasePool.__init__)
        sync_sig = inspect.signature(SyncDatabasePool.__init__)

        async_params = list(async_sig.parameters.keys())
        sync_params = list(sync_sig.parameters.keys())

        assert async_params == sync_params, (
            f"__init__ parameters differ:\n  Async: {async_params}\n  Sync:  {sync_params}"
        )

    @pytest.mark.parametrize("method_name", SHARED_METHODS[1:])  # Skip __init__
    def test_method_parameters_match(self, method_name: str):
        """Method parameters should match (excluding self)."""
        async_method = getattr(AsyncDatabasePool, method_name)
        sync_method = getattr(SyncDatabasePool, method_name)

        async_sig = inspect.signature(async_method)
        sync_sig = inspect.signature(sync_method)

        async_params = list(async_sig.parameters.keys())
        sync_params = list(sync_sig.parameters.keys())

        assert async_params == sync_params, (
            f"{method_name} parameters differ:\n  Async: {async_params}\n  Sync:  {sync_params}"
        )

    def test_no_unexpected_public_methods_on_sync(self):
        """SyncDatabasePool shouldn't have public methods that Async doesn't have."""
        async_methods = {m for m in dir(AsyncDatabasePool) if not m.startswith("_")}
        sync_methods = {m for m in dir(SyncDatabasePool) if not m.startswith("_")}

        # Remove async-only methods from comparison
        async_methods -= set(ASYNC_ONLY_METHODS)

        extra_sync = sync_methods - async_methods
        assert not extra_sync, f"SyncDatabasePool has extra methods: {extra_sync}"

    def test_async_only_methods_documented(self):
        """Async-only methods should actually exist on AsyncDatabasePool."""
        for method in ASYNC_ONLY_METHODS:
            assert hasattr(AsyncDatabasePool, method), (
                f"ASYNC_ONLY_METHODS lists {method} but it doesn't exist"
            )
            assert not hasattr(SyncDatabasePool, method), (
                f"{method} exists on SyncDatabasePool but is listed as async-only"
            )
