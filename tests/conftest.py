import os
from collections.abc import AsyncGenerator

import pytest
from pg_purepy import PooledDatabaseInterface, open_pool

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "postgres")


@pytest.fixture
async def postgresql() -> AsyncGenerator[PooledDatabaseInterface, None]:  # noqa: D103
    async with open_pool(
        POSTGRES_HOST,
        POSTGRES_USER,
        port=POSTGRES_PORT,
        password=POSTGRES_PASSWORD,
        connection_count=1,
        database=POSTGRES_DB,
    ) as pool:
        yield pool
