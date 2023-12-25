import contextlib
from collections.abc import AsyncGenerator

import pytest
from infipod.engine import IsopodPool, SchemaLoadFailedError, spawn_isopods_from_pool
from infipod.schema import Column, TableSchema
from infipod.types import Int4Type, TextType
from pg_purepy import MissingRowError
from pg_purepy.pool import PooledDatabaseInterface
from trio.testing import RaisesGroup

pytestmark = pytest.mark.anyio


class ExampleTable(TableSchema):
    id = Column(Int4Type())
    field = Column(TextType())


@pytest.fixture(scope="function")
async def engine(postgresql: PooledDatabaseInterface) -> AsyncGenerator[IsopodPool, None]:
    await postgresql.execute(
        """
        CREATE TEMPORARY TABLE example_table (
            id INT4 PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            field TEXT NOT NULL
        );
        """
    )

    yield await spawn_isopods_from_pool(postgresql, [ExampleTable])


async def test_basic_engine_connection(postgresql: PooledDatabaseInterface):
    engine = await spawn_isopods_from_pool(postgresql, [])
    res = await engine.fetch_one("SELECT 1;")
    assert res[0] == 1


async def test_loading_engine_without_existing_table(postgresql: PooledDatabaseInterface):
    with RaisesGroup(SchemaLoadFailedError):
        await spawn_isopods_from_pool(postgresql, [ExampleTable])


async def test_loading_non_strict(postgresql: PooledDatabaseInterface):
    await spawn_isopods_from_pool(postgresql, [ExampleTable], strict_schema_loading=False)


async def test_missing_column_server_side(postgresql: PooledDatabaseInterface):
    await postgresql.execute(
        "CREATE TEMPORARY TABLE example_table (id INT4 GENERATED ALWAYS AS IDENTITY);"
    )

    with RaisesGroup(SchemaLoadFailedError):
        await spawn_isopods_from_pool(postgresql, [ExampleTable])


async def test_missing_column_client_side(postgresql: PooledDatabaseInterface):
    await postgresql.execute(
        "CREATE TEMPORARY TABLE example_table (id INT4 PRIMARY KEY, field TEXT, missing TEXT)"
    )

    with RaisesGroup(SchemaLoadFailedError):
        await spawn_isopods_from_pool(postgresql, [ExampleTable])


async def test_result_out_of_bounds(postgresql: PooledDatabaseInterface):
    engine = await spawn_isopods_from_pool(postgresql, [])
    res = await engine.fetch_one("SELECT 1;")

    # KeyError is suppressed, but out-of-bounds indexes mustn't be.
    with pytest.raises(IndexError):
        assert res.get(1)


async def test_indexing_using_column(engine: IsopodPool):
    await engine.execute("INSERT INTO example_table (field) VALUES ($1);", "05lifecut")
    row = await engine.fetch_one("SELECT id, field FROM example_table;")
    assert row[ExampleTable.id] >= 0
    assert row[ExampleTable.field] == "05lifecut"


async def test_indexing_non_existent_column(engine: IsopodPool):
    await engine.execute("INSERT INTO example_table (field) VALUES ($1);", "05lifecut")
    row = await engine.fetch_one("SELECT id FROM example_table;")

    with pytest.raises(KeyError):
        row[ExampleTable.field]


async def test_indexing_using_index(engine: IsopodPool):
    await engine.execute("INSERT INTO example_table (field) VALUES ($1);", "04kamuidrone")
    row = await engine.fetch_one("SELECT id, field FROM example_table;")
    assert row[0] >= 0
    assert row[1] == "04kamuidrone"


async def test_checking_out_connection(engine: IsopodPool):
    # ensures the checked out connection is actually in a transaction

    with contextlib.suppress(ValueError):
        async with engine.checkout_isopod() as isopod:
            await isopod.execute("INSERT INTO example_table (field) VALUES ('test');")
            raise ValueError

    with pytest.raises(MissingRowError):
        await engine.fetch_one("SELECT * FROM example_table;")
