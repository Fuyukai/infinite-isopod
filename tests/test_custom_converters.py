
from collections.abc import AsyncGenerator
from enum import Enum

import pytest
from infipod.engine import IsopodPool, spawn_isopods_from_pool
from infipod.schema import Column, TableSchema
from infipod.types import EnumType, Int4Type
from pg_purepy import PooledDatabaseInterface

pytestmark = pytest.mark.anyio


class ExampleEnum(Enum):
    ONE = 1
    TWO = 2
    

class ExampleTable(TableSchema):
    id = Column(Int4Type())
    field = Column(EnumType(ExampleEnum, postgresql_name="example_enum"))
    

@pytest.fixture(scope="function")
async def engine(postgresql: PooledDatabaseInterface) -> AsyncGenerator[IsopodPool, None]:
    await postgresql.execute("CREATE TYPE example_enum AS ENUM ('ONE', 'TWO');")
    await postgresql.execute(
        """
        CREATE TEMPORARY TABLE example_table (
            id INT4 PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            field example_enum NOT NULL
        );
        """
    )

    try:
        yield await spawn_isopods_from_pool(postgresql, [ExampleTable])
    finally:
        await postgresql.execute("DROP TABLE example_table;")
        await postgresql.execute("DROP TYPE example_enum;")

async def test_loading_with_enum_converter(engine: IsopodPool):
    await engine.execute("INSERT INTO example_table (field) VALUES ('ONE'::example_enum);")
    row = await engine.fetch_one("SELECT field FROM example_table;")
    assert row[ExampleTable.field] == ExampleEnum.ONE
