from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable, Mapping
from contextlib import asynccontextmanager
from os import PathLike
from ssl import SSLContext
from typing import Any, cast, overload

import attr
import pg_purepy
import structlog
from pg_purepy.messages import DataRow

from infipod.schema import Column, TableSchema
from infipod.types import DatabaseTypeWithConverter
from infipod.util import map_as_completed

logger: structlog.stdlib.BoundLogger = structlog.get_logger(name=__name__)

type SchemaMapping = Mapping[type[TableSchema], TableSchema]


class SchemaLoadFailedError(Exception):
    """
    Raised when loading schemas from the database fails.
    """


class IsopodPool:
    """
    A "pool" of :class:`.Isopod`.
    """

    def __init__(  # noqa: D107
        self,
        pool: pg_purepy.PooledDatabaseInterface,
        schemas: SchemaMapping,
    ) -> None:
        self._connection = pool
        self._schemas = schemas

    @asynccontextmanager
    async def checkout_isopod(self) -> AsyncGenerator[Isopod, None]:
        """
        Checks out a single :class:`.Isopod` in a transaction.

        If you don't need a transaction, see :meth:`.IsopodPool.execute`,
        :meth:`.IsopodPool.fetch`, and :meth:`.IsopodPool.fetch_one`.
        """

        async with self._connection.checkout_in_transaction() as conn:
            yield Isopod(connection=conn, pool=self)

    async def execute(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        """
        Executes a single query and returns a row count. This follows the same argument format
        as :func:`.PooledDatabaseInterface.execute`.
        """

        return await self._connection.execute(query, *args, **kwargs)

    async def fetch(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> list[EnhancedRow]:
        """
        Executes a query and fetches a list of :class:`.EnhancedRow` instances.
        """

        return [
            EnhancedRow(self, it) for it in await self._connection.fetch(query, *args, **kwargs)
        ]

    async def fetch_one(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> EnhancedRow:
        """
        Executes a query and fetches a single :class:`.EnhancedRow` instance.
        """

        return EnhancedRow(self, await self._connection.fetch_one(query, *args, **kwargs))


@attr.s(slots=True, kw_only=True)
class Isopod:
    """
    A single isopod wrapping a checked out connection in a transaction.
    """

    connection: pg_purepy.AsyncPostgresConnection = attr.ib()
    pool: IsopodPool = attr.ib()

    async def execute(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        """
        Executes a single query and returns a row count. This follows the same argument format
        as :func:`.PooledDatabaseInterface.execute`.
        """

        return await self.connection.execute(query, *args, **kwargs)

    async def fetch(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> list[EnhancedRow]:
        """
        Executes a query and fetches a list of :class:`.EnhancedRow` instances.
        """

        return [
            EnhancedRow(self.pool, it) for it in await self.connection.fetch(query, *args, **kwargs)
        ]

    async def fetch_one(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> EnhancedRow:
        """
        Executes a query and fetches a single :class:`.EnhancedRow` instance.
        """

        return EnhancedRow(self.pool, await self.connection.fetch_one(query, *args, **kwargs))


class EnhancedRow:
    """
    A wrapper around a :class:`.DataRow` that allows looking values up by column.
    """

    def __init__(self, engine: IsopodPool, row: DataRow) -> None:  # noqa: D107
        self._engine = engine
        self._row = row

    @overload
    def __getitem__(self, key: int, /) -> Any: ...

    @overload
    def __getitem__[T](self, key: Column[T], /) -> T: ...

    def __getitem__[T](self, key: Column[T] | int, /) -> T:
        if isinstance(key, int):
            return cast(Any, self._row.data[key])

        table_type = key.owner
        table_instance = self._engine._schemas[table_type]
        column_idx = table_instance.column_mapping[key]

        for idx, desc in enumerate(self._row.description.columns):
            if desc.table_oid == table_instance.oid and desc.column_index == column_idx:
                data = self._row.data[idx]
                if data is None:
                    raise KeyError(f"Column {key.name} has a NULL value")

                return cast(T, self._row.data[idx])  # type: ignore  # pylance fix

        raise KeyError(f"No such column {key.name} in this row")

    @overload
    def get(self, key: int, /) -> Any | None: ...

    @overload
    def get[T](self, key: Column[T], /) -> T | None: ...

    def get[T](self, key: Column[T] | int, /) -> T | None:
        """
        Gets the value in this result associated with the specified :class:`.Column` ``key`` or
        :class:`int` index.

        :returns: The value, or ``None`` if there is no such value in this row.
        """

        try:
            return self[key]
        except KeyError:
            return None


async def create_table_schemas(
    conn: pg_purepy.PooledDatabaseInterface,
    schema_types: Iterable[type[TableSchema]],
    strict: bool = True,
) -> tuple[SchemaMapping, Iterable[pg_purepy.Converter]]:
    """
    Loads internal PostgreSQL schema data from the database.

    :param conn: The :class:`.PooledDatabaseInterface` to load the schema data for.
    :param schema_types: An iterable of :class:`.TableSchema` type instances to process.
    :param strict: If True, missing data will raise an error instead
    """

    async def backfill_table_data(table: type[TableSchema]) -> TableSchema | None:
        # the "easy" way of doing this is to_regtype($1)::regtype::oid
        # but, sadly, no.

        try:
            oid_row = await conn.fetch_one(
                "SELECT oid FROM pg_class WHERE relname = $1 AND relkind = 'r';",
                table.table_name,
            )
        except pg_purepy.MissingRowError as e:
            if strict:
                raise SchemaLoadFailedError(f"Unknown table {table.table_name}") from e

            logger.warning("Missing table", table=table.table_name)
            return None

        oid: int = cast(int, oid_row.data[0])
        schema = table(table_oid=oid)

        logger.debug("Loaded table OID", oid=oid, table=table.table_name)

        # the confusingly named ``pg_attribute`` table stores all of the actual columns...
        # attnum: The number of the column. Ordinary columns are numbered from 1 up.
        #         System columns, such as ctid, have (arbitrary) negative numbers.

        column_rows = await conn.fetch(
            """
            SELECT * FROM pg_attribute
            JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
            WHERE pg_class.oid = $1 AND pg_attribute.attnum > 0 
            ORDER BY pg_attribute.attnum;
            """,
            oid,
        )

        for row in column_rows:
            column_row_data = row.to_dict()
            column_name = cast(str, column_row_data["attname"])

            if not (column := table.all_columns.get(column_name)):
                if strict:
                    raise SchemaLoadFailedError(
                        f"Missing column in schema: {table.table_name}.{column_name}"
                    )

                logger.warning(
                    "Missing column in schema", column=column_name, table=table.table_name
                )
                continue

            column_oid = column_row_data["attnum"]
            schema.column_mapping[column] = cast(int, column_oid)
            logger.debug(
                "Loaded column index",
                column_name=column.name,
                table=column.owner.table_name,
                ordinal=column_oid,
            )

        if strict:
            diff = set(table.all_columns.values()).difference(schema.column_mapping.keys())
            if diff:
                unmapped = ", ".join(i.name for i in diff)
                raise SchemaLoadFailedError(f"Unmapped columns: {unmapped}")

        return schema

    schemas = list(await map_as_completed(schema_types, backfill_table_data))
    schemas = [i for i in schemas if i is not None]

    # poor mans set, keyed by type ID
    converters: dict[str, pg_purepy.Converter] = {}
    for schema in schemas:
        for column in schema.all_columns.values():
            schema_type = column.type
            if not isinstance(schema_type, DatabaseTypeWithConverter):
                continue

            try:
                oid_row = await conn.fetch_one(
                    "SELECT oid FROM pg_type WHERE typname = $1;", schema_type.postgresql_name
                )
                oid = cast(int, oid_row.data[0])
            except pg_purepy.MissingRowError as e:
                if strict:
                    raise SchemaLoadFailedError(
                        f"Can't find OID for type {schema_type.postgresql_name}"
                    ) from e

                logger.warning(
                    "Unknown type",
                    column=column.name,
                    type=schema_type.postgresql_name,
                    table=schema.table_name,
                )
                continue

            converters[schema_type.postgresql_name] = schema_type.create_converter(oid)

    return ({type(it): it for it in schemas}, converters.values())


@asynccontextmanager
async def spawn_isopods(
    address_or_path: str | PathLike[str],
    username: str,
    tables: Iterable[type[TableSchema]],
    *,
    connection_count: int | None = None,
    port: int = 5432,
    password: str | None = None,
    database: str | None = None,
    ssl_context: SSLContext | None = None,
    strict_schema_loading: bool = True,
) -> AsyncGenerator[IsopodPool, None]:
    """
    Opens a pooled connection to the PostgreSQL server and yields a :class:`.IsopodPool` ORM engine.

    Required parameters:

    :param address_or_path: The address of the server or the *absolute path* of its Unix socket.
    :param username: The username to authenticate with.
    :param tables: A list of :class:`.TableSchema` types that are going to be used with the ORM.
        This will immediately load their metadata from PostgreSQL's information schemas and backfill
        the data.

    Optional parameters:

    :param port: The port to connect to. Ignored for unix sockets.
    :param password: The password to authenticate with.
    :param database: The database to connect to. Defaults to the username.
    :param ssl_context: The SSL context to use for TLS connection. Enables TLS if specified.
    :param strict_schema_loading: If True, then tables and/or columns that are not found on the
        server-side will cause an error. If False, then they will merely cause a warning.

        For nearly all normal usages, you want to have this set to True (the default). There are
        some cases, however, where the tables won't exist; for example, when running a database
        migration.
    """

    async with pg_purepy.open_pool(
        address_or_path,
        username,
        connection_count=connection_count,
        port=port,
        password=password,
        database=database,
        ssl_context=ssl_context,
    ) as pool:
        schemas, converters = await create_table_schemas(pool, tables, strict=strict_schema_loading)

        for converter in converters:
            pool.add_converter(converter)

        yield IsopodPool(pool, schemas)


async def spawn_isopods_from_pool(
    pool: pg_purepy.PooledDatabaseInterface,
    tables: Iterable[type[TableSchema]],
    strict_schema_loading: bool = True,
) -> IsopodPool:
    """
    Creates a new :class:`.IsopodPool` engine using an existing :class:`.PooledDatabaseInterface`.

    :param pool: The connected PostgreSQL connection pool to use for issuing commands.
    :param tables: A list of :class:`.TableSchema` types that are going to be used with the ORM.
        This will immediately load their metadata from PostgreSQL's information schemas and backfill
        the data.

    Optional parameters:

    :param strict_schema_loading: If True, then tables and/or columns that are not found on the
        server-side will cause an error. If False, then they will merely cause a warning.

        For nearly all normal usages, you want to have this set to True (the default). There are
        some cases, however, where the tables won't exist; for example, when running a database
        migration.
    """

    schemas, converters = await create_table_schemas(pool, tables, strict=strict_schema_loading)

    for converter in converters:
        pool.add_converter(converter)

    return IsopodPool(pool, schemas)
