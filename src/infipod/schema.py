from __future__ import annotations

import builtins
from collections.abc import Mapping
from typing import Any, ClassVar, Self

import attr

from infipod.types import DatabaseType
from infipod.util import pascal_to_snake


@attr.s(slots=True, frozen=True)
class Column[ColumnType: Any]:
    """
    A single column in a :class:`.TableSchema`.

    Columns are instantiated as class variables on a table schema, like so::

        class User(TableSchema):
            id = Column(Int4Type(), primary_key=True)
    """

    #: The *schema type* of this column.
    type: DatabaseType[ColumnType] = attr.ib()

    # set by descriptors
    #: The owning :class:`.TableSchema` type for this column.
    owner: builtins.type[TableSchema] = attr.ib(init=False)

    #: The name of this column. If this is not explicitly passed, it will be set to the attribute
    #: name of the column on the class.
    name: str = attr.ib(kw_only=True, default="")

    def __set_name__(self, owner: builtins.type[Any], name: str) -> None:
        if not issubclass(owner, TableSchema):
            raise TypeError("Columns can't be used on non-TableSchema classes")

        object.__setattr__(self, "owner", owner)
        if not self.name:
            object.__setattr__(self, "name", name)


class _TableMeta(type):
    """
    Metaclass to allow easily passing certain arguments in :class:`.TableSchema` subclasses.

    This class should not be used directly as a metaclass; inherit from :class:`.TableSchema`
    instead, which provides instance methods required for the isopods to function properly.
    """

    def __init__(
        mcs,
        name: str,
        bases: tuple[type],
        klass_body: dict[str, Any],
        table_name: str | None = None,
    ) -> None:
        super().__init__(name, bases, klass_body)

        if name != "TableSchema" and "table_name" not in klass_body:
            if table_name is None:
                # auto-generate name
                mcs.table_name: str = pascal_to_snake(name)
            else:
                mcs.table_name = table_name

        mcs.all_columns: dict[str, Column[DatabaseType[Any]]] = {
            i.name: i for i in klass_body.values() if isinstance(i, Column)
        }

    def __new__(
        cls, name: str, bases: tuple[type], klass_body: dict[str, Any], **kwargs: Any
    ) -> Self:
        return type.__new__(cls, name, bases, klass_body)


class TableSchema(metaclass=_TableMeta):
    """
    Base class for all table schemas. This class does nothing by itself, and should be inherited
    by a schema table instead.

    Table schema type instances contain a list of their columns defined as class variables as
    well as optional extra database metadata such as sequences, indexes, foreign keys, or
    constraints that are used for DDL operations.

    Table schema instances are usually not created by the end user, but are stored internally on
    the :class:`.Isopod` instance, and store session-specific information used to map the raw
    PostgreSQL return values to column instances.
    """

    #: The name of this table. This can either be set as an attribute of the class object
    #: directly, or passed as a metaclass argument with ``table_name``, like so::
    #:
    #:     class MyTable(TableSchema, table_name="myTable"): ...
    table_name: ClassVar[str]

    #: A mapping of of all of the columns for this table.
    all_columns: ClassVar[Mapping[str, Column[DatabaseType[Any]]]]

    def __init__(
        self,
        *,
        table_oid: int,
    ) -> None:
        """
        :param table_oid: The *object identifier* for this table, as designed by PostgreSQL at table
            creation time.
        """

        #: The unique OID for this table.
        self.oid = table_oid

        #: A mapping of Column -> column ordinal for all of the columns in this table.
        self.column_mapping: dict[Column[Any], int] = {}
