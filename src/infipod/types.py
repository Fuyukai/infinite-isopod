import abc
from enum import Enum
from typing import override

from pg_purepy import Converter, EnumConverter


class DatabaseType[AcceptedType](abc.ABC):
    """
    A single type in the remote PostgreSQL database.

    A database type is separate but related to a Python-level type; for example, databases have
    multiple text types (``TEXT``, ``VARCHAR``, and friends) but Python only has one (:class:`str`).
    Schema types map between the database type and the Python type.
    """

    #: The string name that PostgreSQL has assigned this type, e.g. ``text``.
    postgresql_name: str


class DatabaseTypeWithConverter[AcceptedType](DatabaseType[AcceptedType], abc.ABC):
    """
    A :class:`.DatabaseType` that has support for adding a converter automatically.
    """

    @abc.abstractmethod
    def create_converter(self, oid: int) -> Converter:
        """
        Creates a new :class:`pg_purepy.Converter` object for this type.
        """

        ...


class IntegerType(DatabaseType[int]):
    """
    Base class for all simple PostgreSQL integer types.
    """


class Int4Type(IntegerType):
    """
    The :class:`.IntegerType` specific for the PostgreSQL ``int4`` type.
    """

    postgresql_name: str = "INT4"


class TextType(DatabaseType[str]):
    """
    The :class:`.DatabaseType` for the PostgreSQL ``text`` type.
    """

    postgresql_name: str = "TEXT"


class EnumType[E: Enum](DatabaseTypeWithConverter[E]):
    """
    A :class:`.DatabaseType` that supports enumerations.
    """

    def __init__(
        self,
        enum_type: type[Enum],
        postgresql_name: str | None = None,
    ) -> None:
        """
        :param enum_type: The :class:`.Enum` type that this field type is for.
        :param postgresql_name: The explicit name of this enumeration in the database, Defaults to
            the name of the provided enum class.
        """

        self.enum_type = enum_type

        if postgresql_name is not None:
            self.postgresql_name: str = postgresql_name
        else:
            self.postgresql_name = self.enum_type.__name__

    @override
    def create_converter(self, oid: int) -> Converter:
        return EnumConverter(oid, self.enum_type)
