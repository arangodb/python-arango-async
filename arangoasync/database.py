__all__ = [
    "Database",
]

from arangoasync.connection import BaseConnection


class Database:
    """Database API."""

    def __init__(self, connection: BaseConnection) -> None:
        self._conn = connection

    @property
    def conn(self) -> BaseConnection:
        """Return the HTTP connection."""
        return self._conn
