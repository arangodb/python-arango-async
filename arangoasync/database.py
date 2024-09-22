__all__ = [
    "Database",
    "StandardDatabase",
]

import json
from typing import Any

from arangoasync.connection import Connection
from arangoasync.exceptions import ServerStatusError
from arangoasync.executor import ApiExecutor, DefaultApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response


class Database:
    """Database API."""

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    @property
    def connection(self) -> Connection:
        """Return the HTTP connection."""
        return self._executor.connection

    @property
    def name(self) -> str:
        """Return the name of the current database."""
        return self.connection.db_name

    # TODO - user real return type
    async def status(self) -> Any:
        """Query the server status.

        Returns:
            Json: Server status.

        Raises:
            ServerSatusError: If retrieval fails.
        """
        request = Request(method=Method.GET, endpoint="/_admin/status")

        # TODO
        # - introduce specific return type for response_handler
        # - introduce specific serializer and deserializer
        def response_handler(resp: Response) -> Any:
            if not resp.is_success:
                raise ServerStatusError(resp, request)
            return json.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)


class StandardDatabase(Database):
    """Standard database API wrapper."""

    def __init__(self, connection: Connection) -> None:
        super().__init__(DefaultApiExecutor(connection))
