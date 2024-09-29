__all__ = [
    "Database",
    "StandardDatabase",
]


from arangoasync.connection import Connection
from arangoasync.exceptions import ServerStatusError
from arangoasync.executor import ApiExecutor, DefaultApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Result
from arangoasync.wrapper import ServerStatusInformation


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

    @property
    def serializer(self) -> Serializer:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer:
        """Return the deserializer."""
        return self._executor.deserializer

    async def status(self) -> Result[ServerStatusInformation]:
        """Query the server status.

        Returns:
            ServerStatusInformation: Server status.

        Raises:
            ServerSatusError: If retrieval fails.
        """
        request = Request(method=Method.GET, endpoint="/_admin/status")

        def response_handler(resp: Response) -> ServerStatusInformation:
            if not resp.is_success:
                raise ServerStatusError(resp, request)
            return ServerStatusInformation(self.deserializer.from_bytes(resp.raw_body))

        return await self._executor.execute(request, response_handler)


class StandardDatabase(Database):
    """Standard database API wrapper."""

    def __init__(self, connection: Connection) -> None:
        super().__init__(DefaultApiExecutor(connection))
