from typing import Callable, TypeVar

from arangoasync.connection import Connection
from arangoasync.request import Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons

T = TypeVar("T")


class DefaultApiExecutor:
    """Default API executor.

    Responsible for executing requests and handling responses.

    Args:
        connection: HTTP connection.
    """

    def __init__(self, connection: Connection) -> None:
        self._conn = connection

    @property
    def connection(self) -> Connection:
        return self._conn

    @property
    def context(self) -> str:
        return "default"

    @property
    def serializer(self) -> Serializer[Json]:
        return self._conn.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        return self._conn.deserializer

    async def execute(
        self, request: Request, response_handler: Callable[[Response], T]
    ) -> T:
        """Execute the request and handle the response.

        Args:
            request: HTTP request.
            response_handler: HTTP response handler.
        """
        response = await self._conn.send_request(request)
        return response_handler(response)


ApiExecutor = DefaultApiExecutor
