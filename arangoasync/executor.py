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
    def db_name(self) -> str:
        return self._conn.db_name

    @property
    def serializer(self) -> Serializer[Json]:
        return self._conn.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        return self._conn.deserializer

    def serialize(self, data: Json) -> str:
        return self.serializer.dumps(data)

    def deserialize(self, data: bytes) -> Json:
        return self.deserializer.loads(data)

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


class TransactionApiExecutor(DefaultApiExecutor):
    """Executes transaction API requests.

    Args:
        connection: HTTP connection.
        transaction_id: str: Transaction ID generated by the server.
    """

    def __init__(self, connection: Connection, transaction_id: str) -> None:
        super().__init__(connection)
        self._id = transaction_id

    @property
    def context(self) -> str:
        return "transaction"

    @property
    def id(self) -> str:
        """Return the transaction ID."""
        return self._id

    async def execute(
        self, request: Request, response_handler: Callable[[Response], T]
    ) -> T:
        """Execute the request and handle the response.

        Args:
            request: HTTP request.
            response_handler: HTTP response handler.
        """
        request.headers["x-arango-trx-id"] = self.id
        response = await self._conn.send_request(request)
        return response_handler(response)


ApiExecutor = DefaultApiExecutor | TransactionApiExecutor
