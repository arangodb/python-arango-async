__all__ = [
    "BaseConnection",
    "BasicConnection",
]

from abc import ABC, abstractmethod
from typing import Any, List

from arangoasync.http import HTTPClient
from arangoasync.request import Method, Request
from arangoasync.resolver import HostResolver
from arangoasync.response import Response


class BaseConnection(ABC):
    """Blueprint for connection to a specific ArangoDB database.

    Args:
        sessions (list): List of client sessions.
        host_resolver (HostResolver): Host resolver.
        http_client (HTTPClient): HTTP client.
        db_name (str): Database name.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
    ) -> None:
        self._sessions = sessions
        self._db_endpoint = f"/_db/{db_name}"
        self._host_resolver = host_resolver
        self._http_client = http_client
        self._db_name = db_name

    @property
    def db_name(self) -> str:
        """Return the database name."""
        return self._db_name

    def prep_response(selfs, resp: Response) -> None:
        """Prepare response for return."""
        # TODO: Populate response fields

    async def process_request(self, request: Request) -> Response:
        """Process request."""
        # TODO add accept-encoding header option
        # TODO regulate number of tries
        # TODO error handling
        host_index = self._host_resolver.get_host_index()
        return await self._http_client.send_request(self._sessions[host_index], request)

    async def ping(self) -> int:
        """Ping host to check if connection is established.

        Returns:
            int: Response status code.
        """
        request = Request(method=Method.GET, endpoint="/_api/collection")
        resp = await self.send_request(request)
        # TODO check raise ServerConnectionError
        return resp.status_code

    @abstractmethod
    async def send_request(self, request: Request) -> Response:  # pragma: no cover
        """Send an HTTP request to the ArangoDB server.

        Args:
            request (Request): HTTP request.

        Returns:
            Response: HTTP response.
        """
        raise NotImplementedError


class BasicConnection(BaseConnection):
    """Connection to a specific ArangoDB database.

    Allows for basic authentication to be used (username and password).

    Args:
        sessions (list): List of client sessions.
        host_resolver (HostResolver): Host resolver.
        http_client (HTTPClient): HTTP client.
        db_name (str): Database name.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
    ) -> None:
        super().__init__(sessions, host_resolver, http_client, db_name)

    async def send_request(self, request: Request) -> Response:
        """Send an HTTP request to the ArangoDB server."""
        response = await self.process_request(request)
        self.prep_response(response)
        return response
