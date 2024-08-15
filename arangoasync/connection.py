__all__ = [
    "BaseConnection",
    "BasicConnection",
]

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from arangoasync.auth import Auth
from arangoasync.compression import CompressionManager, DefaultCompressionManager
from arangoasync.exceptions import (
    ClientConnectionError,
    ConnectionAbortedError,
    ServerConnectionError,
)
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
        compression (CompressionManager | None): Compression manager.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
    ) -> None:
        self._sessions = sessions
        self._db_endpoint = f"/_db/{db_name}"
        self._host_resolver = host_resolver
        self._http_client = http_client
        self._db_name = db_name
        self._compression = compression or DefaultCompressionManager()

    @property
    def db_name(self) -> str:
        """Return the database name."""
        return self._db_name

    def prep_response(self, request: Request, resp: Response) -> Response:
        """Prepare response for return.

        Args:
            request (Request): Request object.
            resp (Response): Response object.

        Returns:
            Response: Response object

        Raises:
            ServerConnectionError: If the response status code is not successful.
        """
        resp.is_success = 200 <= resp.status_code < 300
        if not resp.is_success:
            raise ServerConnectionError(resp, request)
        return resp

    async def process_request(self, request: Request) -> Response:
        """Process request, potentially trying multiple hosts.

        Args:
            request (Request): Request object.

        Returns:
            Response: Response object.

        Raises:
            ConnectionAbortedError: If can't connect to host(s) within limit.
        """

        ex_host_index = -1
        host_index = self._host_resolver.get_host_index()
        for tries in range(self._host_resolver.max_tries):
            try:
                resp = await self._http_client.send_request(
                    self._sessions[host_index], request
                )
                return self.prep_response(request, resp)
            except ClientConnectionError:
                ex_host_index = host_index
                host_index = self._host_resolver.get_host_index()
                if ex_host_index == host_index:
                    self._host_resolver.change_host()
                    host_index = self._host_resolver.get_host_index()

        raise ConnectionAbortedError(
            f"Can't connect to host(s) within limit ({self._host_resolver.max_tries})"
        )

    async def ping(self) -> int:
        """Ping host to check if connection is established.

        Returns:
            int: Response status code.

        Raises:
            ServerConnectionError: If the response status code is not successful.
        """
        request = Request(method=Method.GET, endpoint="/_api/collection")
        resp = await self.send_request(request)
        if resp.status_code in {401, 403}:
            raise ServerConnectionError(resp, request, "Authentication failed.")
        if not resp.is_success:
            raise ServerConnectionError(resp, request, "Bad server response.")
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
        compression (CompressionManager | None): Compression manager.
        auth (Auth | None): Authentication information.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
        auth: Optional[Auth] = None,
    ) -> None:
        super().__init__(sessions, host_resolver, http_client, db_name, compression)
        self._auth = auth

    async def send_request(self, request: Request) -> Response:
        """Send an HTTP request to the ArangoDB server."""
        if request.data is not None and self._compression.needs_compression(
            request.data
        ):
            request.data = self._compression.compress(request.data)
            request.headers["content-encoding"] = self._compression.content_encoding()
        if self._compression.accept_encoding() is not None:
            request.headers["accept-encoding"] = self._compression.accept_encoding()

        if self._auth:
            request.auth = self._auth

        return await self.process_request(request)
