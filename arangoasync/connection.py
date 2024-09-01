__all__ = [
    "BaseConnection",
    "BasicConnection",
]

import json
from abc import ABC, abstractmethod
from typing import Any, List, Optional

import jwt

from arangoasync.auth import Auth, JwtToken
from arangoasync.compression import CompressionManager, DefaultCompressionManager
from arangoasync.exceptions import (
    AuthHeaderError,
    ClientConnectionAbortedError,
    ClientConnectionError,
    JWTRefreshError,
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
        # TODO needs refactoring such that it does not throw
        resp.is_success = 200 <= resp.status_code < 300
        if resp.status_code in {401, 403}:
            raise ServerConnectionError(resp, request, "Authentication failed.")
        if not resp.is_success:
            raise ServerConnectionError(resp, request, "Bad server response.")
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

        raise ClientConnectionAbortedError(
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
        request.headers = {"abde": "fghi"}
        resp = await self.send_request(request)
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
        """Send an HTTP request to the ArangoDB server.

        Args:
            request (Request): HTTP request.

        Returns:
            Response: HTTP response

        Raises:
            ArangoClientError: If an error occurred from the client side.
            ArangoServerError: If an error occurred from the server side.
        """
        if request.data is not None and self._compression.needs_compression(
            request.data
        ):
            request.data = self._compression.compress(request.data)
            request.headers["content-encoding"] = self._compression.content_encoding

        accept_encoding: str | None = self._compression.accept_encoding
        if accept_encoding is not None:
            request.headers["accept-encoding"] = accept_encoding

        if self._auth:
            request.auth = self._auth

        return await self.process_request(request)


class JwtConnection(BaseConnection):
    """Connection to a specific ArangoDB database, using JWT authentication.

    Providing login information (username and password), allows to refresh the JWT.

    Args:
        sessions (list): List of client sessions.
        host_resolver (HostResolver): Host resolver.
        http_client (HTTPClient): HTTP client.
        db_name (str): Database name.
        compression (CompressionManager | None): Compression manager.
        auth (Auth | None): Authentication information.
        token (JwtToken | None): JWT token.

    Raises:
        ValueError: If neither token nor auth is provided.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
        auth: Optional[Auth] = None,
        token: Optional[JwtToken] = None,
    ) -> None:
        super().__init__(sessions, host_resolver, http_client, db_name, compression)
        self._auth = auth
        self._expire_leeway: int = 0
        self._token: Optional[JwtToken] = None
        self._auth_header: Optional[str] = None
        self.token = token

        if self._token is None and self._auth is None:
            raise ValueError("Either token or auth must be provided.")

    @property
    def token(self) -> Optional[JwtToken]:
        """Get the JWT token.

        Returns:
            JwtToken | None: JWT token.
        """
        return self._token

    @token.setter
    def token(self, token: Optional[JwtToken]) -> None:
        """Set the JWT token.

        Args:
            token (JwtToken | None): JWT token.
                Setting it to None will cause the token to be automatically
                refreshed on the next request, if auth information is provided.
        """
        self._token = token
        self._auth_header = f"bearer {self._token.token}" if self._token else None

    async def refresh_token(self) -> None:
        """Refresh the JWT token.

        Raises:
            JWTRefreshError: If the token can't be refreshed.
        """
        if self._auth is None:
            raise JWTRefreshError("Auth must be provided to refresh the token.")

        data = json.dumps(
            dict(username=self._auth.username, password=self._auth.password),
            separators=(",", ":"),
            ensure_ascii=False,
        )
        request = Request(
            method=Method.POST,
            endpoint="/_open/auth",
            data=data.encode("utf-8"),
        )

        try:
            resp = await self.process_request(request)
        except ClientConnectionAbortedError as e:
            raise JWTRefreshError(str(e)) from e
        except ServerConnectionError as e:
            raise JWTRefreshError(str(e)) from e

        if not resp.is_success:
            raise JWTRefreshError(
                f"Failed to refresh the JWT token: "
                f"{resp.status_code} {resp.status_text}"
            )

        token = json.loads(resp.raw_body)
        try:
            self.token = JwtToken(token["jwt"])
        except jwt.ExpiredSignatureError as e:
            raise JWTRefreshError(
                "Failed to refresh the JWT token: got an expired token"
            ) from e

    async def send_request(self, request: Request) -> Response:
        """Send an HTTP request to the ArangoDB server.

        Args:
            request (Request): HTTP request.

        Returns:
            Response: HTTP response

        Raises:
            ArangoClientError: If an error occurred from the client side.
            ArangoServerError: If an error occurred from the server side.
        """
        if self._auth_header is None:
            await self.refresh_token()

        if self._auth_header is None:
            raise AuthHeaderError("Failed to generate authorization header.")

        request.headers["authorization"] = self._auth_header

        try:
            resp = await self.process_request(request)
            if (
                resp.status_code == 401  # Unauthorized
                and self._token is not None
                and self._token.needs_refresh(self._expire_leeway)
            ):
                await self.refresh_token()
            return await self.process_request(request)  # Retry with new token
        except ServerConnectionError:
            # TODO modify after refactoring of prep_response, so we can inspect response
            await self.refresh_token()
            return await self.process_request(request)  # Retry with new token
