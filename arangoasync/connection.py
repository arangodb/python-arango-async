__all__ = [
    "BaseConnection",
    "BasicConnection",
    "Connection",
    "JwtConnection",
    "JwtSuperuserConnection",
]

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from jwt import ExpiredSignatureError

from arangoasync.auth import Auth, JwtToken
from arangoasync.compression import CompressionManager
from arangoasync.errno import HTTP_UNAUTHORIZED
from arangoasync.exceptions import (
    AuthHeaderError,
    ClientConnectionAbortedError,
    ClientConnectionError,
    DeserializationError,
    JWTRefreshError,
    SerializationError,
    ServerConnectionError,
)
from arangoasync.http import HTTPClient
from arangoasync.logger import logger
from arangoasync.request import Method, Request
from arangoasync.resolver import HostResolver
from arangoasync.response import Response
from arangoasync.serialization import (
    DefaultDeserializer,
    DefaultSerializer,
    Deserializer,
    Serializer,
)
from arangoasync.typings import Json, Jsons


class BaseConnection(ABC):
    """Blueprint for connection to a specific ArangoDB database.

    Args:
        sessions (list): List of client sessions.
        host_resolver (HostResolver): Host resolver.
        http_client (HTTPClient): HTTP client.
        db_name (str): Database name.
        compression (CompressionManager | None): Compression manager.
        serializer (Serializer | None): For overriding the default JSON serialization.
            Leave `None` for default.
        deserializer (Deserializer | None): For overriding the default JSON
            deserialization. Leave `None` for default.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
    ) -> None:
        self._sessions = sessions
        self._db_endpoint = f"/_db/{db_name}"
        self._host_resolver = host_resolver
        self._http_client = http_client
        self._db_name = db_name
        self._compression = compression
        self._serializer: Serializer[Json] = serializer or DefaultSerializer()
        self._deserializer: Deserializer[Json, Jsons] = (
            deserializer or DefaultDeserializer()
        )

    @property
    def db_name(self) -> str:
        """Return the database name."""
        return self._db_name

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._deserializer

    @staticmethod
    def raise_for_status(request: Request, resp: Response) -> None:
        """Raise an exception based on the response.

        Args:
            request (Request): Request object.
            resp (Response): Response object.

        Raises:
            ServerConnectionError: If the response status code is not successful.
        """
        if resp.status_code in {401, 403}:
            raise ServerConnectionError(resp, request, "Authentication failed.")
        if not resp.is_success:
            raise ServerConnectionError(resp, request, "Bad server response.")

    def prep_response(self, request: Request, resp: Response) -> Response:
        """Prepare response for return.

        Args:
            request (Request): Request object.
            resp (Response): Response object.

        Returns:
            Response: Response object
        """
        resp.is_success = 200 <= resp.status_code < 300
        if not resp.is_success:
            try:
                body = self._deserializer.loads(resp.raw_body)
            except DeserializationError as e:
                logger.debug(
                    f"Failed to decode response body: {e} (from request {request})"
                )
            else:
                if body.get("error") is True:
                    resp.error_code = body.get("errorNum")
                    resp.error_message = body.get("errorMessage")
        return resp

    def compress_request(self, request: Request) -> bool:
        """Compress request if needed.

        Additionally, the server may be instructed to compress the response.
        The decision to compress the request is based on the compression strategy
        passed during the connection initialization.
        The request headers and may be modified as a result of this operation.

        Args:
            request (Request): Request to be compressed.

        Returns:
            bool: True if compression settings were applied.
        """
        if self._compression is None:
            return False

        result: bool = False
        if request.data is not None and self._compression.needs_compression(
            request.data
        ):
            request.data = self._compression.compress(request.data)
            request.headers["content-encoding"] = self._compression.content_encoding
            result = True

        accept_encoding: str | None = self._compression.accept_encoding
        if accept_encoding is not None:
            request.headers["accept-encoding"] = accept_encoding
            result = True

        return result

    async def process_request(self, request: Request) -> Response:
        """Process request, potentially trying multiple hosts.

        Args:
            request (Request): Request object.

        Returns:
            Response: Response object.

        Raises:
            ConnectionAbortedError: If it can't connect to host(s) within limit.
        """

        request.endpoint = f"{self._db_endpoint}{request.endpoint}"
        host_index = self._host_resolver.get_host_index()
        for tries in range(self._host_resolver.max_tries):
            try:
                logger.debug(
                    f"Sending request to host {host_index} ({tries}): {request}"
                )
                resp = await self._http_client.send_request(
                    self._sessions[host_index], request
                )
                return self.prep_response(request, resp)
            except ClientConnectionError:
                ex_host_index = host_index
                host_index = self._host_resolver.get_host_index()
                if ex_host_index == host_index:
                    # Force change host if the same host is selected
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
        resp = await self.send_request(request)
        self.raise_for_status(request, resp)
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
        serializer (Serializer | None): Override default JSON serialization.
        deserializer (Deserializer | None): Override default JSON deserialization.
        auth (Auth | None): Authentication information.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
        auth: Optional[Auth] = None,
    ) -> None:
        super().__init__(
            sessions,
            host_resolver,
            http_client,
            db_name,
            compression,
            serializer,
            deserializer,
        )
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
        self.compress_request(request)

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
        serializer (Serializer | None): For custom serialization.
        deserializer (Deserializer | None): For custom deserialization.
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
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
        auth: Optional[Auth] = None,
        token: Optional[JwtToken] = None,
    ) -> None:
        super().__init__(
            sessions,
            host_resolver,
            http_client,
            db_name,
            compression,
            serializer,
            deserializer,
        )
        self._auth = auth
        self._expire_leeway: int = 0
        self._token: Optional[JwtToken] = token
        self._auth_header: Optional[str] = None
        self.token = self._token

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

        auth_data = dict(username=self._auth.username, password=self._auth.password)
        try:
            auth = self._serializer.dumps(auth_data)
        except SerializationError as e:
            logger.debug(f"Failed to serialize auth data: {auth_data}")
            raise JWTRefreshError(str(e)) from e

        request = Request(
            method=Method.POST,
            endpoint="/_open/auth",
            data=auth.encode("utf-8"),
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

        token = self._deserializer.loads(resp.raw_body)
        try:
            self.token = JwtToken(token["jwt"])
        except ExpiredSignatureError as e:
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
            AuthHeaderError: If the authentication header could not be generated.
            ArangoClientError: If an error occurred from the client side.
            ArangoServerError: If an error occurred from the server side.
        """
        if self._auth_header is None:
            await self.refresh_token()

        if self._auth_header is None:
            raise AuthHeaderError("Failed to generate authorization header.")

        request.headers["authorization"] = self._auth_header
        self.compress_request(request)

        resp = await self.process_request(request)
        if (
            resp.status_code == HTTP_UNAUTHORIZED
            and self._token is not None
            and self._token.needs_refresh(self._expire_leeway)
        ):
            # If the token has expired, refresh it and retry the request
            await self.refresh_token()
            resp = await self.process_request(request)
        return resp


class JwtSuperuserConnection(BaseConnection):
    """Connection to a specific ArangoDB database, using superuser JWT.

    The JWT token is not refreshed and (username and password) are not required.

    Args:
        sessions (list): List of client sessions.
        host_resolver (HostResolver): Host resolver.
        http_client (HTTPClient): HTTP client.
        db_name (str): Database name.
        compression (CompressionManager | None): Compression manager.
        serializer (Serializer | None): For custom serialization.
        deserializer (Deserializer | None): For custom deserialization.
        token (JwtToken | None): JWT token.
    """

    def __init__(
        self,
        sessions: List[Any],
        host_resolver: HostResolver,
        http_client: HTTPClient,
        db_name: str,
        compression: Optional[CompressionManager] = None,
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
        token: Optional[JwtToken] = None,
    ) -> None:
        super().__init__(
            sessions,
            host_resolver,
            http_client,
            db_name,
            compression,
            serializer,
            deserializer,
        )
        self._token: Optional[JwtToken] = token
        self._auth_header: Optional[str] = None
        self.token = self._token

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

    async def send_request(self, request: Request) -> Response:
        """Send an HTTP request to the ArangoDB server.

        Args:
            request (Request): HTTP request.

        Returns:
            Response: HTTP response

        Raises:
            AuthHeaderError: If the authentication header could not be generated.
            ArangoClientError: If an error occurred from the client side.
            ArangoServerError: If an error occurred from the server side.
        """
        if self._auth_header is None:
            raise AuthHeaderError("Failed to generate authorization header.")
        request.headers["authorization"] = self._auth_header
        self.compress_request(request)

        resp = await self.process_request(request)
        return resp


Connection = BasicConnection | JwtConnection | JwtSuperuserConnection
