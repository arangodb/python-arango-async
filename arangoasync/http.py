__all__ = [
    "HTTPClient",
    "AioHTTPClient",
    "DefaultHTTPClient",
]

from abc import ABC, abstractmethod
from ssl import SSLContext, create_default_context
from typing import Any, Optional

from aiohttp import (
    BaseConnector,
    BasicAuth,
    ClientSession,
    ClientTimeout,
    TCPConnector,
    client_exceptions,
)

from arangoasync.exceptions import ClientConnectionError
from arangoasync.request import Request
from arangoasync.response import Response


class HTTPClient(ABC):  # pragma: no cover
    """Abstract base class for HTTP clients.

    Custom HTTP clients should inherit from this class.

    Example:
        .. code-block:: python

            class MyCustomHTTPClient(HTTPClient):
                def create_session(self, host):
                    pass
                async def close_session(self, session):
                    pass
                async def send_request(self, session, request):
                    pass
    """

    @abstractmethod
    def create_session(self, host: str) -> Any:
        """Return a new session given the base host URL.

        Note:
            This method must be overridden by the user.

        Args:
            host (str): ArangoDB host URL.

        Returns:
            Requests session object.
        """
        raise NotImplementedError

    @abstractmethod
    async def close_session(self, session: Any) -> None:
        """Close the session.

        Note:
            This method must be overridden by the user.

        Args:
            session (Any): Client session object.
        """
        raise NotImplementedError

    @abstractmethod
    async def send_request(
        self,
        session: Any,
        request: Request,
    ) -> Response:
        """Send an HTTP request.

        Note:
            This method must be overridden by the user.

        Args:
            session (Any): Client session object.
            request (Request): HTTP request.

        Returns:
            Response: HTTP response.
        """
        raise NotImplementedError


class AioHTTPClient(HTTPClient):
    """HTTP client implemented on top of aiohttp_.

    Args:
        connector (aiohttp.BaseConnector | None): Supports connection pooling.
            By default, 100 simultaneous connections are supported, with a 60-second
            timeout for connection reusing after release.
        timeout (aiohttp.ClientTimeout | None): Client timeout settings.
            300s total timeout by default for a complete request/response operation.
        read_bufsize (int): Size of read buffer (64KB default).
        ssl_context (ssl.SSLContext | bool): SSL validation mode.
            `True` for default SSL checks (see :func:`ssl.create_default_context`).
            `False` disables SSL checks.
            Additionally, you can pass a custom :class:`ssl.SSLContext`.

    .. _aiohttp:
        https://docs.aiohttp.org/en/stable/
    """

    def __init__(
        self,
        connector: Optional[BaseConnector] = None,
        timeout: Optional[ClientTimeout] = None,
        read_bufsize: int = 2**16,
        ssl_context: bool | SSLContext = True,
    ) -> None:
        self._connector = connector or TCPConnector(
            keepalive_timeout=60,  # timeout for connection reusing after releasing
            limit=100,  # total number simultaneous connections
        )
        self._timeout = timeout or ClientTimeout(
            total=300,  # total number of seconds for the whole request
            connect=60,  # max number of seconds for acquiring a pool connection
        )
        self._read_bufsize = read_bufsize
        self._ssl_context = (
            ssl_context if ssl_context is not True else create_default_context()
        )

    def create_session(self, host: str) -> ClientSession:
        """Return a new session given the base host URL.

        Args:
            host (str): ArangoDB host URL. Must not include any paths. Typically, this
                is the address and port of a coordinator (e.g. "http://127.0.0.1:8529").

        Returns:
            aiohttp.ClientSession: Session object, used to send future requests.
        """
        return ClientSession(
            base_url=host,
            connector=self._connector,
            timeout=self._timeout,
            read_bufsize=self._read_bufsize,
        )

    async def close_session(self, session: ClientSession) -> None:
        """Close the session.

        Args:
            session (Any): Client session object.
        """
        await session.close()

    async def send_request(
        self,
        session: ClientSession,
        request: Request,
    ) -> Response:
        """Send an HTTP request.

        Args:
            session (aiohttp.ClientSession): Session object used to make the request.
            request (Request): HTTP request.

        Returns:
            Response: HTTP response.

        Raises:
            ClientConnectionError: If the request fails.
        """

        if request.auth is not None:
            auth = BasicAuth(
                login=request.auth.username,
                password=request.auth.password,
                encoding=request.auth.encoding,
            )
        else:
            auth = None

        try:
            async with session.request(
                request.method.name,
                request.endpoint,
                headers=request.normalized_headers(),
                params=request.normalized_params(),
                data=request.data,
                auth=auth,
                ssl=self._ssl_context,
            ) as response:
                raw_body = await response.read()
                return Response(
                    method=request.method,
                    url=str(response.real_url),
                    headers=response.headers,
                    status_code=response.status,
                    status_text=str(response.reason),
                    raw_body=raw_body,
                )
        except client_exceptions.ClientConnectionError as e:
            raise ClientConnectionError(str(e)) from e


DefaultHTTPClient = AioHTTPClient
