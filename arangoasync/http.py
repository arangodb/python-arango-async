__all__ = [
    "HTTPClient",
    "AioHTTPClient",
    "DefaultHTTPClient",
]

from abc import ABC, abstractmethod
from typing import Any, Optional

from aiohttp import BaseConnector, BasicAuth, ClientSession, ClientTimeout, TCPConnector

from arangoasync.auth import Auth
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
        auth (Auth | None): HTTP authentication helper.
            Should be used for specifying authorization data in client API.
        compression_threshold (int): Will compress requests to the server if the size
            of the request body (in bytes) is at least the value of this option.

    .. _aiohttp:
        https://docs.aiohttp.org/en/stable/
    """

    def __init__(
        self,
        connector: Optional[BaseConnector] = None,
        timeout: Optional[ClientTimeout] = None,
        read_bufsize: int = 2**16,
        auth: Optional[Auth] = None,
        compression_threshold: int = 1024,
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
        self._auth = (
            BasicAuth(
                login=auth.username, password=auth.password, encoding=auth.encoding
            )
            if auth
            else None
        )
        self._compression_threshold = compression_threshold

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
            auth=self._auth,
            read_bufsize=self._read_bufsize,
        )

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
        """
        method = request.method
        endpoint = request.endpoint
        headers = request.headers
        params = request.params
        data = request.data
        compress = data is not None and len(data) >= self._compression_threshold

        async with session.request(
            method.name,
            endpoint,
            headers=headers,
            params=params,
            data=data,
            compress=compress,
        ) as response:
            raw_body = await response.read()
            return Response(
                method=method,
                url=str(response.real_url),
                headers=response.headers,
                status_code=response.status,
                status_text=str(response.reason),
                raw_body=raw_body,
            )


DefaultHTTPClient = AioHTTPClient
