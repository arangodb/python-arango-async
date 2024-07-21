__all__ = [
    "HTTPClient",
    "AioHTTPClient",
    "DefaultHTTPClient",
]

from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar

from aiohttp import BaseConnector, BasicAuth, ClientSession, ClientTimeout, TCPConnector

from arangoasync.request import Request
from arangoasync.response import Response

T = TypeVar("T")


class HTTPClient(ABC):  # pragma: no cover
    """Abstract base class for HTTP clients.
    Custom HTTP clients should inherit from this class.
    """

    @abstractmethod
    def create_session(self, host: str) -> Any:
        """Return a new session given the base host URL.

        This method must be overridden by the user.

        :param host: ArangoDB host URL.
        :type host: str
        :returns: Requests session object.
        :rtype: Any
        """
        raise NotImplementedError

    @abstractmethod
    async def send_request(
        self,
        session: Any,
        request: Request,
    ) -> Response:
        """Send an HTTP request.

        This method must be overridden by the user.

        :param session: Session object.
        :type session: Any
        :param request: HTTP request.
        :type request: arangoasync.request.Request
        :returns: HTTP response.
        :rtype: arangoasync.response.Response
        """
        raise NotImplementedError


class AioHTTPClient(HTTPClient, Generic[T]):
    """HTTP client implemented on top of [aiohttp](https://docs.aiohttp.org/en/stable/).

    :param connector: Supports connection pooling.
        By default, 100 simultaneous connections are supported, with a 60-second timeout
        for connection reusing after release.
    :type connector: aiohttp.BaseConnector | None
    :param timeout: Timeout settings.
        300s total timeout by default for a complete request/response operation.
    :type timeout: aiohttp.ClientTimeout | None
    :param read_bufsize: Size of read buffer (64KB default).
    :type read_bufsize: int
    :param auth: HTTP authentication helper.
        Should be used for specifying authorization data in client API.
    :type auth: aiohttp.BasicAuth | None
    :param compression_threshold: Will compress requests to the server if
        the size of the request body (in bytes) is at least the value of this
        option.
    :type compression_threshold: int
    """

    def __init__(
        self,
        connector: Optional[BaseConnector] = None,
        timeout: Optional[ClientTimeout] = None,
        read_bufsize: int = 2**16,
        auth: Optional[BasicAuth] = None,
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
        self._auth = auth
        self._compression_threshold = compression_threshold

    def create_session(self, host: str) -> ClientSession:
        """Return a new session given the base host URL.

        :param host: ArangoDB host URL. Typically, the address and port of a coordinator
            (e.g. "http://127.0.0.1:8529").
        :type host: str
        :returns: Session object.
        :rtype: aiohttp.ClientSession
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

        :param session: Session object.
        :type session: aiohttp.ClientSession
        :param request: HTTP request.
        :type request: arangoasync.request.Request
        :returns: HTTP response.
        :rtype: arangoasync.response.Response
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
                status_text=response.reason,
                raw_body=raw_body,
            )


DefaultHTTPClient = AioHTTPClient
