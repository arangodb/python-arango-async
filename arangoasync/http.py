# TODO __all__ = []

from abc import ABC, abstractmethod
from typing import Optional

from aiohttp import BaseConnector, BasicAuth, ClientSession, ClientTimeout
from request import Request
from response import Response


class Session(ABC):  # pragma: no cover
    """Abstract base class for HTTP sessions."""

    @abstractmethod
    async def request(self, request: Request) -> Response:
        """Send an HTTP request.

        This method must be overridden by the user.

        :param request: HTTP request.
        :type request: arangoasync.request.Request
        :returns: HTTP response.
        :rtype: arangoasync.response.Response
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Close the session.

        This method must be overridden by the user.
        """
        raise NotImplementedError


class HTTPClient(ABC):  # pragma: no cover
    """Abstract base class for HTTP clients."""

    @abstractmethod
    def create_session(self, host: str) -> Session:
        """Return a new requests session given the host URL.

        This method must be overridden by the user.

        :param host: ArangoDB host URL.
        :type host: str
        :returns: Requests session object.
        :rtype: arangoasync.http.Session
        """
        raise NotImplementedError

    @abstractmethod
    async def send_request(
        self,
        session: Session,
        url: str,
        request: Request,
    ) -> Response:
        """Send an HTTP request.

        This method must be overridden by the user.

        :param session: Session object.
        :type session: arangoasync.http.Session
        :param url: Request URL.
        :type url: str
        :param request: HTTP request.
        :type request: arangoasync.request.Request
        :returns: HTTP response.
        :rtype: arango.response.Response
        """
        raise NotImplementedError


class DefaultSession(Session):
    """Wrapper on top of an aiohttp.ClientSession."""

    def __init__(
        self,
        host: str,
        connector: BaseConnector,
        timeout: ClientTimeout,
        read_bufsize: int = 2**16,
        auth: Optional[BasicAuth] = None,
    ) -> None:
        """Initialize the session.

        :param host: ArangoDB coordinator URL (eg http://localhost:8530).
        :type host: str
        :param connector: Supports connection pooling.
        :type connector: aiohttp.BaseConnector
        :param timeout: Request timeout settings.
        :type timeout: aiohttp.ClientTimeout
        :param read_bufsize: Size of read buffer. 64 Kib by default.
        :type read_bufsize: int
        :param auth: HTTP Authorization.
        :type auth: aiohttp.BasicAuth | None
        """
        self._session = ClientSession(
            base_url=host,
            connector=connector,
            timeout=timeout,
            auth=auth,
            read_bufsize=read_bufsize,
            connector_owner=False,
            auto_decompress=True,
        )

    async def request(self, request: Request) -> Response:
        """Send an HTTP request.

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

        async with self._session.request(
            method.name,
            endpoint,
            headers=headers,
            params=params,
            data=data,
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

    async def close(self) -> None:
        """Close the session."""
        await self._session.close()


# TODO implement DefaultHTTPClient
