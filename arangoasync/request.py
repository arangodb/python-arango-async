__all__ = [
    "Method",
    "Request",
]

from enum import Enum, auto
from typing import Optional

from arangoasync.auth import Auth
from arangoasync.typings import Params, RequestHeaders
from arangoasync.version import __version__


class Method(Enum):
    """HTTP methods enum: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS"""

    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


class Request:
    """HTTP request.

    Args:
        method (Method): HTTP method.
        endpoint (str): API endpoint.
        headers (dict | None): Request headers.
        params (dict | None): URL parameters.
        data (bytes | None): Request payload.
        auth (Auth | None): Authentication.

    Attributes:
        method (Method): HTTP method.
        endpoint (str): API endpoint.
        headers (dict | None): Request headers.
        params (dict | None): URL parameters.
        data (bytes | None): Request payload.
        auth (Auth | None): Authentication.
    """

    __slots__ = (
        "method",
        "endpoint",
        "headers",
        "params",
        "data",
        "auth",
    )

    def __init__(
        self,
        method: Method,
        endpoint: str,
        headers: Optional[RequestHeaders] = None,
        params: Optional[Params] = None,
        data: Optional[bytes | str] = None,
        auth: Optional[Auth] = None,
    ) -> None:
        self.method: Method = method
        self.endpoint: str = endpoint
        self.headers: RequestHeaders = headers or dict()
        self.params: Params = params or dict()
        self.data: Optional[bytes | str] = data
        self.auth: Optional[Auth] = auth

    def normalized_headers(self) -> RequestHeaders:
        """Normalize request headers.

        Returns:
            dict: Normalized request headers.
        """
        driver_header = f"arangoasync/{__version__}"
        normalized_headers: RequestHeaders = {
            "charset": "utf-8",
            "content-type": "application/json",
            "x-arango-driver": driver_header,
        }

        if self.headers is not None:
            for key, value in self.headers.items():
                normalized_headers[key.lower()] = value

        return normalized_headers

    def normalized_params(self) -> Params:
        """Normalize URL parameters.

        Returns:
            dict: Normalized URL parameters.
        """
        normalized_params: Params = {}

        if self.params is not None:
            for key, value in self.params.items():
                if isinstance(value, bool):
                    value = int(value)
                normalized_params[key] = str(value)

        return normalized_params

    def __repr__(self) -> str:
        return f"<{self.method.name} {self.endpoint}>"
