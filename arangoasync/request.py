__all__ = [
    "Method",
    "Request",
]

from enum import Enum, auto
from typing import Optional

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
        data (str | None): Request payload.

    Attributes:
        method (Method): HTTP method.
        endpoint (str): API endpoint.
        headers (dict | None): Request headers.
        params (dict | None): URL parameters.
        data (str | None): Request payload.
    """

    __slots__ = (
        "method",
        "endpoint",
        "headers",
        "params",
        "data",
    )

    def __init__(
        self,
        method: Method,
        endpoint: str,
        headers: Optional[RequestHeaders] = None,
        params: Optional[Params] = None,
        data: Optional[str] = None,
    ) -> None:
        self.method: Method = method
        self.endpoint: str = endpoint
        self.headers: RequestHeaders = self._normalize_headers(headers)
        self.params: Params = self._normalize_params(params)
        self.data: Optional[str] = data

    @staticmethod
    def _normalize_headers(headers: Optional[RequestHeaders]) -> RequestHeaders:
        """Normalize request headers.

        Parameters:
            headers (dict | None): Request headers.

        Returns:
            dict: Normalized request headers.
        """
        driver_header = f"arangoasync/{__version__}"
        normalized_headers: RequestHeaders = {
            "charset": "utf-8",
            "content-type": "application/json",
            "x-arango-driver": driver_header,
        }

        if headers is not None:
            for key, value in headers.items():
                normalized_headers[key.lower()] = value

        return normalized_headers

    @staticmethod
    def _normalize_params(params: Optional[Params]) -> Params:
        """Normalize URL parameters.

        Parameters:
            params (dict | None): URL parameters.

        Returns:
            dict: Normalized URL parameters.
        """
        normalized_params: Params = {}

        if params is not None:
            for key, value in params.items():
                if isinstance(value, bool):
                    value = int(value)
                normalized_params[key] = str(value)

        return normalized_params
