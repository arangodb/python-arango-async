__all__ = [
    "Method",
    "Request",
]

from enum import Enum, auto
from typing import Optional

from arangoasync.typings import Headers, Params
from arangoasync.version import __version__


class Method(Enum):
    """HTTP methods."""

    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


class Request:
    """HTTP request.

    :param method: HTTP method.
    :type method: request.Method
    :param endpoint: API endpoint.
    :type endpoint: str
    :param headers: Request headers.
    :type headers: dict | None
    :param params: URL parameters.
    :type params: dict | None
    :param data: Request payload.
    :type data: Any
    :param deserialize: Whether the response body should be deserialized.
    :type deserialize: bool

    :ivar method: HTTP method.
    :vartype method: request.Method
    :ivar endpoint: API endpoint, for example "_api/version".
    :vartype endpoint: str
    :ivar headers: Request headers.
    :vartype headers: dict | None
    :ivar params: URL (query) parameters.
    :vartype params: dict | None
    :ivar data: Request payload.
    :vartype data: Any
    :ivar deserialize: Whether the response body should be deserialized.
    :vartype deserialize: bool
    """

    __slots__ = (
        "method",
        "endpoint",
        "headers",
        "params",
        "data",
        "deserialize",
    )

    def __init__(
        self,
        method: Method,
        endpoint: str,
        headers: Optional[Headers] = None,
        params: Optional[Params] = None,
        data: Optional[str] = None,
        deserialize: bool = True,
    ) -> None:
        self.method: Method = method
        self.endpoint: str = endpoint
        self.headers: Headers = self._normalize_headers(headers)
        self.params: Params = self._normalize_params(params)
        self.data: Optional[str] = data
        self.deserialize: bool = deserialize

    @staticmethod
    def _normalize_headers(headers: Optional[Headers]) -> Headers:
        """Normalize request headers.

        :param headers: Request headers.
        :type headers: dict | None
        :returns: Normalized request headers.
        :rtype: dict
        """
        driver_header = f"arangoasync/{__version__}"
        normalized_headers: Headers = {
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

        :param params: URL parameters.
        :type params: dict | None
        :returns: Normalized URL parameters.
        :rtype: dict
        """
        normalized_params: Params = {}

        if params is not None:
            for key, value in params.items():
                if isinstance(value, bool):
                    value = int(value)
                normalized_params[key] = str(value)

        return normalized_params