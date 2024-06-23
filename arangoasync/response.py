from typing import Generic, Optional, TypeVar

from request import Method
from typings import Headers

T = TypeVar("T")


class Response(Generic[T]):
    """HTTP response.

    :param method: HTTP method.
    :type method: request.Method
    :param url: API URL.
    :type url: str
    :param headers: Response headers.
    :type headers: dict | None
    :param status_code: Response status code.
    :type status_code: int
    :param status_text: Response status text.
    :type status_text: str
    :param raw_body: Raw response body.
    :type raw_body: str

    :ivar method: HTTP method.
    :vartype method: request.Method
    :ivar url: API URL.
    :vartype url: str
    :ivar headers: Response headers.
    :vartype headers: dict | None
    :ivar status_code: Response status code.
    :vartype status_code: int
    :ivar status_text: Response status text.
    :vartype status_text: str
    :ivar raw_body: Raw response body.
    :vartype raw_body: str
    :ivar body: Response body after processing.
    :vartype body: Any
    :ivar error_code: Error code from ArangoDB server.
    :vartype error_code: int
    :ivar error_message: Error message from ArangoDB server.
    :vartype error_message: str
    :ivar is_success: True if response status code was 2XX.
    :vartype is_success: bool
    """

    __slots__ = (
        "method",
        "url",
        "headers",
        "status_code",
        "status_text",
        "body",
        "raw_body",
        "error_code",
        "error_message",
        "is_success",
    )

    def __init__(
        self,
        method: Method,
        url: str,
        headers: Headers,
        status_code: int,
        status_text: str,
        raw_body: bytes,
    ) -> None:
        self.method: Method = method
        self.url: str = url
        self.headers: Headers = headers
        self.status_code: int = status_code
        self.status_text: str = status_text
        self.raw_body: bytes = raw_body

        # Populated later
        self.body: Optional[T] = None
        self.error_code: Optional[int] = None
        self.error_message: Optional[str] = None
        self.is_success: Optional[bool] = None
