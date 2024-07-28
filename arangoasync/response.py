__all__ = [
    "Response",
]

from typing import Optional

from arangoasync.request import Method
from arangoasync.typings import ResponseHeaders


class Response:
    """HTTP response.

    Parameters:
        method (Method): HTTP method.
        url (str): API URL.
        headers (dict): Response headers.
        status_code (int): Response status code.
        status_text (str): Response status text.
        raw_body (bytes): Raw response body.

    Attributes:
        method (Method): HTTP method.
        url (str): API URL.
        headers (dict): Response headers.
        status_code (int): Response status code.
        status_text (str): Response status text.
        raw_body (bytes): Raw response body.
        error_code (int | None): Error code from ArangoDB server.
        error_message (str | None): Error message from ArangoDB server.
        is_success (bool | None): True if response status code was 2XX.
    """

    __slots__ = (
        "method",
        "url",
        "headers",
        "status_code",
        "status_text",
        "raw_body",
        "error_code",
        "error_message",
        "is_success",
    )

    def __init__(
        self,
        method: Method,
        url: str,
        headers: ResponseHeaders,
        status_code: int,
        status_text: str,
        raw_body: bytes,
    ) -> None:
        self.method: Method = method
        self.url: str = url
        self.headers: ResponseHeaders = headers
        self.status_code: int = status_code
        self.status_text: str = status_text
        self.raw_body: bytes = raw_body

        # Populated later
        self.error_code: Optional[int] = None
        self.error_message: Optional[str] = None
        self.is_success: Optional[bool] = None
