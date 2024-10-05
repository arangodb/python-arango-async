from typing import Optional

from arangoasync.request import Request
from arangoasync.response import Response


class ArangoError(Exception):
    """Base class for all exceptions in python-arango-async."""


class ArangoClientError(ArangoError):
    """Base class for all client-related exceptions.

    Args:
        msg (str): Error message.

    Attributes:
        source (str): Source of the error (always set to "client")
        message (str): Error message.
    """

    source = "client"

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.message = msg


class ArangoServerError(ArangoError):
    """Base class for all server-related exceptions.

    Args:
        resp (Response): HTTP response object.
        request (Request): HTTP request object.
        msg (str | None): Error message.

    Attributes:
        source (str): Source of the error (always set to "server")
        message (str): Error message.
        url (str): URL of the request.
        response (Response): HTTP response object.
        request (Request): HTTP request object.
        http_method (str): HTTP method of the request.
        http_code (int): HTTP status code of the response.
        http_headers (dict): HTTP headers of the response.
    """

    source = "server"

    def __init__(
        self, resp: Response, request: Request, msg: Optional[str] = None
    ) -> None:
        msg = msg or resp.error_message or resp.status_text
        self.error_message = resp.error_message
        self.error_code = resp.error_code
        if self.error_code is not None:
            msg = f"[HTTP {resp.status_code}][ERR {self.error_code}] {msg}"
        else:
            msg = f"[HTTP {resp.status_code}] {msg}"
            self.error_code = resp.status_code
        super().__init__(msg)
        self.message = msg
        self.url = resp.url
        self.response = resp
        self.request = request
        self.http_method = resp.method.name
        self.http_code = resp.status_code
        self.http_headers = resp.headers


class AuthHeaderError(ArangoClientError):
    """The authentication header could not be determined."""


class CollectionCreateError(ArangoServerError):
    """Failed to create collection."""


class CollectionDeleteError(ArangoServerError):
    """Failed to delete collection."""


class CollectionListError(ArangoServerError):
    """Failed to retrieve collections."""


class ClientConnectionAbortedError(ArangoClientError):
    """The connection was aborted."""


class ClientConnectionError(ArangoClientError):
    """The request was unable to reach the server."""


class DatabaseCreateError(ArangoServerError):
    """Failed to create database."""


class DatabaseDeleteError(ArangoServerError):
    """Failed to delete database."""


class DatabaseListError(ArangoServerError):
    """Failed to retrieve databases."""


class DeserializationError(ArangoClientError):
    """Failed to deserialize the server response."""


class DocumentGetError(ArangoServerError):
    """Failed to retrieve document."""


class DocumentParseError(ArangoClientError):
    """Failed to parse document input."""


class DocumentRevisionError(ArangoServerError):
    """The expected and actual document revisions mismatched."""


class JWTRefreshError(ArangoClientError):
    """Failed to refresh the JWT token."""


class SerializationError(ArangoClientError):
    """Failed to serialize the request."""


class ServerConnectionError(ArangoServerError):
    """Failed to connect to ArangoDB server."""


class ServerStatusError(ArangoServerError):
    """Failed to retrieve server status."""
