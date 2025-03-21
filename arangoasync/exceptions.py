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
        if msg is None:
            msg = resp.error_message or resp.status_text
        else:
            msg = f"{msg} ({resp.error_message or resp.status_text})"
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


class AQLCacheClearError(ArangoServerError):
    """Failed to clear the query cache."""


class AQLCacheConfigureError(ArangoServerError):
    """Failed to configure query cache properties."""


class AQLCacheEntriesError(ArangoServerError):
    """Failed to retrieve AQL cache entries."""


class AQLCachePropertiesError(ArangoServerError):
    """Failed to retrieve query cache properties."""


class AQLFunctionCreateError(ArangoServerError):
    """Failed to create AQL user function."""


class AQLFunctionDeleteError(ArangoServerError):
    """Failed to delete AQL user function."""


class AQLFunctionListError(ArangoServerError):
    """Failed to retrieve AQL user functions."""


class AQLQueryClearError(ArangoServerError):
    """Failed to clear slow AQL queries."""


class AQLQueryExecuteError(ArangoServerError):
    """Failed to execute query."""


class AQLQueryExplainError(ArangoServerError):
    """Failed to parse and explain query."""


class AQLQueryKillError(ArangoServerError):
    """Failed to kill the query."""


class AQLQueryListError(ArangoServerError):
    """Failed to retrieve running AQL queries."""


class AQLQueryRulesGetError(ArangoServerError):
    """Failed to retrieve AQL query rules."""


class AQLQueryTrackingGetError(ArangoServerError):
    """Failed to retrieve AQL tracking properties."""


class AQLQueryTrackingSetError(ArangoServerError):
    """Failed to configure AQL tracking properties."""


class AQLQueryValidateError(ArangoServerError):
    """Failed to parse and validate query."""


class AsyncExecuteError(ArangoServerError):
    """Failed to execute async API request."""


class AsyncJobCancelError(ArangoServerError):
    """Failed to cancel async job."""


class AsyncJobClearError(ArangoServerError):
    """Failed to clear async job results."""


class AsyncJobListError(ArangoServerError):
    """Failed to retrieve async jobs."""


class AsyncJobResultError(ArangoServerError):
    """Failed to retrieve async job result."""


class AsyncJobStatusError(ArangoServerError):
    """Failed to retrieve async job status."""


class AuthHeaderError(ArangoClientError):
    """The authentication header could not be determined."""


class CollectionCreateError(ArangoServerError):
    """Failed to create collection."""


class CollectionDeleteError(ArangoServerError):
    """Failed to delete collection."""


class CollectionListError(ArangoServerError):
    """Failed to retrieve collections."""


class CollectionPropertiesError(ArangoServerError):
    """Failed to retrieve collection properties."""


class ClientConnectionAbortedError(ArangoClientError):
    """The connection was aborted."""


class ClientConnectionError(ArangoClientError):
    """The request was unable to reach the server."""


class CollectionTruncateError(ArangoServerError):
    """Failed to truncate collection."""


class CursorCloseError(ArangoServerError):
    """Failed to delete the cursor result from server."""


class CursorCountError(ArangoClientError, TypeError):
    """The cursor count was not enabled."""


class CursorEmptyError(ArangoClientError):
    """The current batch in cursor was empty."""


class CursorNextError(ArangoServerError):
    """Failed to retrieve the next result batch from server."""


class CursorStateError(ArangoClientError):
    """The cursor object was in a bad state."""


class DatabaseCreateError(ArangoServerError):
    """Failed to create database."""


class DatabaseDeleteError(ArangoServerError):
    """Failed to delete database."""


class DatabaseListError(ArangoServerError):
    """Failed to retrieve databases."""


class DatabasePropertiesError(ArangoServerError):
    """Failed to retrieve database properties."""


class DeserializationError(ArangoClientError):
    """Failed to deserialize the server response."""


class DocumentCountError(ArangoServerError):
    """Failed to retrieve document count."""


class DocumentDeleteError(ArangoServerError):
    """Failed to delete document."""


class DocumentGetError(ArangoServerError):
    """Failed to retrieve document."""


class DocumentInsertError(ArangoServerError):
    """Failed to insert document."""


class DocumentParseError(ArangoClientError):
    """Failed to parse document input."""


class DocumentReplaceError(ArangoServerError):
    """Failed to replace document."""


class DocumentRevisionError(ArangoServerError):
    """The expected and actual document revisions mismatched."""


class DocumentUpdateError(ArangoServerError):
    """Failed to update document."""


class IndexCreateError(ArangoServerError):
    """Failed to create collection index."""


class IndexDeleteError(ArangoServerError):
    """Failed to delete collection index."""


class IndexGetError(ArangoServerError):
    """Failed to retrieve collection index."""


class IndexListError(ArangoServerError):
    """Failed to retrieve collection indexes."""


class IndexLoadError(ArangoServerError):
    """Failed to load indexes into memory."""


class JWTRefreshError(ArangoClientError):
    """Failed to refresh the JWT token."""


class JWTSecretListError(ArangoServerError):
    """Failed to retrieve information on currently loaded JWT secrets."""


class JWTSecretReloadError(ArangoServerError):
    """Failed to reload JWT secrets."""


class PermissionGetError(ArangoServerError):
    """Failed to retrieve user permission."""


class PermissionListError(ArangoServerError):
    """Failed to list user permissions."""


class PermissionResetError(ArangoServerError):
    """Failed to reset user permission."""


class PermissionUpdateError(ArangoServerError):
    """Failed to update user permission."""


class SerializationError(ArangoClientError):
    """Failed to serialize the request."""


class ServerConnectionError(ArangoServerError):
    """Failed to connect to ArangoDB server."""


class ServerStatusError(ArangoServerError):
    """Failed to retrieve server status."""


class ServerVersionError(ArangoServerError):
    """Failed to retrieve server version."""


class SortValidationError(ArangoClientError):
    """Invalid sort parameters."""


class TransactionAbortError(ArangoServerError):
    """Failed to abort transaction."""


class TransactionCommitError(ArangoServerError):
    """Failed to commit transaction."""


class TransactionExecuteError(ArangoServerError):
    """Failed to execute JavaScript transaction."""


class TransactionInitError(ArangoServerError):
    """Failed to initialize transaction."""


class TransactionListError(ArangoServerError):
    """Failed to retrieve transactions."""


class TransactionStatusError(ArangoServerError):
    """Failed to retrieve transaction status."""


class UserCreateError(ArangoServerError):
    """Failed to create user."""


class UserDeleteError(ArangoServerError):
    """Failed to delete user."""


class UserGetError(ArangoServerError):
    """Failed to retrieve user details."""


class UserListError(ArangoServerError):
    """Failed to retrieve users."""


class UserReplaceError(ArangoServerError):
    """Failed to replace user."""


class UserUpdateError(ArangoServerError):
    """Failed to update user."""
