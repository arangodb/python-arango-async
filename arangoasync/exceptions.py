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


class AnalyzerCreateError(ArangoServerError):
    """Failed to create analyzer."""


class AnalyzerGetError(ArangoServerError):
    """Failed to retrieve analyzer details."""


class AnalyzerDeleteError(ArangoServerError):
    """Failed to delete analyzer."""


class AnalyzerListError(ArangoServerError):
    """Failed to retrieve analyzers."""


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


class BackupCreateError(ArangoServerError):
    """Failed to create a backup."""


class BackupDeleteError(ArangoServerError):
    """Failed to delete a backup."""


class BackupDownloadError(ArangoServerError):
    """Failed to download a backup from remote repository."""


class BackupGetError(ArangoServerError):
    """Failed to retrieve backup details."""


class BackupRestoreError(ArangoServerError):
    """Failed to restore from backup."""


class BackupUploadError(ArangoServerError):
    """Failed to upload a backup to remote repository."""


class CollectionCreateError(ArangoServerError):
    """Failed to create collection."""


class CollectionChecksumError(ArangoServerError):
    """Failed to retrieve collection checksum."""


class CollectionConfigureError(ArangoServerError):
    """Failed to configure collection properties."""


class CollectionCompactError(ArangoServerError):
    """Failed to compact collection."""


class CollectionDeleteError(ArangoServerError):
    """Failed to delete collection."""


class CollectionKeyGeneratorsError(ArangoServerError):
    """Failed to retrieve key generators."""


class CollectionListError(ArangoServerError):
    """Failed to retrieve collections."""


class CollectionPropertiesError(ArangoServerError):
    """Failed to retrieve collection properties."""


class CollectionRecalculateCountError(ArangoServerError):
    """Failed to recalculate document count."""


class CollectionRenameError(ArangoServerError):
    """Failed to rename collection."""


class CollectionResponsibleShardError(ArangoServerError):
    """Failed to retrieve responsible shard."""


class CollectionRevisionError(ArangoServerError):
    """Failed to retrieve collection revision."""


class CollectionShardsError(ArangoServerError):
    """Failed to retrieve collection shards."""


class CollectionStatisticsError(ArangoServerError):
    """Failed to retrieve collection statistics."""


class CollectionTruncateError(ArangoServerError):
    """Failed to truncate collection."""


class ClientConnectionAbortedError(ArangoClientError):
    """The connection was aborted."""


class ClientConnectionError(ArangoClientError):
    """The request was unable to reach the server."""


class ClusterEndpointsError(ArangoServerError):
    """Failed to retrieve coordinator endpoints."""


class ClusterHealthError(ArangoServerError):
    """Failed to retrieve cluster health."""


class ClusterMaintenanceModeError(ArangoServerError):
    """Failed to enable/disable cluster supervision maintenance mode."""


class ClusterRebalanceError(ArangoServerError):
    """Failed to execute cluster rebalancing operation."""


class ClusterServerRoleError(ArangoServerError):
    """Failed to retrieve server role in a cluster."""


class ClusterServerIDError(ArangoServerError):
    """Failed to retrieve server ID."""


class ClusterStatisticsError(ArangoServerError):
    """Failed to retrieve DB-Server statistics."""


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


class DatabaseSupportInfoError(ArangoServerError):
    """Failed to retrieve support info for deployment."""


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


class EdgeCollectionListError(ArangoServerError):
    """Failed to retrieve edge collections."""


class EdgeDefinitionListError(ArangoServerError):
    """Failed to retrieve edge definitions."""


class EdgeDefinitionCreateError(ArangoServerError):
    """Failed to create edge definition."""


class EdgeDefinitionReplaceError(ArangoServerError):
    """Failed to replace edge definition."""


class EdgeDefinitionDeleteError(ArangoServerError):
    """Failed to delete edge definition."""


class EdgeListError(ArangoServerError):
    """Failed to retrieve edges coming in and out of a vertex."""


class FoxxConfigGetError(ArangoServerError):
    """Failed to retrieve Foxx service configuration."""


class FoxxConfigReplaceError(ArangoServerError):
    """Failed to replace Foxx service configuration."""


class FoxxConfigUpdateError(ArangoServerError):
    """Failed to update Foxx service configuration."""


class FoxxCommitError(ArangoServerError):
    """Failed to commit local Foxx service state."""


class FoxxDependencyGetError(ArangoServerError):
    """Failed to retrieve Foxx service dependencies."""


class FoxxDependencyReplaceError(ArangoServerError):
    """Failed to replace Foxx service dependencies."""


class FoxxDependencyUpdateError(ArangoServerError):
    """Failed to update Foxx service dependencies."""


class FoxxScriptListError(ArangoServerError):
    """Failed to retrieve Foxx service scripts."""


class FoxxDevModeEnableError(ArangoServerError):
    """Failed to enable development mode for Foxx service."""


class FoxxDevModeDisableError(ArangoServerError):
    """Failed to disable development mode for Foxx service."""


class FoxxDownloadError(ArangoServerError):
    """Failed to download Foxx service bundle."""


class FoxxReadmeGetError(ArangoServerError):
    """Failed to retrieve Foxx service readme."""


class FoxxScriptRunError(ArangoServerError):
    """Failed to run Foxx service script."""


class FoxxServiceCreateError(ArangoServerError):
    """Failed to create Foxx service."""


class FoxxServiceDeleteError(ArangoServerError):
    """Failed to delete Foxx services."""


class FoxxServiceGetError(ArangoServerError):
    """Failed to retrieve Foxx service metadata."""


class FoxxServiceListError(ArangoServerError):
    """Failed to retrieve Foxx services."""


class FoxxServiceReplaceError(ArangoServerError):
    """Failed to replace Foxx service."""


class FoxxServiceUpdateError(ArangoServerError):
    """Failed to update Foxx service."""


class FoxxSwaggerGetError(ArangoServerError):
    """Failed to retrieve Foxx service swagger."""


class FoxxTestRunError(ArangoServerError):
    """Failed to run Foxx service tests."""


class GraphCreateError(ArangoServerError):
    """Failed to create the graph."""


class GraphDeleteError(ArangoServerError):
    """Failed to delete the graph."""


class GraphListError(ArangoServerError):
    """Failed to retrieve graphs."""


class GraphPropertiesError(ArangoServerError):
    """Failed to retrieve graph properties."""


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


class ServerAvailableOptionsGetError(ArangoServerError):
    """Failed to retrieve available server options."""


class ServerCheckAvailabilityError(ArangoServerError):
    """Failed to retrieve server availability mode."""


class ServerConnectionError(ArangoServerError):
    """Failed to connect to ArangoDB server."""


class ServerCurrentOptionsGetError(ArangoServerError):
    """Failed to retrieve currently-set server options."""


class ServerEncryptionError(ArangoServerError):
    """Failed to reload user-defined encryption keys."""


class ServerEngineError(ArangoServerError):
    """Failed to retrieve database engine."""


class ServerModeError(ArangoServerError):
    """Failed to retrieve server mode."""


class ServerModeSetError(ArangoServerError):
    """Failed to set server mode."""


class ServerLicenseGetError(ArangoServerError):
    """Failed to retrieve server license."""


class ServerLicenseSetError(ArangoServerError):
    """Failed to set server license."""


class ServerStatusError(ArangoServerError):
    """Failed to retrieve server status."""


class ServerTLSError(ArangoServerError):
    """Failed to retrieve TLS data."""


class ServerTLSReloadError(ArangoServerError):
    """Failed to reload TLS."""


class ServerTimeError(ArangoServerError):
    """Failed to retrieve server system time."""


class ServerVersionError(ArangoServerError):
    """Failed to retrieve server version."""


class SortValidationError(ArangoClientError):
    """Invalid sort parameters."""


class TaskCreateError(ArangoServerError):
    """Failed to create server task."""


class TaskDeleteError(ArangoServerError):
    """Failed to delete server task."""


class TaskGetError(ArangoServerError):
    """Failed to retrieve server task details."""


class TaskListError(ArangoServerError):
    """Failed to retrieve server tasks."""


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


class VertexCollectionCreateError(ArangoServerError):
    """Failed to create vertex collection."""


class VertexCollectionDeleteError(ArangoServerError):
    """Failed to delete vertex collection."""


class VertexCollectionListError(ArangoServerError):
    """Failed to retrieve vertex collections."""


class ViewCreateError(ArangoServerError):
    """Failed to create view."""


class ViewDeleteError(ArangoServerError):
    """Failed to delete view."""


class ViewGetError(ArangoServerError):
    """Failed to retrieve view details."""


class ViewListError(ArangoServerError):
    """Failed to retrieve views."""


class ViewRenameError(ArangoServerError):
    """Failed to rename view."""


class ViewReplaceError(ArangoServerError):
    """Failed to replace view."""


class ViewUpdateError(ArangoServerError):
    """Failed to update view."""
