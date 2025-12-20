__all__ = ["Backup"]

from numbers import Number
from typing import Optional, cast

from arangoasync.exceptions import (
    BackupCreateError,
    BackupDeleteError,
    BackupDownloadError,
    BackupGetError,
    BackupRestoreError,
    BackupUploadError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons


class Backup:
    """Backup API wrapper."""

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    async def get(self, backup_id: Optional[str] = None) -> Result[Json]:
        """Return backup details.

        Args:
            backup_id (str | None): If set, the returned list is restricted to the
                backup with the given id.

        Returns:
            dict: Backup details.

        Raises:
            BackupGetError: If the operation fails.

        References:
            - `list-backups <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#list-all-backups>`__
        """  # noqa: E501
        data: Json = {}
        if backup_id is not None:
            data["id"] = backup_id

        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/list",
            data=self.serializer.dumps(data) if data else None,
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise BackupGetError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def create(
        self,
        label: Optional[str] = None,
        allow_inconsistent: Optional[bool] = None,
        force: Optional[bool] = None,
        timeout: Optional[Number] = None,
    ) -> Result[Json]:
        """Create a backup when the global write lock can be obtained.

        Args:
            label (str | None): Label for this backup. If not specified, a UUID is used.
            allow_inconsistent (bool | None): Allow inconsistent backup when the global
                transaction lock cannot be acquired before timeout.
            force (bool | None): Forcefully abort all running transactions to ensure a
                consistent backup when the global transaction lock cannot be
                acquired before timeout. Default (and highly recommended) value
                is `False`.
            timeout (float | None): The time in seconds that the operation tries to
                get a consistent snapshot.

        Returns:
            dict: Backup information.

        Raises:
            BackupCreateError: If the backup creation fails.

        References:
            - `create-backup <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#create-a-backup>`__
        """  # noqa: E501
        data: Json = {}
        if label is not None:
            data["label"] = label
        if allow_inconsistent is not None:
            data["allowInconsistent"] = allow_inconsistent
        if force is not None:
            data["force"] = force
        if timeout is not None:
            data["timeout"] = timeout

        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/create",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise BackupCreateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def restore(self, backup_id: str) -> Result[Json]:
        """Restore a local backup.

        Args:
            backup_id (str): Backup ID.

        Returns:
            dict: Result of the restore operation.

        Raises:
            BackupRestoreError: If the restore operation fails.

        References:
            - `restore-backup <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#restore-a-backup>`__
        """  # noqa: E501
        data: Json = {"id": backup_id}
        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/restore",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise BackupRestoreError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def delete(self, backup_id: str) -> None:
        """Delete a backup.

        Args:
            backup_id (str): Backup ID.

        Raises:
            BackupDeleteError: If the delete operation fails.

        References:
            - `delete-backup <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#delete-a-backup>`__
        """  # noqa: E501
        data: Json = {"id": backup_id}
        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/delete",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise BackupDeleteError(resp, request)

        await self._executor.execute(request, response_handler)

    async def upload(
        self,
        backup_id: Optional[str] = None,
        repository: Optional[str] = None,
        abort: Optional[bool] = None,
        config: Optional[Json] = None,
        upload_id: Optional[str] = None,
    ) -> Result[Json]:
        """Manage backup uploads.

        Args:
            backup_id (str | None): Backup ID used for scheduling an upload. Mutually
                exclusive with parameter **upload_id**.
            repository (str | None): Remote repository URL(e.g. "local://tmp/backups").
            abort (str | None): If set to `True`, running upload is aborted. Used with
                parameter **upload_id**.
            config (dict | None): Remote repository configuration. Required for scheduling
                an upload and mutually exclusive with parameter **upload_id**.
            upload_id (str | None): Upload ID. Mutually exclusive with parameters
                **backup_id**, **repository**, and **config**.

        Returns:
            dict: Upload details.

        Raises:
            BackupUploadError: If upload operation fails.

        References:
            - `upload-a-backup-to-a-remote-repository <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#upload-a-backup-to-a-remote-repository>`__
        """  # noqa: E501
        data: Json = {}
        if upload_id is not None:
            data["uploadId"] = upload_id
        if backup_id is not None:
            data["id"] = backup_id
        if repository is not None:
            data["remoteRepository"] = repository
        if abort is not None:
            data["abort"] = abort
        if config is not None:
            data["config"] = config

        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/upload",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise BackupUploadError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def download(
        self,
        backup_id: Optional[str] = None,
        repository: Optional[str] = None,
        abort: Optional[bool] = None,
        config: Optional[Json] = None,
        download_id: Optional[str] = None,
    ) -> Result[Json]:
        """Manage backup downloads.

        Args:
            backup_id (str | None): Backup ID used for scheduling a download. Mutually
                exclusive with parameter **download_id**.
            repository (str | None): Remote repository URL (e.g. "local://tmp/backups").
            abort (bool | None): If set to `True`, running download is aborted.
            config (dict | None): Remote repository configuration. Required for scheduling
                a download and mutually exclusive with parameter **download_id**.
            download_id (str | None): Download ID. Mutually exclusive with parameters
                **backup_id**, **repository**, and **config**.

        Returns:
            dict: Download details.

        Raises:
            BackupDownloadError: If the download operation fails.

        References:
            - `download-a-backup-from-a-remote-repository <https://docs.arango.ai/arangodb/stable/develop/http-api/hot-backups/#download-a-backup-from-a-remote-repository>`__
        """  # noqa: E501
        data: Json = {}
        if download_id is not None:
            data["downloadId"] = download_id
        if backup_id is not None:
            data["id"] = backup_id
        if repository is not None:
            data["remoteRepository"] = repository
        if abort is not None:
            data["abort"] = abort
        if config is not None:
            data["config"] = config

        request = Request(
            method=Method.POST,
            endpoint="/_admin/backup/download",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise BackupDownloadError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)
