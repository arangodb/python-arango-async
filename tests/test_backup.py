import pytest
from packaging import version

from arangoasync.client import ArangoClient
from arangoasync.exceptions import (
    BackupCreateError,
    BackupDeleteError,
    BackupDownloadError,
    BackupGetError,
    BackupRestoreError,
    BackupUploadError,
)


@pytest.mark.asyncio
async def test_backup(url, sys_db_name, bad_db, token, enterprise, cluster, db_version):
    if not enterprise:
        pytest.skip("Backup API is only available in ArangoDB Enterprise Edition")
    if not cluster:
        pytest.skip("For simplicity, the backup API is only tested in cluster setups")
    if db_version < version.parse("3.12.0"):
        pytest.skip(
            "For simplicity, the backup API is only tested in the latest versions"
        )

    with pytest.raises(BackupCreateError):
        await bad_db.backup.create()
    with pytest.raises(BackupGetError):
        await bad_db.backup.get()
    with pytest.raises(BackupRestoreError):
        await bad_db.backup.restore("foobar")
    with pytest.raises(BackupDeleteError):
        await bad_db.backup.delete("foobar")
    with pytest.raises(BackupUploadError):
        await bad_db.backup.upload()
    with pytest.raises(BackupDownloadError):
        await bad_db.backup.download()

    async with ArangoClient(hosts=url) as client:
        db = await client.db(
            sys_db_name, auth_method="superuser", token=token, verify=True
        )
        backup = db.backup
        result = await backup.create()
        backup_id = result["id"]
        result = await backup.get()
        assert "list" in result
        result = await backup.restore(backup_id)
        assert "previous" in result
        config = {"local": {"type": "local"}}
        result = await backup.upload(backup_id, repository="local://tmp", config=config)
        assert "uploadId" in result
        result = await backup.download(
            backup_id, repository="local://tmp", config=config
        )
        assert "downloadId" in result
        await backup.delete(backup_id)
