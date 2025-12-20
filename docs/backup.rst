Backups
-------

Hot Backups are near instantaneous consistent snapshots of an entire ArangoDB deployment.
This includes all databases, collections, indexes, Views, graphs, and users at any given time.
For more information, refer to `ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arango.ai

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import JwtToken

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        token = JwtToken.generate_token(LOGIN_SECRET)

        # Connect to "_system" database as root user.
        db = await client.db(
            "_system", auth_method="superuser", token=token, verify=True
        )

        # Get the backup API wrapper.
        backup = db.backup

        # Create a backup.
        result = await backup.create(
            label="foo",
            allow_inconsistent=True,
            force=False,
            timeout=1000
        )
        backup_id = result["id"]

        # Retrieve details on all backups
        backups = await backup.get()

        # Retrieve details on a specific backup.
        details = await backup.get(backup_id=backup_id)

        # Upload a backup to a remote repository.
        result = await backup.upload(
            backup_id=backup_id,
            repository="local://tmp/backups",
            config={"local": {"type": "local"}}
        )
        upload_id = result["uploadId"]

        # Get status of an upload.
        status = await backup.upload(upload_id=upload_id)

        # Abort an upload.
        await backup.upload(upload_id=upload_id, abort=True)

        # Download a backup from a remote repository.
        result = await backup.download(
            backup_id=backup_id,
            repository="local://tmp/backups",
            config={"local": {"type": "local"}}
        )
        download_id = result["downloadId"]

        # Get status of an download.
        status = await backup.download(download_id=download_id)

        # Abort an download.
        await backup.download(download_id=download_id, abort=True)

        # Restore from a backup.
        await backup.restore(backup_id)

        # Delete a backup.
        await backup.delete(backup_id)

See :class:`arangoasync.backup.Backup` for API specification.
