Databases
---------

ArangoDB server can have an arbitrary number of **databases**. Each database
has its own set of :doc:`collections <collection>` and graphs.
There is a special database named ``_system``, which cannot be dropped and
provides operations for managing users, permissions and other databases. Most
of the operations can only be executed by admin users. See :doc:`user` for more
information.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "_system" database as root user.
        sys_db = await client.db("_system", auth=auth)

        # List all databases.
        await sys_db.databases()

        # Create a new database named "test" if it does not exist.
        # Only root user has access to it at time of its creation.
        if not await sys_db.has_database("test"):
            await sys_db.create_database("test")

        # Delete the database.
        await sys_db.delete_database("test")

        # Create a new database named "test" along with a new set of users.
        # Only "jane", "john", "jake" and root user have access to it.
        if not await sys_db.has_database("test"):
            await sys_db.create_database(
            name="test",
            users=[
                {"username": "jane", "password": "foo", "active": True},
                {"username": "john", "password": "bar", "active": True},
                {"username": "jake", "password": "baz", "active": True},
            ],
        )

        # Connect to the new "test" database as user "jane".
        db = await client.db("test", auth=Auth("jane", "foo"))

        # Make sure that user "jane" has read and write permissions.
        await sys_db.update_permission(username="jane", permission="rw", database="test")

        # Retrieve various database and server information.
        name = db.name
        version = await db.version()
        status = await db.status()
        collections = await db.collections()

        # Delete the database. Note that the new users will remain.
        await sys_db.delete_database("test")

See :class:`arangoasync.client.ArangoClient` and :class:`arangoasync.database.StandardDatabase` for API specification.
