Server Administration
---------------------

ArangoDB provides operations for server administration and monitoring.
Most of these operations can only be performed by admin users via the
``_system`` database.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "_system" database as root user.
        sys_db = await client.db("_system", auth=auth)

        # Retrieve the database engine.
        await sys_db.engine()
