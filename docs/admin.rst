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

        # Retrieve the server time..
        time = await sys_db.time()

        # Check server availability
        availability = sys_db.check_availability()

        # Support info
        info = sys_db.support_info()

        # Get the startup option configuration
        options = await sys_db.options()

        # Get the available startup options
        options = await sys_db.options_available()

        # Return whether or not a server is in read-only mode
        mode = await sys_db.mode()
