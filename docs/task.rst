Tasks
-----

ArangoDB can schedule user-defined Javascript snippets as one-time or periodic
(re-scheduled after each execution) tasks. Tasks are executed in the context of
the database they are defined in.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Create a new task which simply prints parameters.
        await db.create_task(
            name="test_task",
            command="""
                var task = function(params){
                    var db = require('@arangodb');
                    db.print(params);
                }
                task(params);
            """,
            params={"foo": "bar"},
            offset=300,
            period=10,
            task_id="001"
        )

        # List all active tasks
        tasks = await db.tasks()

        # Retrieve details of a task by ID.
        details = await db.task("001")

        # Delete an existing task by ID.
        await db.delete_task('001', ignore_missing=True)


.. note::
    When deleting a database, any tasks that were initialized under its context
    remain active. It is therefore advisable to delete any running tasks before
    deleting the database.
