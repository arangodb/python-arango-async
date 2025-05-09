Logging
-------

If if helps to debug your application, you can enable logging to see all the requests sent by the driver to the ArangoDB server.

.. code-block:: python

    import logging
    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.logger import logger

    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(level=logging.DEBUG)

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        # Insert a document into the collection.
        await students.insert({"name": "John Doe", "age": 25})

The insert generates a log message similar to: `DEBUG:arangoasync:Sending request to host 0 (0): <POST /_db/_system/_api/document/students>`.
