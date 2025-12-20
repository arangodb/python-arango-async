Analyzers
---------

For more information on analyzers, refer to `ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arango.ai

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Create an analyzer.
        await db.create_analyzer(
            name='test_analyzer',
            analyzer_type='identity',
            properties={},
            features=[]
        )

        # Retrieve the created analyzer.
        analyzer = await db.analyzer('test_analyzer')

        # Retrieve list of analyzers.
        await db.analyzers()

        # Delete an analyzer.
        await db.delete_analyzer('test_analyzer', ignore_missing=True)

Refer to :class:`arangoasync.database.StandardDatabase` class for API specification.
