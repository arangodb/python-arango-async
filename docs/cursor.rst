Cursors
-------

Many operations provided by the driver (e.g. executing :doc:`aql` queries)
return result **cursors** to batch the network communication between ArangoDB
server and the client. Each HTTP request from a cursor fetches the
next batch of results (usually documents). Depending on the query, the total
number of items in the result set may or may not be known in advance.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Set up some test data to query against.
        await db.collection("students").insert_many([
            {"_key": "Abby", "age": 22},
            {"_key": "John", "age": 18},
            {"_key": "Mary", "age": 21},
            {"_key": "Suzy", "age": 23},
            {"_key": "Dave", "age": 20}
        ])

        # Execute an AQL query which returns a cursor object.
        cursor = await db.aql.execute(
            "FOR doc IN students FILTER doc.age > @val RETURN doc",
            bind_vars={"val": 17},
            batch_size=2,
            count=True
        )

        # Get the cursor ID.
        cid = cursor.id

        # Get the items in the current batch.
        batch = cursor.batch

        # Check if the current batch is empty.
        is_empty = cursor.empty()

        # Get the total count of the result set.
        cnt = cursor.count

        # Flag indicating if there are more to be fetched from server.
        has_more = cursor.has_more

        # Flag indicating if the results are cached.
        is_cached = cursor.cached

        # Get the cursor statistics.
        stats = cursor.statistics

        # Get the performance profile.
        profile = cursor.profile

        # Get any warnings produced from the query.
        warnings = cursor.warnings

        # Return the next item from the cursor. If current batch is depleted, the
        # next batch is fetched from the server automatically.
        await cursor.next()

        # Return the next item from the cursor. If current batch is depleted, an
        # exception is thrown. You need to fetch the next batch manually.
        cursor.pop()

        # Fetch the next batch and add them to the cursor object.
        await cursor.fetch()

        # Delete the cursor from the server.
        await cursor.close()

See :class:`arangoasync.cursor.Cursor` for API specification.

Cursors can be used together with a context manager to ensure that the resources get freed up
when the cursor is no longer needed. Asynchronous iteration is also supported, allowing you to
iterate over the cursor results without blocking the event loop.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.exceptions import CursorCloseError

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Set up some test data to query against.
        await db.collection("students").insert_many([
            {"_key": "Abby", "age": 22},
            {"_key": "John", "age": 18},
            {"_key": "Mary", "age": 21},
            {"_key": "Suzy", "age": 23},
            {"_key": "Dave", "age": 20}
        ])

        # Execute an AQL query which returns a cursor object.
        cursor = await db.aql.execute(
            "FOR doc IN students FILTER doc.age > @val RETURN doc",
            bind_vars={"val": 17},
            batch_size=2,
            count=True
        )

        # Iterate over the cursor in an async context manager.
        async with cursor as ctx:
            async for student in ctx:
                print(student)

        # The cursor is automatically closed when exiting the context manager.
        try:
            await cursor.close()
        except CursorCloseError:
            print(f"Cursor already closed!")

If the fetched result batch is depleted while you are iterating over a cursor
(or while calling the method :func:`arangoasync.cursor.Cursor.next`), the driver
automatically sends an HTTP request to the server in order to fetch the next batch
(just-in-time style). To control exactly when the fetches occur, you can use
methods like :func:`arangoasync.cursor.Cursor.fetch` and :func:`arangoasync.cursor.Cursor.pop`
instead.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Set up some test data to query against.
        await db.collection("students").insert_many([
            {"_key": "Abby", "age": 22},
            {"_key": "John", "age": 18},
            {"_key": "Mary", "age": 21}
        ])

        # You can manually fetch and pop for finer control.
        cursor = await db.aql.execute("FOR doc IN students RETURN doc", batch_size=1)
        while cursor.has_more: # Fetch until nothing is left on the server.
            await cursor.fetch()
        while not cursor.empty(): # Pop until nothing is left on the cursor.
            student = cursor.pop()
            print(student)

You can use the `allow_retry` parameter of :func:`arangoasync.aql.AQL.execute`
to automatically retry the request if the cursor encountered any issues during
the previous fetch operation. Note that this feature causes the server to
cache the last batch. To allow re-fetching of the very last batch of the query,
the server cannot automatically delete the cursor. Once you have successfully
received the last batch, you should call :func:`arangoasync.cursor.Cursor.close`,
or use a context manager to ensure the cursor is closed properly.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.typings import QueryProperties

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Set up some test data to query against.
        await db.collection("students").insert_many([
            {"_key": "Abby", "age": 22},
            {"_key": "John", "age": 18},
            {"_key": "Mary", "age": 21}
        ])

        cursor = await db.aql.execute(
            "FOR doc IN students RETURN doc",
            batch_size=1,
            options=QueryProperties(allow_retry=True)
        )

        while cursor.has_more:
            try:
                await cursor.fetch()
            except ConnectionError:
                # Retry the request.
                continue

        while not cursor.empty():
            student = cursor.pop()
            print(student)

        # Delete the cursor from the server.
        await cursor.close()

For more information about various query properties, see :class:`arangoasync.typings.QueryProperties`.
