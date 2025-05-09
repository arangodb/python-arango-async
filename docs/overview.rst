Getting Started
---------------

Here is an example showing how **python-arango-async** client can be used:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "_system" database as root user.
        sys_db = await client.db("_system", auth=auth)

        # Create a new database named "test".
        await sys_db.create_database("test")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Create a new collection named "students".
        students = await db.create_collection("students")

        # Add a persistent index to the collection.
        await students.add_index(type="persistent", fields=["name"], options={"unique": True})

        # Insert new documents into the collection.
        await students.insert({"name": "jane", "age": 39})
        await students.insert({"name": "josh", "age": 18})
        await students.insert({"name": "judy", "age": 21})

        # Execute an AQL query and iterate through the result cursor.
        cursor = await db.aql.execute("FOR doc IN students RETURN doc")
        async with cursor:
            student_names = []
            async for doc in cursor:
                student_names.append(doc["name"])

You may also use the client without a context manager, but you must ensure to close the client when done:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    client = ArangoClient(hosts="http://localhost:8529")
    auth = Auth(username="root", password="passwd")
    sys_db = await client.db("_system", auth=auth)

    # Create a new database named "test".
    await sys_db.create_database("test")

    # Connect to "test" database as root user.
    db = await client.db("test", auth=auth)

    # List all collections in the "test" database.
    collections = await db.collections()

    # Close the client when done.
    await client.close()
