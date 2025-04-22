Collections
-----------

A **collection** contains :doc:`documents <document>`. It is uniquely identified
by its name which must consist only of hyphen, underscore and alphanumeric
characters. There are three types of collections in python-arango:

* **Standard Collection:** contains regular documents.
* **Vertex Collection:** contains vertex documents for graphs (not supported yet).
* **Edge Collection:** contains edge documents for graphs (not supported yet).


Here is an example showing how you can manage standard collections:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # List all collections in the database.
        await db.collections()

        # Create a new collection named "students" if it does not exist.
        # This returns an API wrapper for "students" collection.
        if await db.has_collection("students"):
            students = db.collection("students")
        else:
            students = await db.create_collection("students")

        # Retrieve collection properties.
        name = students.name
        db_name = students.db_name
        properties = await students.properties()
        count = await students.count()

        # Perform various operations.
        await students.truncate()

        # Delete the collection.
        await db.delete_collection("students")

See :class:`arangoasync.collection.StandardCollection` for API specification.
