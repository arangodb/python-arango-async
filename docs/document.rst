Documents
---------

In python-arango-async, a **document** is an object with the following
properties:

* Is JSON serializable.
* May be nested to an arbitrary depth.
* May contain lists.
* Contains the ``_key`` field, which identifies the document uniquely within a
  specific collection.
* Contains the ``_id`` field (also called the *handle*), which identifies the
  document uniquely across all collections within a database. This ID is a
  combination of the collection name and the document key using the format
  ``{collection}/{key}`` (see example below).
* Contains the ``_rev`` field. ArangoDB supports MVCC (Multiple Version
  Concurrency Control) and is capable of storing each document in multiple
  revisions. Latest revision of a document is indicated by this field. The
  field is populated by ArangoDB and is not required as input unless you want
  to validate a document against its current revision.

For more information on documents and associated terminologies, refer to
`ArangoDB Manual`_. Here is an example of a valid document in "students"
collection:

.. _ArangoDB Manual: https://docs.arangodb.com

.. code-block:: json

    {
        "_id": "students/bruce",
        "_key": "bruce",
        "_rev": "_Wm3dzEi--_",
        "first_name": "Bruce",
        "last_name": "Wayne",
        "address": {
            "street" : "1007 Mountain Dr.",
            "city": "Gotham",
            "state": "NJ"
        },
        "is_rich": true,
        "friends": ["robin", "gordon"]
    }

.. _edge-documents:

**Edge documents (edges)** are similar to standard documents but with two
additional required fields ``_from`` and ``_to``. Values of these fields must
be the handles of "from" and "to" vertex documents linked by the edge document
in question (see :doc:`graph` for details). Edge documents are contained in
:ref:`edge collections <edge-collections>`. Here is an example of a valid edge
document in "friends" edge collection:

.. code-block:: python

    {
        "_id": "friends/001",
        "_key": "001",
        "_rev": "_Wm3d4le--_",
        "_fro"': "students/john",
        "_to": "students/jane",
        "closeness": 9.5
    }

Standard documents are managed via collection API wrapper:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        # Create some test documents to play around with.
        lola = {"_key": "lola", "GPA": 3.5, "first": "Lola", "last": "Martin"}
        abby = {"_key": "abby", "GPA": 3.2, "first": "Abby", "last": "Page"}
        john = {"_key": "john", "GPA": 3.6, "first": "John", "last": "Kim"}
        emma = {"_key": "emma", "GPA": 4.0, "first": "Emma", "last": "Park"}

        # Insert a new document. This returns the document metadata.
        metadata = await students.insert(lola)
        assert metadata["_id"] == "students/lola"
        assert metadata["_key"] == "lola"

        # Insert multiple documents.
        await students.insert_many([abby, john, emma])

        # Check if documents exist in the collection.
        assert await students.has("lola")

        # Retrieve the total document count.
        count = await students.count()

        # Retrieve one or more matching documents.
        async for student in await students.find({"first": "John"}):
            assert student["_key"] == "john"
            assert student["GPA"] == 3.6
            assert student["last"] == "Kim"

        # Retrieve one or more matching documents, sorted by a field.
        async for student in await students.find({"first": "John"}, sort=[{"sort_by": "GPA", "sort_order": "DESC"}]):
            assert student["_key"] == "john"
            assert student["GPA"] == 3.6
            assert student["last"] == "Kim"

        # Retrieve a document by key.
        await students.get("john")

        # Retrieve a document by ID.
        await students.get("students/john")

        # Retrieve a document by body with "_id" field.
        await students.get({"_id": "students/john"})

        # Retrieve a document by body with "_key" field.
        await students.get({"_key": "john"})

        # Retrieve multiple documents by ID, key or body.
        await students.get_many(["abby", "students/lola", {"_key": "john"}])

        # Update a single document.
        lola["GPA"] = 2.6
        await students.update(lola)

        # Update one or more matching documents.
        await students.update_match({"last": "Park"}, {"GPA": 3.0})

        # Replace a single document.
        emma["GPA"] = 3.1
        await students.replace(emma)

        # Replace one or more matching documents.
        becky = {"first": "Becky", "last": "Solis", "GPA": "3.3"}
        await students.replace_match({"first": "Emma"}, becky)

        # Delete a document by body with "_id" or "_key" field.
        await students.delete(emma)

        # Delete multiple documents. Missing ones are ignored.
        await students.delete_many([abby, emma])

        # Delete one or more matching documents.
        await students.delete_match({"first": "Emma"})

Importing documents in bulk is faster when using specialized methods. Suppose
our data is in a file containing JSON Lines (JSONL) format. Each line is expected
to be one JSON object. Example of a "students.jsonl" file:

.. code-block:: json

    {"_key":"john","name":"John Smith","age":35}
    {"_key":"katie","name":"Katie Foster","age":28}

To import this file into the "students" collection, we can use the `import_bulk` API:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    import aiofiles

    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        # Read the JSONL file asynchronously.
        async with aiofiles.open('students.jsonl', mode='r') as f:
            documents = await f.read()

        # Import documents in bulk.
        result = await students.import_bulk(documents, doc_type="documents")

You can manage documents via database API wrappers also, but only simple
operations (i.e. get, insert, update, replace, delete) are supported and you
must provide document IDs instead of keys:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Create a new collection named "students" if it does not exist.
        if not await db.has_collection("students"):
            await db.create_collection("students")

        # Create some test documents to play around with.
        # The documents must have the "_id" field instead.
        lola = {"_id": "students/lola", "GPA": 3.5}
        abby = {"_id": "students/abby", "GPA": 3.2}
        john = {"_id": "students/john", "GPA": 3.6}
        emma = {"_id": "students/emma", "GPA": 4.0}

        # Insert a new document.
        metadata = await db.insert_document("students", lola)
        assert metadata["_id"] == "students/lola"
        assert metadata["_key"] == "lola"

        # Check if a document exists.
        assert await db.has_document(lola) is True

        # Get a document (by ID or body with "_id" field).
        await db.document("students/lola")
        await db.document(abby)

        # Update a document.
        lola["GPA"] = 3.6
        await db.update_document(lola)

        # Replace a document.
        lola["GPA"] = 3.4
        await db.replace_document(lola)

        # Delete a document (by ID or body with "_id" field).
        await db.delete_document("students/lola")

See :class:`arangoasync.database.StandardDatabase` and :class:`arangoasync.collection.StandardCollection` for API specification.
