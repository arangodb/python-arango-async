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

See :class:`arangoasync.database.StandardDatabase` and :class:`arangoasync.collection.StandardCollection` for API specification.
