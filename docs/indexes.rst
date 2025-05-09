Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge collection has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arangodb.com

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Create a new collection named "cities".
        cities = await db.create_collection("cities")

        # List the indexes in the collection.
        indexes = await cities.indexes()

        # Add a new persistent index on document fields "continent" and "country".
        # Indexes may be added with a name that can be referred to in AQL queries.
        persistent_index = await cities.add_index(
            type="persistent",
            fields=['continent', 'country'],
            options={"unique": True, "name": "continent_country_index"}
        )

        # Add new fulltext indexes on fields "continent" and "country".
        index = await cities.add_index(type="fulltext", fields=["continent"])
        index = await cities.add_index(type="fulltext", fields=["country"])

        # Add a new geo-spatial index on field 'coordinates'.
        index = await cities.add_index(type="geo", fields=["coordinates"])

        # Add a new TTL (time-to-live) index on field 'currency'.
        index = await cities.add_index(type="ttl", fields=["currency"], options={"expireAfter": 200})

        # Delete the last index from the collection.
        await cities.delete_index(index["id"])

See :class:`arangoasync.collection.StandardCollection` for API specification.
