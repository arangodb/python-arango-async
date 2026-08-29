Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on ``_key`` field by default. This index
cannot be deleted or modified. Every edge collection has additional indexes
on fields ``_from`` and ``_to``. For more information on indexes, refer to
`ArangoDB Manual`_.

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

        # Insert documents with vector embeddings.
        await cities.insert_many([
            {
                "_key": f"city{i}",
                "continent": f"continent{i}",
                "country": f"country{i}",
                "population": i,
                "coordinates": [float(i % 180), float(i % 90)],
                "embedding": [float(i), float(i % 7), float(i % 11), 1.0],
            }
            for i in range(100)
        ])

        # Let ArangoDB determine the number of vector-index centroids.
        vector_index = await cities.add_index(
            type="vector",
            fields=["embedding"],
            options={
                "name": "vector_index",
                "params": {
                    "metric": "cosine",
                    "dimension": 4,
                },
            },
        )

        # Index creation may succeed even if vector training fails.
        if vector_index.training_state != "ready":
            raise RuntimeError(
                vector_index.error_message or "Vector index is not ready"
            )

Omitted or scaling-object ``nLists``, ``numberOfDocsPerCentroid``, factory
placeholders such as ``IVF{},Flat``, and successful-but-unusable creation
behavior require ArangoDB 3.12.10 or later. A successful creation response
means that the index exists, but callers should check ``training_state`` before
using it. If training fails permanently, the state is ``"unusable"`` and
``error_message`` describes the failure.

See :class:`arangoasync.collection.StandardCollection` for API specification.
