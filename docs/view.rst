Views
-----

All types of views are supported. . For more information on **view**
management, refer to `ArangoDB Manual`_.

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

        # Retrieve list of views.
        await db.views()

        # Create a view.
        await db.create_view(
            name="foo",
            view_type="arangosearch",
            properties={
                "cleanupIntervalStep": 0,
                "consolidationIntervalMsec": 0
            }
        )

        # Rename a view (not supported in cluster deployments).
        await db.rename_view("foo", "bar")

        # Retrieve view properties.
        await db.view("bar")

        # Retrieve view summary.
        await db.view_info("bar")

        # Partially update view properties.
        await db.update_view(
            name="bar",
            properties={
                "cleanupIntervalStep": 1000,
                "consolidationIntervalMsec": 200
            }
        )

        # Replace view properties. Unspecified ones are reset to default.
        await db.replace_view(
            name="bar",
            properties={"cleanupIntervalStep": 2000}
        )

        # Delete a view.
        await db.delete_view("bar")

For more information on the content of view **properties**,
see `Search Alias Views`_ and `Arangosearch Views`_.

.. _Search Alias Views: https://docs.arango.ai/stable/develop/http-api/views/search-alias-views/
.. _Arangosearch Views: https://docs.arango.ai/stable/develop/http-api/views/arangosearch-views/

Refer to :class:`arangoasync.database.StandardDatabase` class for API specification.
