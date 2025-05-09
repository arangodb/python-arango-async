.. _Helpers:

Helper Types
------------

The driver comes with a set of helper types and wrappers to make it easier to work with the ArangoDB API. These are
designed to behave like dictionaries, but with some additional features and methods. See the :class:`arangoasync.typings.JsonWrapper` class for more details.

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

        properties = QueryProperties(
            allow_dirty_reads=True,
            allow_retry=False,
            fail_on_warning=True,
            fill_block_cache=False,
            full_count=True,
            intermediate_commit_count=1000,
            intermediate_commit_size=1048576,
            max_dnf_condition_members=10,
            max_nodes_per_callstack=100,
            max_number_of_plans=5,
            max_runtime=60.0,
            max_transaction_size=10485760,
            max_warning_count=10,
            optimizer={"rules": ["-all", "+use-indexes"]},
            profile=1,
            satellite_sync_wait=10.0,
            skip_inaccessible_collections=True,
            spill_over_threshold_memory_usage=10485760,
            spill_over_threshold_num_rows=100000,
            stream=True,
            use_plan_cache=True,
        )

        # The types are fully serializable.
        print(properties)

        await db.aql.execute(
            "FOR doc IN students RETURN doc",
            batch_size=1,
            options=properties,
        )

You can easily customize the data representation using formatters. By default, keys are in the format used by the ArangoDB
API, but you can change them to snake_case if you prefer. See :func:`arangoasync.typings.JsonWrapper.format` for more details.

**Example:**

.. code-block:: python

    from arangoasync.typings import Json, UserInfo

    data = {
        "user": "john",
        "password": "secret",
        "active": True,
        "extra": {"role": "admin"},
    }
    user_info = UserInfo(**data)

    def uppercase_formatter(data: Json) -> Json:
        result: Json = {}
        for key, value in data.items():
            result[key.upper()] = value
        return result

    print(user_info.format(uppercase_formatter))

Helpers
=======

Below are all the available helpers.

.. automodule:: arangoasync.typings
    :members:
