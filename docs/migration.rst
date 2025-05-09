Coming from python-arango
-------------------------

Generally, migrating from `python-arango`_ should be a smooth transition. For the most part, the API is similar,
but there are a few things to note._

Helpers
=======

The current driver comes with :ref:`Helpers`, because we want to:

1. Facilitate better type hinting and auto-completion in IDEs.
2. Ensure an easier 1-to-1 mapping of the ArangoDB API.

For example, coming from the synchronous driver, creating a new user looks like this:

.. code-block:: python

    sys_db.create_user(
        username="johndoe@gmail.com",
        password="first_password",
        active=True,
        extra={"team": "backend", "title": "engineer"}
    )

In the asynchronous driver, it looks like this:

.. code-block:: python

    from arangoasync.typings import UserInfo

    user_info = UserInfo(
        username="johndoe@gmail.com",
        password="first_password",
        active=True,
        extra={"team": "backend", "title": "engineer"}
    )
    await sys_db.create_user(user_info)

CamelCase vs. snake_case
========================

Upon returning results, for the most part, the synchronous driver mostly tries to stick to snake case. Unfortunately,
this is not always consistent.

.. code-block:: python

    status = db.status()
    assert "host" in status
    assert "operation_mode" in status

The asynchronous driver, however, tries to stick to a simple rule:

* If the API returns a camel case key, it will be returned as is.
* Parameters passed from client to server use the snake case equivalent of the camel case keys required by the API
  (e.g. `userName` becomes `user_name`). This is done to ensure PEP8 compatibility.

.. code-block:: python

    from arangoasync.typings import ServerStatusInformation

    status: ServerStatusInformation = await db.status()
    assert "host" in status
    assert "operationMode" in status
    print(status.host)
    print(status.operation_mode)

You can use the :func:`arangoasync.typings.JsonWrapper.format` method to gain more control over the formatting of
keys.

Serialization
=============

Check out the :ref:`Serialization` section to learn more about how to implement your own serializer/deserializer. The
current driver makes use of generic types and allows for a higher degree of customization.

Mixing sync and async
=====================

Sometimes you may need to mix the two. This is not recommended, but it takes time to migrate everything. If you need to
do this, you can use the :func:`asyncio.to_thread` function to run a synchronous function in separate thread, without
compromising the async event loop.

.. code-block:: python

    # Use a python-arango synchronous client
    sys_db = await asyncio.to_thread(
        client.db,
        "_system",
        username="root",
        password="passwd"
    )

.. _python-arango: https://docs.python-arango.com
