Authentication
--------------

Two HTTP authentication methods are supported out of the box:

1. Basic username and password authentication
2. JSON Web Tokens (JWT)

Basic Authentication
====================

This is the default authentication method.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(
            username="root",
            password="passwd",
            encoding="utf-8"  # Encoding for the password, default is utf-8.
        )

        # Connect to "test" database as root user.
        db = await client.db(
            "test",               # database name
            auth_method="basic",  # use basic authentication (default)
            auth=auth,            # authentication details
            verify=True,          # verify the connection (optional)
        )

JSON Web Tokens (JWT)
=====================

You can obtain the JWT token from the use server using username and password.
Upon expiration, the token gets refreshed automatically and requests are retried.
The client and server clocks must be synchronized for the automatic refresh
to work correctly.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Successful authentication with auth only
        db = await client.db(
            "test",
            auth_method="jwt",
            auth=auth,
            verify=True,
        )

        # Now you have the token on hand.
        token = db.connection.token

        # You can use the token directly.
        db = await client.db("test", auth_method="jwt", token=token, verify=True)

        # In order to allow the token to be automatically refreshed, you should use both auth and token.
        db = await client.db(
            "test",
            auth_method="jwt",
            auth=auth,
            token=token,
            verify=True,
        )

        # Force a token refresh.
        await db.connection.refresh_token()
        new_token = db.connection.token

        # Log in with the first token.
        db2 = await client.db(
            "test",
            auth_method="jwt",
            token=token,
            verify=True,
        )

        # You can manually set tokens.
        db2.connection.token = new_token
        await db2.connection.ping()


If you configured a superuser token, you don't need to provide any credentials.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import JwtToken

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:

        # Generate a JWT token for authentication. You must know the "secret".
        token = JwtToken.generate_token("secret")

        # Superuser authentication, no need for the auth parameter.
        db = await client.db(
            "test",
            auth_method="superuser",
            token=token,
            verify=True
        )
