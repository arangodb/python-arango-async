Authentication
--------------

Two HTTP authentication methods are supported out of the box:

1. Basic username and password authentication
2. JSON Web Tokens (JWT)

Basic Authentication
====================

This is the default authentication method.

Example:

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

JWTs are sent in the HTTP ``Authorization`` header as Bearer tokens.
Use ``auth_method="jwt"`` with credentials, a token, or both.

Tokens with renewal credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Provide ``auth`` to allow the driver to obtain and renew tokens. Without an
initial ``token``, acquisition happens on the first database request, not when
``await client.db()`` returns. With ``verify=True``, a test request during
``await client.db()`` triggers acquisition, and acquisition failures propagate
to the caller.

If both ``auth`` and ``token`` are provided, the driver uses the supplied token
initially and retains the credentials for renewal.

For each database request, the renewal policy is:

1. Send the request with the current token.
2. If the response has HTTP status 401 and the token is past its locally
   recorded expiration, obtain a replacement using the retained credentials.
3. Retry the original request once with the replacement token. A second
   rejection is returned to the operation's response handler without another
   renewal or replay.

If acquisition fails, the original request is not replayed and the acquisition
error propagates. Authentication failures during acquisition raise
:class:`arangoasync.exceptions.JWTRefreshError`.

The renewal decision does not check the ArangoDB error code. A 401 received
while the token is locally unexpired does not trigger renewal. Other HTTP
statuses, including 403, do not trigger renewal either; the operation's response
handler determines the resulting error.

Renewal happens after a qualifying rejection, not proactively or on a timer.
The driver does not fall back to Basic authentication. Keep client and server
clocks synchronized: local expiration checks affect token validation and
whether a rejection triggers renewal.

Example:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")
        db = await client.db(
            "test", auth_method="jwt", auth=auth, verify=True
        )
        token = db.connection.token

        # Reuse an initial token and retain credentials for renewal.
        db = await client.db(
            "test", auth_method="jwt", auth=auth, token=token, verify=True
        )

        # Explicit refresh obtains a replacement using the credentials.
        await db.connection.refresh_token()

Tokens without renewal credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Provide ``token`` without ``auth`` when the application supplies replacements.
Pass a :class:`arangoasync.auth.JwtToken`, which checks expiration locally when
constructed. An expired token raises ``jwt.exceptions.ExpiredSignatureError``.

A token-only connection has no credentials to obtain a replacement. Currently,
a 401 received after local expiration still enters the refresh path and raises
:class:`arangoasync.exceptions.JWTRefreshError` before making an acquisition
request. A 401 received before local expiration goes to the operation's response
handler. Applications must replace tokens themselves.

Assign a new ``JwtToken`` to ``db.connection.token`` to replace the token.
Existing database, collection, and cursor wrappers sharing that connection use
the replacement on subsequent requests. Separate ``client.db()`` calls create
independent connections, even on the same client.

Example:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import JwtToken

    # These strings come from the application's token provider.
    token = JwtToken(encoded_token)
    async with ArangoClient(hosts="http://localhost:8529") as client:
        db = await client.db(
            "test", auth_method="jwt", token=token, verify=True
        )
        db.connection.token = JwtToken(replacement_encoded_token)
        await db.connection.ping()

Superuser tokens
~~~~~~~~~~~~~~~~

Use ``auth_method="superuser"`` with a ``JwtToken``. The ``auth`` parameter is
ignored and the driver never renews the token or replays a rejected request for
JWT recovery. The same local validation applies when constructing a
``JwtToken`` for user or superuser access. Replace a superuser token through
``db.connection.token`` as shown above.

With ``verify=True``, a rejected superuser token raises
:class:`arangoasync.exceptions.ServerConnectionError` during ``await client.db()``.

Example:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import JwtToken

    async with ArangoClient(hosts="http://localhost:8529") as client:
        # Generating a superuser token requires the server's JWT secret.
        token = JwtToken.generate_token("secret")
        db = await client.db(
            "test", auth_method="superuser", token=token, verify=True
        )
