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

JWTs are sent as Bearer tokens in the HTTP ``Authorization`` header.
Use ``auth_method="jwt"`` with credentials, a token, or both.

Tokens with renewal credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Provide ``auth`` to allow token acquisition and renewal. Without an initial
``token``, acquisition happens on the first request; ``verify=True`` triggers
that request during ``await client.db()``. Providing both ``auth`` and ``token``
uses the supplied token initially and retains credentials for renewal.

For each database request, the renewal policy is:

1. Send the request with the current token.
2. If the response has HTTP status 401 and the token is past its locally
   recorded expiration, obtain a replacement using the retained credentials.
3. Retry the original request once with the replacement token. A second
   rejection is returned to the operation's response handler without another
   renewal or replay.

If acquisition fails, the original request is not replayed and the error
propagates. Authentication failures during acquisition raise
:class:`arangoasync.exceptions.JWTRefreshError`.

The ArangoDB error code is not checked; other responses do not trigger renewal.
There is no proactive renewal or Basic authentication fallback.
Keep client and server clocks synchronized for local expiration checks.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    async with ArangoClient(hosts="http://localhost:8529") as client:
        db = await client.db(
            "test", auth_method="jwt",
            auth=Auth(username="root", password="passwd"), verify=True
        )
        # Explicit refresh uses the retained credentials.
        await db.connection.refresh_token()

Tokens without renewal credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Provide ``token`` without ``auth`` when the application supplies replacements.
Pass a :class:`arangoasync.auth.JwtToken`; constructing one with an expired token
raises ``jwt.exceptions.ExpiredSignatureError``. A token-only connection receiving
HTTP 401 after local expiry currently raises
:class:`arangoasync.exceptions.JWTRefreshError` because it lacks renewal credentials.

Replace tokens through ``db.connection.token``. Existing wrappers sharing that
connection use the replacement on subsequent requests; separate ``client.db()``
calls remain independent.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import JwtToken

    # Token strings come from the application's provider.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        db = await client.db(
            "test", auth_method="jwt", token=JwtToken(encoded_token), verify=True
        )
        db.connection.token = JwtToken(replacement_encoded_token)

Superuser tokens
~~~~~~~~~~~~~~~~

Use ``auth_method="superuser"`` with a ``JwtToken``. The driver ignores ``auth``
and performs no automatic renewal or replay. Token validation and replacement
work as above. With ``verify=True``, a rejected superuser token raises
:class:`arangoasync.exceptions.ServerConnectionError` during ``await client.db()``.

Within a client context, generate a token using the server's JWT secret:

.. code-block:: python

    token = JwtToken.generate_token("secret")
    db = await client.db(
        "test", auth_method="superuser", token=token, verify=True
    )
