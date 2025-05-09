TLS
---

When you need fine-grained control over TLS settings, you build a Python
:class:`ssl.SSLContext` and hand it to the :class:`arangoasync.http.DefaultHTTPClient` class.
Here are the most common patterns.


Basic client-side HTTPS with default settings
=============================================

Create a “secure by default” client context. This will verify server certificates against your
OS trust store and check hostnames.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.http import DefaultHTTPClient
    import ssl

    # Create a default client context.
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    http_client = DefaultHTTPClient(ssl_context=ssl_ctx)

    # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="https://localhost:8529",
        http_client=http_client,
    )

Custom CA bundle
================

If you have a custom CA file, this allows you to trust the private CA.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.http import DefaultHTTPClient
    import ssl

    # Use a custom CA bundle.
    ssl_ctx = ssl.create_default_context(cafile="path/to/ca.pem")
    http_client = DefaultHTTPClient(ssl_context=ssl_ctx)

    # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="https://localhost:8529",
        http_client=http_client,
    )

Disabling certificate verification
==================================

If you want to disable *all* certification checks (not recommended), create an unverified
context.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.http import DefaultHTTPClient
    import ssl

    # Disable certificate verification.
    ssl_ctx = ssl._create_unverified_context()
    http_client = DefaultHTTPClient(ssl_context=ssl_ctx)

    # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="https://localhost:8529",
        http_client=http_client,
    )

Use a client certificate chain
==============================

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.http import DefaultHTTPClient
    import ssl

    # Load a certificate chain.
    ssl_ctx = ssl.create_default_context(cafile="path/to/ca.pem")
    ssl_ctx.load_cert_chain(certfile="path/to/cert.pem", keyfile="path/to/key.pem")
    http_client = DefaultHTTPClient(ssl_context=ssl_ctx)

    # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="https://localhost:8529",
        http_client=http_client,
    )

.. note::
    For best performance, re-use one SSLContext across many requests/sessions to amortize handshake cost.

If you want to have fine-grained control over the HTTP connection, you should define
your HTTP client as described in the :ref:`HTTP` section.
