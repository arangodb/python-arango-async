Compression
------------

The :class:`arangoasync.client.ArangoClient` lets you define the preferred compression policy for request and responses. By default
compression is disabled. You can change this by passing the `compression` parameter when creating the client. You may use
:class:`arangoasync.compression.DefaultCompressionManager` or a custom subclass of :class:`arangoasync.compression.CompressionManager`.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.compression import DefaultCompressionManager

    client = ArangoClient(
        hosts="http://localhost:8529",
        compression=DefaultCompressionManager(),
    )

Furthermore, you can customize the request compression policy by defining the minimum size of the request body that
should be compressed and the desired compression level. Or, in order to explicitly disable compression, you can set the
threshold parameter to -1.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.compression import DefaultCompressionManager

    # Disable request compression.
    client1 = ArangoClient(
        hosts="http://localhost:8529",
        compression=DefaultCompressionManager(threshold=-1),
    )

    # Enable request compression with a minimum size of 2 KB and a compression level of 8.
    client2 = ArangoClient(
        hosts="http://localhost:8529",
        compression=DefaultCompressionManager(threshold=2048, level=8),
    )

You can set the `accept` parameter in order to inform the server that the client prefers compressed responses (in the form
of an *Accept-Encoding* header). By default the `DefaultCompressionManager` is configured to accept responses compressed using
the *deflate* algorithm. Note that the server may or may not honor this preference, depending on how it is
configured. This can be controlled by setting the `--http.compress-response-threshold` option to a value greater than 0
when starting the ArangoDB server.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.compression import AcceptEncoding, DefaultCompressionManager

    # Accept compressed responses explicitly.
    client = ArangoClient(
        hosts="http://localhost:8529",
        compression=DefaultCompressionManager(accept=AcceptEncoding.DEFLATE),
    )

See the :class:`arangoasync.compression.CompressionManager` class for more details on how to customize the compression policy.
