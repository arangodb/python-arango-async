Error Handling
--------------

All python-arango exceptions inherit :class:`arangoasync.exceptions.ArangoError`,
which splits into subclasses :class:`arangoasync.exceptions.ArangoServerError` and
:class:`arangoasync.exceptions.ArangoClientError`.

Server Errors
=============

:class:`arangoasync.exceptions.ArangoServerError` exceptions lightly wrap non-2xx
HTTP responses coming from ArangoDB. Each exception object contains the error
message, error code and HTTP request response details.

Client Errors
=============

:class:`arangoasync.exceptions.ArangoClientError` exceptions originate from
python-arango-async client itself. They do not contain error codes nor HTTP request
response details.

**Example**

.. code-block:: python

    from arangoasync.exceptions import ArangoClientError, ArangoServerError

    try:
        # Some operation that raises an error
    except ArangoClientError:
        # An error occurred on the client side
    except ArangoServerError:
        # An error occurred on the server side
