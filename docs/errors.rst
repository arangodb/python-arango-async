Error Handling
--------------

All python-arango exceptions inherit :class:`arangoasync.exceptions.ArangoError`,
which splits into subclasses :class:`arangoasync.exceptions.ArangoServerError` and
:class:`arangoasync.exceptions.ArangoClientError`.

**Example**

.. code-block:: python

    from arangoasync.exceptions import ArangoClientError, ArangoServerError

    try:
        # Some operation that raises an error
    except ArangoClientError:
        # An error occurred on the client side
    except ArangoServerError:
        # An error occurred on the server side


Server Errors
=============

:class:`arangoasync.exceptions.ArangoServerError` exceptions lightly wrap non-2xx
HTTP responses coming from ArangoDB. Each exception object contains the error
message, error code and HTTP request response details.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient, ArangoServerError, DocumentInsertError
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        try:
            await students.insert({"_key": "John"})
            await students.insert({"_key": "John"})  # duplicate key error
        except DocumentInsertError as err:
            assert isinstance(err, ArangoServerError)
            assert err.source == "server"

            msg = err.message            # Exception message usually from ArangoDB
            err_msg = err.error_message  # Raw error message from ArangoDB
            code = err.error_code        # Error code from ArangoDB
            url = err.url                # URL (API endpoint)
            method = err.http_method     # HTTP method (e.g. "POST")
            headers = err.http_headers   # Response headers
            http_code = err.http_code    # Status code (e.g. 200)

            # You can inspect the ArangoDB response directly.
            response = err.response
            method = response.method            # HTTP method
            headers = response.headers          # Response headers
            url = response.url                  # Full request URL
            success = response.is_success       # Set to True if HTTP code is 2XX
            raw_body = response.raw_body        # Raw string response body
            status_txt = response.status_text   # Status text (e.g "OK")
            status_code = response.status_code  # Status code (e.g. 200)
            err_code = response.error_code      # Error code from ArangoDB

            # You can also inspect the request sent to ArangoDB.
            request = err.request
            method = request.method      # HTTP method
            endpoint = request.endpoint  # API endpoint starting with "/_api"
            headers = request.headers    # Request headers
            params = request.params      # URL parameters
            data = request.data          # Request payload

Client Errors
=============

:class:`arangoasync.exceptions.ArangoClientError` exceptions originate from
driver client itself. They do not contain error codes nor HTTP request
response details.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient, ArangoClientError, DocumentParseError
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        try:
            await students.get({"_id": "invalid_id"})  # malformed document
        except DocumentParseError as err:
            assert isinstance(err, ArangoClientError)
            assert err.source == "client"

            # Only the error message is set.
            print(err.message)

Exceptions
==========

Below are all exceptions.

.. automodule:: arangoasync.exceptions
    :members:
