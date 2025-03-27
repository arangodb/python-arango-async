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
python-arango client itself. They do not contain error codes nor HTTP request
response details.
