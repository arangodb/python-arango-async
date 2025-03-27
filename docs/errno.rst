Error Codes
-----------

Python-Arango-Async provides ArangoDB error code constants for convenience.

**Example**

.. testcode::

    from arangoasync import errno

    # Some examples
    assert errno.NOT_IMPLEMENTED == 9
    assert errno.DOCUMENT_REV_BAD == 1239
    assert errno.DOCUMENT_NOT_FOUND == 1202

For more information, refer to `ArangoDB manual`_.

.. _ArangoDB manual: https://www.arangodb.com/docs/stable/appendix-error-codes.html
