Error Codes
-----------

ArangoDB error code constants are provided for convenience.

**Example**

.. testcode::

    from arangoasync import errno

    # Some examples
    assert errno.NOT_IMPLEMENTED == 9
    assert errno.DOCUMENT_REV_BAD == 1239
    assert errno.DOCUMENT_NOT_FOUND == 1202

For more information, refer to the `ArangoDB Manual`_.

.. _ArangoDB Manual: https://www.arangodb.com/docs/stable/appendix-error-codes.html
