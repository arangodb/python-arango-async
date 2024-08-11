class ArangoError(Exception):
    """Base class for all exceptions in python-arango-async."""


class ArangoClientError(ArangoError):
    """Base class for all client-related exceptions.

    Args:
        msg (str): Error message.

    Attributes:
        source (str): Source of the error (always set to "client")
        message (str): Error message.
    """

    source = "client"

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.message = msg


class JWTExpiredError(ArangoClientError):
    """JWT token has expired."""
