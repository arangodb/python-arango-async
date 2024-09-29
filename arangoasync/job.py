__all__ = ["AsyncJob"]


from typing import Generic, TypeVar

T = TypeVar("T")


class AsyncJob(Generic[T]):
    """Job for tracking and retrieving result of an async API execution."""

    pass
