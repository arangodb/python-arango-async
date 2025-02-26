__all__ = ["Cursor"]


from collections import deque
from typing import Any, Deque, List, Optional

from arangoasync.errno import HTTP_NOT_FOUND
from arangoasync.exceptions import (
    CursorCloseError,
    CursorCountError,
    CursorEmptyError,
    CursorNextError,
    CursorStateError,
)
from arangoasync.executor import NonAsyncExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    Json,
    Jsons,
    QueryExecutionExtra,
    QueryExecutionPlan,
    QueryExecutionProfile,
    QueryExecutionStats,
)


class Cursor:
    """Cursor API wrapper.

    Cursors fetch query results from ArangoDB server in batches. Cursor objects
    are *stateful* as they store the fetched items in-memory. They must not be
    shared across threads without a proper locking mechanism.

    Args:
        executor: Required to execute the API requests.
        data: Cursor initialization data. Returned by the server when the query
            is created.
    """

    def __init__(self, executor: NonAsyncExecutor, data: Json) -> None:
        self._executor = executor
        self._cached: Optional[bool] = None
        self._count: Optional[int] = None
        self._extra = QueryExecutionExtra({})
        self._has_more: Optional[bool] = None
        self._id: Optional[str] = None
        self._next_batch_id: Optional[str] = None
        self._batch: Deque[Any] = deque()
        self._update(data)

    def __aiter__(self) -> "Cursor":
        return self

    async def __anext__(self) -> Any:
        return await self.next()

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close(ignore_missing=True)

    def __len__(self) -> int:
        if self._count is None:
            raise CursorCountError("Cursor count not enabled")
        return self._count

    def __repr__(self) -> str:
        return f"<Cursor {self._id}>" if self._id else "<Cursor>"

    @property
    def cached(self) -> Optional[bool]:
        """Whether the result was served from the query cache or not."""
        return self._cached

    @property
    def count(self) -> Optional[int]:
        """The total number of result documents available."""
        return self._count

    @property
    def extra(self) -> QueryExecutionExtra:
        """Extra information about the query execution."""
        return self._extra

    @property
    def has_more(self) -> Optional[bool]:
        """Whether there are more results available on the server."""
        return self._has_more

    @property
    def id(self) -> Optional[str]:
        """Cursor ID."""
        return self._id

    @property
    def next_batch_id(self) -> Optional[str]:
        """ID of the batch after current one."""
        return self._next_batch_id

    @property
    def batch(self) -> Deque[Any]:
        """Return the current batch of results."""
        return self._batch

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    @property
    def statistics(self) -> QueryExecutionStats:
        """Query statistics."""
        return self.extra.stats

    @property
    def profile(self) -> QueryExecutionProfile:
        """Query profiling information."""
        return self.extra.profile

    @property
    def plan(self) -> QueryExecutionPlan:
        """Execution plan for the query."""
        return self.extra.plan

    @property
    def warnings(self) -> List[Json]:
        """Warnings generated during query execution."""
        return self.extra.warnings

    def empty(self) -> bool:
        """Check if the current batch is empty."""
        return len(self._batch) == 0

    async def next(self) -> Any:
        """Retrieve and pop the next item.

        If current batch is empty/depleted, an API request is automatically
        sent to fetch the next batch from the server and update the cursor.

        Returns:
            Any: Next item.

        Raises:
            StopAsyncIteration: If there are no more items to retrieve.
            CursorNextError: If the cursor failed to fetch the next batch.
            CursorStateError: If the cursor ID is not set.
        """
        if self.empty():
            if not self.has_more:
                raise StopAsyncIteration
            await self.fetch()
        return self.pop()

    def pop(self) -> Any:
        """Pop the next item from the current batch.

        If current batch is empty/depleted, an exception is raised. You must
        call :func:`arangoasync.cursor.Cursor.fetch` to manually fetch the next
        batch from server.

        Returns:
            Any: Next item from the current batch.

        Raises:
            CursorEmptyError: If the current batch is empty.
        """
        try:
            return self._batch.popleft()
        except IndexError:
            raise CursorEmptyError("Current batch is empty")

    async def fetch(self, batch_id: Optional[str] = None) -> List[Any]:
        """Fetch the next batch from the server and update the cursor.

        Args:
            batch_id (str | None): ID of the batch to fetch. If not set, the
                next batch after the current one is fetched.

        Returns:
            List[Any]: New batch results.

        Raises:
            CursorNextError: If the cursor is empty.
            CursorStateError: If the cursor ID is not set.

        References:
            - `read-the-next-batch-from-a-cursor <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#read-the-next-batch-from-a-cursor>`__
            - `read-a-batch-from-the-cursor-again <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#read-a-batch-from-the-cursor-again>`__
        """  # noqa: E501
        if self._id is None:
            raise CursorStateError("Cursor ID is not set")

        endpoint = f"/_api/cursor/{self._id}"
        if batch_id is not None:
            endpoint += f"/{batch_id}"

        request = Request(
            method=Method.POST,
            endpoint=endpoint,
        )

        def response_handler(resp: Response) -> List[Any]:
            if not resp.is_success:
                raise CursorNextError(resp, request)
            return self._update(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def close(self, ignore_missing: bool = False) -> bool:
        """Close the cursor and free any server resources associated with it.

        Args:
            ignore_missing (bool): Do not raise an exception on missing cursor.

        Returns:
            bool: `True` if the cursor was closed successfully. `False` if there
                was no cursor to close. If there is no cursor associated with the
                query, `False` is returned.

        Raises:
            CursorCloseError: If the cursor failed to close.

        References:
            - `delete-a-cursor <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#delete-a-cursor>`__
        """  # noqa: E501
        if self._id is None:
            return False

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/cursor/{self._id}",
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            raise CursorCloseError(resp, request)

        return await self._executor.execute(request, response_handler)

    def _update(self, data: Json) -> List[Any]:
        """Update the cursor with the new data."""
        if "id" in data:
            self._id = data.get("id")
        self._cached = data.get("cached")
        self._count = data.get("count")
        self._extra = QueryExecutionExtra(data.get("extra", dict()))
        self._has_more = data.get("hasMore")
        self._next_batch_id = data.get("nextBatchId")
        result: List[Any] = data.get("result", list())
        self._batch.extend(result)
        return result
