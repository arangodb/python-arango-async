__all__ = ["AsyncJob"]


import asyncio
from typing import Callable, Generic, Optional, TypeVar

from arangoasync.connection import Connection
from arangoasync.errno import HTTP_NOT_FOUND
from arangoasync.exceptions import (
    AsyncJobCancelError,
    AsyncJobClearError,
    AsyncJobResultError,
    AsyncJobStatusError,
)
from arangoasync.request import Method, Request
from arangoasync.response import Response

T = TypeVar("T")


class AsyncJob(Generic[T]):
    """Job for tracking and retrieving result of an async API execution.

    Args:
        conn: HTTP connection.
        job_id: Async job ID.
        response_handler: HTTP response handler

    References:
        - `jobs <https://docs.arangodb.com/stable/develop/http-api/jobs/>`__
    """  # noqa: E501

    def __init__(
        self,
        conn: Connection,
        job_id: str,
        response_handler: Callable[[Response], T],
    ) -> None:
        self._conn = conn
        self._id = job_id
        self._response_handler = response_handler

    def __repr__(self) -> str:
        return f"<AsyncJob {self._id}>"

    @property
    def id(self) -> str:
        """Return the async job ID.

        Returns:
            str: Async job ID.
        """
        return self._id

    async def status(self) -> str:
        """Return the async job status from server.

        Once a job result is retrieved via func:`arangoasync.job.AsyncJob.result`
        method, it is deleted from server and subsequent status queries will
        fail.

        Returns:
            str: Async job status. Possible values are "pending" (job is still
            in queue), "done" (job finished or raised an error).

        Raises:
            ArangoError: If there is a problem with the request.
            AsyncJobStatusError: If retrieval fails or the job is not found.

        References:
            - `list-async-jobs-by-status-or-get-the-status-of-specific-job <https://docs.arangodb.com/stable/develop/http-api/jobs/#list-async-jobs-by-status-or-get-the-status-of-specific-job>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint=f"/_api/job/{self._id}")
        response = await self._conn.send_request(request)

        if response.is_success:
            if response.status_code == 204:
                return "pending"
            else:
                return "done"
        if response.error_code == HTTP_NOT_FOUND:
            error_message = f"job {self._id} not found"
            raise AsyncJobStatusError(response, request, error_message)
        raise AsyncJobStatusError(response, request)

    async def result(self) -> T:
        """Fetch the async job result from server.

        If the job raised an exception, it is propagated up at this point.

        Once job result is retrieved, it is deleted from server and subsequent
        queries for result will fail.

        Returns:
            Async job result.

        Raises:
            ArangoError: If the job raised an exception or there was a problem with
                the request.
            AsyncJobResultError: If retrieval fails, because job no longer exists or
                is still pending.

        References:
            - `get-the-results-of-an-async-job <https://docs.arangodb.com/stable/develop/http-api/jobs/#get-the-results-of-an-async-job>`__
        """  # noqa: E501
        request = Request(method=Method.PUT, endpoint=f"/_api/job/{self._id}")
        response = await self._conn.send_request(request)

        if (
            "x-arango-async-id" in response.headers
            or "X-Arango-Async-Id" in response.headers
        ):
            # The job result is available on the server
            return self._response_handler(response)

        if response.status_code == 204:
            # The job is still in the pending queue or not yet finished.
            raise AsyncJobResultError(response, request, self._not_done())
        # The job is not known (anymore).
        # We can tell the status from the HTTP status code.
        if response.error_code == HTTP_NOT_FOUND:
            raise AsyncJobResultError(response, request, self._not_found())
        raise AsyncJobResultError(response, request)

    async def cancel(self, ignore_missing: bool = False) -> bool:
        """Cancel the async job.

        An async job cannot be cancelled once it is taken out of the queue.

        Note:
            It still might take some time to actually cancel the running async job.

        Args:
            ignore_missing: Do not raise an exception if the job is not found.

        Returns:
            `True` if job was cancelled successfully, `False` if the job was not found
            but **ignore_missing** was set to `True`.

        Raises:
            ArangoError: If there was a problem with the request.
            AsyncJobCancelError: If cancellation fails.

        References:
            - `cancel-an-async-job <https://docs.arangodb.com/stable/develop/http-api/jobs/#cancel-an-async-job>`__
        """  # noqa: E501
        request = Request(method=Method.PUT, endpoint=f"/_api/job/{self._id}/cancel")
        response = await self._conn.send_request(request)

        if response.is_success:
            return True
        if response.error_code == HTTP_NOT_FOUND:
            if ignore_missing:
                return False
            raise AsyncJobCancelError(response, request, self._not_found())
        raise AsyncJobCancelError(response, request)

    async def clear(
        self,
        ignore_missing: bool = False,
    ) -> bool:
        """Delete the job result from the server.

        Args:
            ignore_missing: Do not raise an exception if the job is not found.

        Returns:
            `True` if result was deleted successfully, `False` if the job was
            not found but **ignore_missing** was set to `True`.

        Raises:
            ArangoError: If there was a problem with the request.
            AsyncJobClearError: If deletion fails.

        References:
            - `delete-async-job-results <https://docs.arangodb.com/stable/develop/http-api/jobs/#delete-async-job-results>`__
        """  # noqa: E501
        request = Request(method=Method.DELETE, endpoint=f"/_api/job/{self._id}")
        resp = await self._conn.send_request(request)

        if resp.is_success:
            return True
        if resp.error_code == HTTP_NOT_FOUND:
            if ignore_missing:
                return False
            raise AsyncJobClearError(resp, request, self._not_found())
        raise AsyncJobClearError(resp, request)

    async def wait(self, seconds: Optional[float] = None) -> bool:
        """Wait for the async job to finish.

        Args:
            seconds: Number of seconds to wait between status checks. If not
                provided, the method will wait indefinitely.

        Returns:
            `True` if the job is done, `False` if the job is still pending.
        """
        while True:
            if await self.status() == "done":
                return True
            if seconds is None:
                await asyncio.sleep(1)
            else:
                seconds -= 1
                if seconds < 0:
                    return False
                await asyncio.sleep(1)

    def _not_found(self) -> str:
        return f"job {self._id} not found"

    def _not_done(self) -> str:
        return f"job {self._id} not done"
