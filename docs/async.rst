Async API Execution
-------------------

In **asynchronous API executions**, the driver sends API requests to ArangoDB in
fire-and-forget style. The server processes them in the background, and
the results can be retrieved once available via :class:`arangoasync.job.AsyncJob` objects.

**Example:**

.. code-block:: python

    import time
    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.errno import HTTP_BAD_PARAMETER
    from arangoasync.exceptions import (
        AQLQueryExecuteError,
        AsyncJobCancelError,
        AsyncJobClearError,
    )

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Begin async execution. This returns an instance of AsyncDatabase, a
        # database-level API wrapper tailored specifically for async execution.
        async_db = db.begin_async_execution(return_result=True)

        # Child wrappers are also tailored for async execution.
        async_aql = async_db.aql
        async_col = async_db.collection("students")

        # API execution context is always set to "async".
        assert async_db.context == "async"
        assert async_aql.context == "async"
        assert async_col.context == "async"

        # On API execution, AsyncJob objects are returned instead of results.
        job1 = await async_col.insert({"_key": "Neal"})
        job2 = await async_col.insert({"_key": "Lily"})
        job3 = await async_aql.execute("RETURN 100000")
        job4 = await async_aql.execute("INVALID QUERY")  # Fails due to syntax error.

        # Retrieve the status of each async job.
        for job in [job1, job2, job3, job4]:
            # Job status can be "pending" or "done".
            assert await job.status() in {"pending", "done"}

            # Let's wait until the jobs are finished.
            while await job.status() != "done":
                time.sleep(0.1)

        # Retrieve the results of successful jobs.
        metadata = await job1.result()
        assert metadata["_id"] == "students/Neal"

        metadata = await job2.result()
        assert metadata["_id"] == "students/Lily"

        cursor = await job3.result()
        assert await cursor.next() == 100000

        # If a job fails, the exception is propagated up during result retrieval.
        try:
            result = await job4.result()
        except AQLQueryExecuteError as err:
            assert err.http_code == HTTP_BAD_PARAMETER

        # Cancel a job. Only pending jobs still in queue may be cancelled.
        # Since job3 is done, there is nothing to cancel and an exception is raised.
        try:
            await job3.cancel()
        except AsyncJobCancelError as err:
            print(err.message)

        # Clear the result of a job from ArangoDB server to free up resources.
        # Result of job4 was removed from the server automatically upon retrieval,
        # so attempt to clear it raises an exception.
        try:
            await job4.clear()
        except AsyncJobClearError as err:
            print(err.message)

        # List the IDs of the first 100 async jobs completed.
        jobs_done = await db.async_jobs(status="done", count=100)

        # List the IDs of the first 100 async jobs still pending.
        jobs_pending = await db.async_jobs(status='pending', count=100)

        # Clear all async jobs still sitting on the server.
        await db.clear_async_jobs()

Cursors returned from async API wrappers will no longer send async requests when they fetch more results, but behave
like regular cursors instead. This makes sense, because the point of cursors is iteration, whereas async jobs are meant
for one-shot requests. However, the first result retrieval is still async, and only then the cursor is returned, making
async AQL requests effective for queries with a long execution time.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        # Insert some documents into the collection.
        await students.insert_many([{"_key": "Neal"}, {"_key": "Lily"}])

        # Begin async execution.
        async_db = db.begin_async_execution(return_result=True)

        aql = async_db.aql
        job = await aql.execute(
            f"FOR d IN {students.name} SORT d._key RETURN d",
            count=True,
            batch_size=1,
            ttl=1000,
        )
        await job.wait()

        # Iterate through the cursor.
        # Although the request to fetch the cursor is async, its underlying executor is no longer async.
        # Next batches will be fetched in real-time.
        doc_cnt = 0
        cursor = await job.result()
        async with cursor as ctx:
            async for _ in ctx:
                doc_cnt += 1
        assert doc_cnt == 2

.. note::
    Be mindful of server-side memory capacity when issuing a large number of
    async requests in small time interval.

See :class:`arangoasync.database.AsyncDatabase` and :class:`arangoasync.job.AsyncJob` for API specification.
