import asyncio
import time

import pytest

from arangoasync.exceptions import (
    AQLQueryExecuteError,
    AsyncJobCancelError,
    AsyncJobListError,
    AsyncJobResultError,
)


@pytest.mark.asyncio
async def test_async_no_result(db, bad_db, doc_col, docs):
    # There should be no jobs to begin with
    jobs = await db.async_jobs(status="pending")
    assert len(jobs) == 0
    with pytest.raises(AsyncJobListError):
        await bad_db.async_jobs(status="pending")

    # Create a basic job
    async_db = db.begin_async_execution(return_result=False)
    async_col = async_db.collection(doc_col.name)

    # Should return None, because return_result=False
    job1 = await async_col.insert(docs[0])
    assert job1 is None
    time.sleep(1)
    # There should be none pending or done
    jobs_pending, jobs_done = await asyncio.gather(
        db.async_jobs(status="pending"),
        db.async_jobs(status="done"),
    )
    assert len(jobs_pending) == 0
    assert len(jobs_done) == 0

    # Create a long-running job
    aql = async_db.aql
    job2, job3 = await asyncio.gather(
        aql.execute("RETURN SLEEP(5)"), aql.execute("RETURN SLEEP(5)")
    )
    time.sleep(1)
    assert job2 is None
    assert job3 is None
    jobs_pending, jobs_done = await asyncio.gather(
        db.async_jobs(status="pending"),
        db.async_jobs(status="done"),
    )
    assert len(jobs_pending) == 0
    assert len(jobs_done) == 0

    with pytest.raises(AsyncJobListError):
        await db.async_jobs(status="invalid-parameter")
    with pytest.raises(AsyncJobListError):
        await bad_db.async_jobs(status="pending")


@pytest.mark.asyncio
async def test_async_result(db, bad_db, doc_col, docs):
    # There should be no jobs to begin with
    jobs = await db.async_jobs(status="pending")
    assert len(jobs) == 0

    # Create a basic job and wait for it to finish
    async_db = db.begin_async_execution(return_result=True)
    async_col = async_db.collection(doc_col.name)
    job1 = await async_col.insert(docs[0])
    await job1.wait()
    assert await job1.status() == "done"
    res = await job1.result()
    assert isinstance(res, dict)

    # Check that exceptions are being propagated correctly
    aql = async_db.aql
    job2 = await aql.execute("INVALID QUERY")
    await job2.wait()
    with pytest.raises(AQLQueryExecuteError):
        _ = await job2.result()

    # Long-running job
    job3 = await aql.execute("RETURN SLEEP(5)")
    time.sleep(1)
    assert await job3.status() == "pending"
    jobs = await db.async_jobs(status="pending")
    assert len(jobs) == 1
    await job3.wait()

    # Clear jobs for which result has not been claimed
    jobs = await db.async_jobs(status="done")
    assert len(jobs) == 1
    await db.clear_async_jobs()
    jobs = await db.async_jobs(status="done")
    assert len(jobs) == 0

    # Attempt to cancel a finished job
    assert await job3.cancel(ignore_missing=True) is False
    with pytest.raises(AsyncJobCancelError):
        await job3.cancel()

    # Attempt to clear a single job
    job4 = await aql.execute("RETURN 1")
    await job4.wait()
    await job4.clear()

    # Attempt to get the result of a pending job
    job5 = await aql.execute("RETURN SLEEP(5)")
    time.sleep(1)
    with pytest.raises(AsyncJobResultError):
        _ = await job5.result()
    await job5.wait()


@pytest.mark.asyncio
async def test_async_cursor(db, doc_col, docs):
    # Insert some documents first
    await asyncio.gather(*(doc_col.insert(doc) for doc in docs))

    async_db = db.begin_async_execution()
    aql = async_db.aql
    job = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=1,
        ttl=1000,
    )
    await job.wait()

    # Get the cursor. Bear in mind that its underlying executor is no longer async.
    doc_cnt = 0
    cursor = await job.result()
    async with cursor as ctx:
        async for _ in ctx:
            doc_cnt += 1
    assert doc_cnt == len(docs)
