import asyncio

import pytest

from arangoasync.aql import AQL
from arangoasync.errno import CURSOR_NOT_FOUND, HTTP_BAD_PARAMETER
from arangoasync.exceptions import (
    CursorCloseError,
    CursorCountError,
    CursorEmptyError,
    CursorNextError,
    CursorStateError,
)
from arangoasync.typings import QueryExecutionStats, QueryProperties


@pytest.mark.asyncio
async def test_cursor_basic_query(db, doc_col, docs, cluster):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    # Execute query
    aql: AQL = db.aql
    options = QueryProperties(optimizer={"rules": ["+all"]}, profile=2)
    cursor = await aql.execute(
        query=f"FOR doc IN {doc_col.name} SORT doc.val RETURN doc",
        count=True,
        batch_size=2,
        ttl=1000,
        options=options,
    )

    # Check cursor attributes
    cursor_id = cursor.id
    assert "Cursor" in repr(cursor)
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 6
    assert cursor.empty() is False
    batch = cursor.batch
    assert len(batch) == 2
    for idx in range(2):
        assert batch[idx]["val"] == docs[idx]["val"]

    # Check cursor statistics
    statistics: QueryExecutionStats = cursor.statistics
    assert statistics.writes_executed == 0
    assert statistics.filtered == 0
    assert statistics.writes_ignored == 0
    assert statistics.execution_time > 0
    if cluster:
        assert statistics.http_requests > 0
    assert statistics.scanned_full > 0
    assert "nodes" in statistics

    # Check cursor warnings
    assert cursor.warnings == []

    # Check cursor profile
    profile = cursor.profile
    assert profile.initializing > 0
    assert profile.parsing > 0

    # Check query execution plan
    plan = cursor.plan
    assert "nodes" in plan
    assert plan.collections[0]["name"] == doc_col.name
    assert plan.is_modification_query is False

    # Retrieve the next document (should be already in the batch)
    assert (await cursor.next())["val"] == docs[0]["val"]
    assert cursor.id == cursor_id
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.statistics == statistics
    assert cursor.profile == profile
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 6
    assert len(cursor.batch) == 1
    assert cursor.batch[0]["val"] == docs[1]["val"]

    # Retrieve the next document (should be already in the batch)
    assert (await cursor.next())["val"] == docs[1]["val"]
    assert cursor.id == cursor_id
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.statistics == statistics
    assert cursor.profile == profile
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 6
    assert cursor.empty() is True

    # Retrieve the next document (should be fetched from the server)
    assert (await cursor.next())["val"] == docs[2]["val"]
    assert cursor.id == cursor_id
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.statistics == statistics
    assert cursor.profile == profile
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 6
    assert cursor.batch[0]["val"] == docs[3]["val"]
    assert cursor.empty() is False

    # Retrieve the rest of the documents
    for idx in range(3, 6):
        assert (await cursor.next())["val"] == docs[idx]["val"]

    # There should be no longer any documents to retrieve
    assert cursor.empty() is True
    assert cursor.has_more is False
    with pytest.raises(StopAsyncIteration):
        await cursor.next()

    # Close the cursor (should be already gone because it has been consumed)
    assert await cursor.close(ignore_missing=True) is False


@pytest.mark.asyncio
async def test_cursor_write_query(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    # Execute query, updating some documents
    aql: AQL = db.aql
    options = QueryProperties(optimizer={"rules": ["+all"]}, profile=1, max_runtime=0.0)
    cursor = await aql.execute(
        """
        FOR d IN {col} FILTER d.val == @first OR d.val == @second
        SORT d.val
        UPDATE {{_key: d._key, _val: @val }} IN {col}
        RETURN NEW
        """.format(
            col=doc_col.name
        ),
        bind_vars={"first": 1, "second": 2, "val": 42},
        count=True,
        batch_size=1,
        ttl=1000,
        options=options,
    )

    # Check cursor attributes
    cursor_id = cursor.id
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 2
    assert cursor.batch[0]["val"] == docs[0]["val"]
    assert cursor.empty() is False

    statistics = cursor.statistics
    assert statistics.writes_executed == 2
    assert statistics.filtered == 4  # 2 docs matched, 4 docs ignored
    assert statistics.writes_ignored == 0
    assert statistics.execution_time > 0

    profile = cursor.profile
    assert profile.initializing > 0
    assert profile.parsing > 0

    # First document
    assert (await cursor.next())["val"] == docs[0]["val"]
    assert cursor.id == cursor_id
    assert cursor.has_more is True
    assert cursor.cached is False
    assert cursor.statistics == statistics
    assert cursor.profile == profile
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 2
    assert cursor.empty() is True
    assert len(cursor.batch) == 0

    # Second document, this is fetched from the server
    assert (await cursor.next())["val"] == docs[1]["val"]
    assert cursor.id == cursor_id
    assert cursor.has_more is False
    assert cursor.cached is False
    assert cursor.statistics == statistics
    assert cursor.profile == profile
    assert cursor.warnings == []
    assert cursor.count == len(cursor) == 2
    assert cursor.empty() is True

    # There should be no longer any documents to retrieve, hence the cursor is closed
    with pytest.raises(CursorCloseError) as err:
        await cursor.close(ignore_missing=False)
    assert err.value.error_code == CURSOR_NOT_FOUND
    assert await cursor.close(ignore_missing=True) is False


@pytest.mark.asyncio
async def test_cursor_invalid_id(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=2,
        ttl=1000,
        options={"optimizer": {"rules": ["+all"]}, "profile": 1},
    )

    # Set the cursor ID to "invalid" and assert errors
    setattr(cursor, "_id", "invalid")

    # Cursor should not be found
    with pytest.raises(CursorNextError) as err:
        async for _ in cursor:
            pass
    assert err.value.error_code == CURSOR_NOT_FOUND
    with pytest.raises(CursorCloseError) as err:
        await cursor.close(ignore_missing=False)
    assert err.value.error_code == CURSOR_NOT_FOUND
    assert await cursor.close(ignore_missing=True) is False

    # Set the cursor ID to None and assert errors
    setattr(cursor, "_id", None)
    with pytest.raises(CursorStateError):
        print(await cursor.next())
    with pytest.raises(CursorStateError):
        await cursor.fetch()
    assert await cursor.close() is False


@pytest.mark.asyncio
async def test_cursor_premature_close(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=2,
        ttl=1000,
    )
    assert len(cursor.batch) == 2
    assert await cursor.close() is True

    # Cursor should be already closed
    with pytest.raises(CursorCloseError) as err:
        await cursor.close(ignore_missing=False)
    assert err.value.error_code == CURSOR_NOT_FOUND
    assert await cursor.close(ignore_missing=True) is False


@pytest.mark.asyncio
async def test_cursor_context_manager(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=2,
        ttl=1000,
    )
    async with cursor as ctx:
        assert (await ctx.next())["val"] == docs[0]["val"]

    # Cursor should be already closed
    with pytest.raises(CursorCloseError) as err:
        await cursor.close(ignore_missing=False)
    assert err.value.error_code == CURSOR_NOT_FOUND
    assert await cursor.close(ignore_missing=True) is False


@pytest.mark.asyncio
async def test_cursor_iteration(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=2,
        ttl=1000,
    )
    doc_cnt = 0
    async with cursor as ctx:
        async for _ in ctx:
            doc_cnt += 1
    assert doc_cnt == len(docs)


@pytest.mark.asyncio
async def test_cursor_manual_fetch_and_pop(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=1,
        ttl=1000,
        options={"allowRetry": True},
    )

    # Fetch documents manually
    for idx in range(2, len(docs)):
        result = await cursor.fetch()
        assert len(result) == 1
        assert cursor.count == len(docs)
        assert cursor.has_more
        assert len(cursor.batch) == idx
        assert result[0]["val"] == docs[idx - 1]["val"]
    result = await cursor.fetch()
    assert result[0]["val"] == docs[len(docs) - 1]["val"]
    assert len(cursor.batch) == len(docs)
    assert not cursor.has_more

    # Pop documents manually
    idx = 0
    while not cursor.empty():
        doc = cursor.pop()
        assert doc["val"] == docs[idx]["val"]
        idx += 1
    assert len(cursor.batch) == 0

    # Cursor should be empty
    with pytest.raises(CursorEmptyError):
        await cursor.pop()


@pytest.mark.asyncio
async def test_cursor_retry(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    # Do not allow retries
    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=1,
        ttl=1000,
        options={"allowRetry": False},
    )

    # Increase the batch id by doing a fetch
    await cursor.fetch()
    while not cursor.empty():
        cursor.pop()
    next_batch_id = cursor.next_batch_id

    # Fetch the next batch
    await cursor.fetch()
    # Retry is not allowed
    with pytest.raises(CursorNextError) as err:
        await cursor.fetch(batch_id=next_batch_id)
    assert err.value.error_code == HTTP_BAD_PARAMETER

    await cursor.close()

    # Now let's allow retries
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=True,
        batch_size=1,
        ttl=1000,
        options={"allowRetry": True},
    )

    # Increase the batch id by doing a fetch
    await cursor.fetch()
    while not cursor.empty():
        cursor.pop()
    next_batch_id = cursor.next_batch_id

    # Fetch the next batch
    prev_batch = await cursor.fetch()
    next_next_batch_id = cursor.next_batch_id
    # Should fetch the same batch again
    next_batch = await cursor.fetch(batch_id=next_batch_id)
    assert next_batch == prev_batch
    # Next batch id should be the same
    assert cursor.next_batch_id == next_next_batch_id

    # Fetch the next batch
    next_next_batch = await cursor.fetch()
    assert next_next_batch != next_batch

    assert await cursor.close()


@pytest.mark.asyncio
async def test_cursor_no_count(db, doc_col, docs):
    # Insert documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    aql: AQL = db.aql
    cursor = await aql.execute(
        f"FOR d IN {doc_col.name} SORT d._key RETURN d",
        count=False,
        batch_size=2,
        ttl=1000,
    )

    # Cursor count is not enabled
    with pytest.raises(CursorCountError):
        _ = len(cursor)
    with pytest.raises(CursorCountError):
        _ = bool(cursor)

    while cursor.has_more:
        assert cursor.count is None
        assert await cursor.fetch()
