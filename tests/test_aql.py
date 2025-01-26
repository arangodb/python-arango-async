import asyncio
import time

import pytest

from arangoasync.errno import QUERY_PARSE
from arangoasync.exceptions import (
    AQLQueryClearError,
    AQLQueryExecuteError,
    AQLQueryExplainError,
    AQLQueryKillError,
    AQLQueryListError,
    AQLQueryRulesGetError,
    AQLQueryTrackingGetError,
    AQLQueryTrackingSetError,
    AQLQueryValidateError,
)
from arangoasync.typings import QueryExplainOptions


@pytest.mark.asyncio
async def test_simple_query(db, bad_db, doc_col, docs):
    await doc_col.insert(docs[0])
    aql = db.aql
    _ = await aql.execute(
        query="FOR doc IN @@collection RETURN doc",
        bind_vars={"@collection": doc_col.name},
    )

    assert repr(db.aql) == f"<AQL in {db.name}>"

    with pytest.raises(AQLQueryExecuteError):
        _ = await bad_db.aql.execute(
            query="FOR doc IN @@collection RETURN doc",
            bind_vars={"@collection": doc_col.name},
        )


@pytest.mark.asyncio
async def test_query_tracking(db, bad_db):
    aql = db.aql

    # Get the current tracking properties.
    tracking = await aql.tracking()
    assert tracking.enabled is True
    assert tracking.track_slow_queries is True

    # Disable tracking.
    tracking = await aql.set_tracking(enabled=False)
    assert tracking.enabled is False

    # Re-enable.
    tracking = await aql.set_tracking(enabled=True, max_slow_queries=5)
    assert tracking.enabled is True
    assert tracking.max_slow_queries == 5

    # Exceptions with bad database
    with pytest.raises(AQLQueryTrackingGetError):
        _ = await bad_db.aql.tracking()
    with pytest.raises(AQLQueryTrackingSetError):
        _ = await bad_db.aql.set_tracking(enabled=False)


@pytest.mark.asyncio
async def test_list_queries(superuser, db, bad_db):
    aql = db.aql

    # Do not await, let it run in the background.
    long_running_task = asyncio.create_task(aql.execute("RETURN SLEEP(10)"))
    time.sleep(1)

    for _ in range(10):
        queries = await aql.queries()
        if len(queries) > 0:
            break

    # Only superuser can list all queries from all databases.
    all_queries = await superuser.aql.queries(all_queries=True)
    assert len(all_queries) > 0

    # Only test no-throws.
    _ = await aql.slow_queries()
    _ = await superuser.aql.slow_queries(all_queries=True)
    await aql.clear_slow_queries()
    await superuser.aql.clear_slow_queries(all_queries=True)

    with pytest.raises(AQLQueryListError):
        _ = await bad_db.aql.queries()
    with pytest.raises(AQLQueryListError):
        _ = await bad_db.aql.slow_queries()
    with pytest.raises(AQLQueryClearError):
        await bad_db.aql.clear_slow_queries()
    with pytest.raises(AQLQueryListError):
        _ = await aql.queries(all_queries=True)
    with pytest.raises(AQLQueryListError):
        _ = await aql.slow_queries(all_queries=True)
    with pytest.raises(AQLQueryClearError):
        await aql.clear_slow_queries(all_queries=True)

    long_running_task.cancel()


@pytest.mark.asyncio
async def test_kill_query(db, bad_db, superuser):
    aql = db.aql

    # Do not await, let it run in the background.
    long_running_task = asyncio.create_task(aql.execute("RETURN SLEEP(10)"))
    time.sleep(1)

    queries = list()
    for _ in range(10):
        queries = await aql.queries()
        if len(queries) > 0:
            break

    # Kill the query
    query_id = queries[0]["id"]
    assert await aql.kill(query_id) is True

    # Ignore missing
    assert await aql.kill("fakeid", ignore_missing=True) is False
    assert (
        await superuser.aql.kill("fakeid", ignore_missing=True, all_queries=True)
        is False
    )

    # Check exceptions
    with pytest.raises(AQLQueryKillError):
        await aql.kill("fakeid")
    with pytest.raises(AQLQueryKillError):
        await bad_db.aql.kill(query_id)

    long_running_task.cancel()


@pytest.mark.asyncio
async def test_explain_query(db, doc_col, bad_db):
    aql = db.aql

    # Explain a simple query
    result = await aql.explain("RETURN 1")
    assert "plan" in result

    # Something more complex
    options = QueryExplainOptions(
        all_plans=True,
        max_plans=10,
        optimizer={"rules": ["+all", "-use-index-range"]},
    )
    explanations = await aql.explain(
        f"FOR d IN {doc_col.name} RETURN d",
        options=options,
    )
    assert "plans" in explanations
    explanation = await aql.explain(
        f"FOR d IN {doc_col.name} RETURN d",
        options=options.to_dict(),
    )
    assert "plans" in explanation

    # Check exceptions
    with pytest.raises(AQLQueryExplainError):
        _ = await bad_db.aql.explain("RETURN 1")


@pytest.mark.asyncio
async def test_validate_query(db, doc_col, bad_db):
    aql = db.aql

    # Validate invalid query
    with pytest.raises(AQLQueryValidateError) as err:
        await aql.validate("INVALID QUERY")
    assert err.value.error_code == QUERY_PARSE

    # Test validate valid query
    result = await aql.validate(f"FOR d IN {doc_col.name} RETURN d")
    assert result["parsed"] is True

    with pytest.raises(AQLQueryValidateError):
        _ = await bad_db.aql.validate("RETURN 1")


@pytest.mark.asyncio
async def test_query_rules(db, bad_db):
    aql = db.aql

    rules = await aql.query_rules()
    assert len(rules) > 0

    with pytest.raises(AQLQueryRulesGetError):
        _ = await bad_db.aql.query_rules()
