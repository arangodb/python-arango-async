import asyncio
import time

import pytest
from packaging import version

from arangoasync.errno import (
    FORBIDDEN,
    QUERY_FUNCTION_INVALID_CODE,
    QUERY_FUNCTION_NOT_FOUND,
    QUERY_PARSE,
)
from arangoasync.exceptions import (
    AQLCacheClearError,
    AQLCacheConfigureError,
    AQLCacheEntriesError,
    AQLCachePropertiesError,
    AQLFunctionCreateError,
    AQLFunctionDeleteError,
    AQLFunctionListError,
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


@pytest.mark.asyncio
async def test_cache_results_management(db, bad_db, doc_col, docs, cluster):
    if cluster:
        pytest.skip("Cluster mode does not support query result cache management")

    aql = db.aql
    cache = aql.cache

    # Sanity check, just see if the response is OK.
    _ = await cache.properties()
    with pytest.raises(AQLCachePropertiesError) as err:
        _ = await bad_db.aql.cache.properties()
    assert err.value.error_code == FORBIDDEN

    # Turn on caching
    result = await cache.configure(mode="on")
    assert result.mode == "on"
    result = await cache.properties()
    assert result.mode == "on"
    with pytest.raises(AQLCacheConfigureError) as err:
        _ = await bad_db.aql.cache.configure(mode="on")
    assert err.value.error_code == FORBIDDEN

    # Run a simple query to use the cache
    await doc_col.insert(docs[0])
    _ = await aql.execute(
        query="FOR doc IN @@collection RETURN doc",
        bind_vars={"@collection": doc_col.name},
        options={"cache": True},
    )

    # Check the entries
    entries = await cache.entries()
    assert isinstance(entries, list)
    assert len(entries) > 0

    with pytest.raises(AQLCacheEntriesError) as err:
        _ = await bad_db.aql.cache.entries()
    assert err.value.error_code == FORBIDDEN

    # Clear the cache
    await cache.clear()
    entries = await cache.entries()
    assert len(entries) == 0
    with pytest.raises(AQLCacheClearError) as err:
        await bad_db.aql.cache.clear()
    assert err.value.error_code == FORBIDDEN


@pytest.mark.asyncio
async def test_cache_plan_management(db, bad_db, doc_col, docs, db_version):
    if db_version < version.parse("3.12.4"):
        pytest.skip("Query plan cache is supported in ArangoDB 3.12.4+")

    aql = db.aql
    cache = aql.cache

    # Run a simple query to use the cache
    await doc_col.insert(docs[0])
    _ = await aql.execute(
        query="FOR doc IN @@collection RETURN doc",
        bind_vars={"@collection": doc_col.name},
        options={"usePlanCache": True},
    )

    # Check the entries
    entries = await cache.plan_entries()
    assert isinstance(entries, list)
    assert len(entries) > 0
    with pytest.raises(AQLCacheEntriesError) as err:
        _ = await bad_db.aql.cache.plan_entries()
    assert err.value.error_code == FORBIDDEN

    # Clear the cache
    await cache.clear_plan()
    entries = await cache.plan_entries()
    assert len(entries) == 0
    with pytest.raises(AQLCacheClearError) as err:
        await bad_db.aql.cache.clear_plan()
    assert err.value.error_code == FORBIDDEN


@pytest.mark.asyncio
async def test_aql_function_management(db, bad_db):
    fn_group = "functions::temperature"
    fn_name_1 = "functions::temperature::celsius_to_fahrenheit"
    fn_body_1 = "function (celsius) { return celsius * 1.8 + 32; }"
    fn_name_2 = "functions::temperature::fahrenheit_to_celsius"
    fn_body_2 = "function (fahrenheit) { return (fahrenheit - 32) / 1.8; }"
    bad_fn_name = "functions::temperature::should_not_exist"
    bad_fn_body = "function (celsius) { invalid syntax }"

    aql = db.aql
    # List AQL functions
    assert await aql.functions() == []

    # List AQL functions with bad database
    with pytest.raises(AQLFunctionListError) as err:
        await bad_db.aql.functions()
    assert err.value.error_code == FORBIDDEN

    # Create invalid AQL function
    with pytest.raises(AQLFunctionCreateError) as err:
        await aql.create_function(bad_fn_name, bad_fn_body)
    assert err.value.error_code == QUERY_FUNCTION_INVALID_CODE

    # Create first AQL function
    result = await aql.create_function(fn_name_1, fn_body_1, is_deterministic=True)
    assert result["isNewlyCreated"] is True
    functions = await aql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert functions[0]["isDeterministic"] is True

    # Create same AQL function again
    result = await aql.create_function(fn_name_1, fn_body_1, is_deterministic=True)
    assert result["isNewlyCreated"] is False
    functions = await aql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert functions[0]["isDeterministic"] is True

    # Create second AQL function
    result = await aql.create_function(fn_name_2, fn_body_2, is_deterministic=False)
    assert result["isNewlyCreated"] is True
    functions = await aql.functions()
    assert len(functions) == 2
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert functions[0]["isDeterministic"] is True
    assert functions[1]["name"] == fn_name_2
    assert functions[1]["code"] == fn_body_2
    assert functions[1]["isDeterministic"] is False

    # Delete first function
    result = await aql.delete_function(fn_name_1)
    assert result["deletedCount"] == 1
    functions = await aql.functions()
    assert len(functions) == 1

    # Delete missing function
    with pytest.raises(AQLFunctionDeleteError) as err:
        await aql.delete_function(fn_name_1)
    assert err.value.error_code == QUERY_FUNCTION_NOT_FOUND
    result = await aql.delete_function(fn_name_1, ignore_missing=True)
    assert "deletedCount" not in result

    # Delete function from bad db
    with pytest.raises(AQLFunctionDeleteError) as err:
        await bad_db.aql.delete_function(fn_name_2)
    assert err.value.error_code == FORBIDDEN

    # Delete function group
    result = await aql.delete_function(fn_group, group=True)
    assert result["deletedCount"] == 1
    functions = await aql.functions()
    assert len(functions) == 0
