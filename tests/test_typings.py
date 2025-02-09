import pytest

from arangoasync.typings import (
    CollectionInfo,
    CollectionStatus,
    CollectionType,
    JsonWrapper,
    KeyOptions,
    QueryCacheProperties,
    QueryExecutionExtra,
    QueryExecutionPlan,
    QueryExecutionProfile,
    QueryExecutionStats,
    QueryExplainOptions,
    QueryProperties,
    QueryTrackingConfiguration,
    UserInfo,
)


def test_basic_wrapper():
    wrapper = JsonWrapper({"a": 1, "b": 2})
    assert wrapper["a"] == 1
    assert wrapper["b"] == 2

    wrapper["c"] = 3
    assert wrapper["c"] == 3

    del wrapper["a"]
    assert "a" not in wrapper

    wrapper = JsonWrapper({"a": 1, "b": 2})
    keys = list(iter(wrapper))
    assert keys == ["a", "b"]
    assert len(wrapper) == 2

    assert "a" in wrapper
    assert "c" not in wrapper

    assert repr(wrapper) == "JsonWrapper({'a': 1, 'b': 2})"
    wrapper = JsonWrapper({"a": 1, "b": 2})
    assert str(wrapper) == "{'a': 1, 'b': 2}"
    assert wrapper == {"a": 1, "b": 2}

    assert wrapper.get("a") == 1
    assert wrapper.get("c", 3) == 3

    items = list(wrapper.items())
    assert items == [("a", 1), ("b", 2)]
    assert wrapper.to_dict() == {"a": 1, "b": 2}


def test_KeyOptions():
    options = KeyOptions(generator_type="autoincrement")
    options.validate()
    with pytest.raises(ValueError, match="Invalid key generator type 'invalid_type'"):
        KeyOptions(generator_type="invalid_type").validate()
    with pytest.raises(ValueError, match='"increment" value'):
        KeyOptions(generator_type="uuid", increment=5).validate()
    with pytest.raises(ValueError, match='"offset" value'):
        KeyOptions(generator_type="uuid", offset=5).validate()
    with pytest.raises(ValueError, match='"type" value'):
        KeyOptions(data={"allowUserKeys": True}).validate()
    with pytest.raises(ValueError, match='"allowUserKeys" value'):
        KeyOptions(data={"type": "autoincrement"}).validate()


def test_CollectionType():
    assert CollectionType.from_int(2) == CollectionType.DOCUMENT
    assert CollectionType.from_int(3) == CollectionType.EDGE
    with pytest.raises(ValueError, match="Invalid collection type value: 1"):
        CollectionType.from_int(1)
    assert CollectionType.from_str("document") == CollectionType.DOCUMENT
    assert CollectionType.from_str("edge") == CollectionType.EDGE
    with pytest.raises(ValueError, match="Invalid collection type value: invalid"):
        CollectionType.from_str("invalid")


def test_CollectionStatus():
    assert CollectionStatus.from_int(1) == CollectionStatus.NEW
    assert CollectionStatus.from_int(2) == CollectionStatus.UNLOADED
    assert CollectionStatus.from_int(3) == CollectionStatus.LOADED
    assert CollectionStatus.from_int(4) == CollectionStatus.UNLOADING
    assert CollectionStatus.from_int(5) == CollectionStatus.DELETED
    assert CollectionStatus.from_int(6) == CollectionStatus.LOADING
    with pytest.raises(ValueError, match="Invalid collection status value: 0"):
        CollectionStatus.from_int(0)


def test_CollectionInfo():
    data = {
        "id": "151",
        "name": "animals",
        "status": 3,
        "type": 2,
        "isSystem": False,
        "globallyUniqueId": "hDA74058C1843/151",
    }
    info = CollectionInfo(data)
    assert info.globally_unique_id == "hDA74058C1843/151"
    assert info.is_system is False
    assert info.name == "animals"
    assert info.status == CollectionStatus.LOADED
    assert info.col_type == CollectionType.DOCUMENT

    # Custom formatter
    formatted_data = info.format(
        lambda x: {k: v.upper() if isinstance(v, str) else v for k, v in x.items()}
    )
    assert formatted_data["name"] == "ANIMALS"

    # Default formatter
    formatted_data = info.format()
    assert formatted_data == {
        "id": "151",
        "name": "animals",
        "system": False,
        "type": "document",
        "status": "loaded",
    }


def test_UserInfo():
    data = {
        "user": "john",
        "password": "secret",
        "active": True,
        "extra": {"role": "admin"},
    }
    user_info = UserInfo(**data)
    assert user_info.user == "john"
    assert user_info.password == "secret"
    assert user_info.active is True
    assert user_info.extra == {"role": "admin"}
    assert user_info.to_dict() == data


def test_QueryProperties():
    properties = QueryProperties(
        allow_dirty_reads=True,
        allow_retry=False,
        fail_on_warning=True,
        fill_block_cache=False,
        full_count=True,
        intermediate_commit_count=1000,
        intermediate_commit_size=1048576,
        max_dnf_condition_members=10,
        max_nodes_per_callstack=100,
        max_number_of_plans=5,
        max_runtime=60.0,
        max_transaction_size=10485760,
        max_warning_count=10,
        optimizer={"rules": ["-all", "+use-indexes"]},
        profile=1,
        satellite_sync_wait=10.0,
        skip_inaccessible_collections=True,
        spill_over_threshold_memory_usage=10485760,
        spill_over_threshold_num_rows=100000,
        stream=True,
        use_plan_cache=True,
    )
    assert properties.allow_dirty_reads is True
    assert properties.allow_retry is False
    assert properties.fail_on_warning is True
    assert properties.fill_block_cache is False
    assert properties.full_count is True
    assert properties.intermediate_commit_count == 1000
    assert properties.intermediate_commit_size == 1048576
    assert properties.max_dnf_condition_members == 10
    assert properties.max_nodes_per_callstack == 100
    assert properties.max_number_of_plans == 5
    assert properties.max_runtime == 60.0
    assert properties.max_transaction_size == 10485760
    assert properties.max_warning_count == 10
    assert properties.optimizer == {"rules": ["-all", "+use-indexes"]}
    assert properties.profile == 1
    assert properties.satellite_sync_wait == 10.0
    assert properties.skip_inaccessible_collections is True
    assert properties.spill_over_threshold_memory_usage == 10485760
    assert properties.spill_over_threshold_num_rows == 100000
    assert properties.stream is True
    assert properties.use_plan_cache is True


def test_QueryExecutionPlan():
    data = {
        "collections": [{"name": "test_collection"}],
        "estimatedCost": 10.5,
        "estimatedNrItems": 100,
        "isModificationQuery": False,
        "nodes": [{"type": "SingletonNode"}],
        "rules": ["rule1", "rule2"],
        "variables": [{"name": "var1"}],
    }
    plan = QueryExecutionPlan(data)
    assert plan.collections == [{"name": "test_collection"}]
    assert plan.estimated_cost == 10.5
    assert plan.estimated_nr_items == 100
    assert plan.is_modification_query is False
    assert plan.nodes == [{"type": "SingletonNode"}]
    assert plan.rules == ["rule1", "rule2"]
    assert plan.variables == [{"name": "var1"}]


def test_QueryExecutionProfile():
    data = {
        "initializing": 0.0000028529999838156073,
        "parsing": 0.000029285000010759177,
        "optimizing ast": 0.0000040699999885873694,
        "loading collections": 0.000012807000018710823,
        "instantiating plan": 0.00002348999998957879,
        "optimizing plan": 0.00006598600000984334,
        "instantiating executors": 0.000027471999999306718,
        "executing": 0.7550992429999894,
        "finalizing": 0.00004103500000951499,
    }
    profile = QueryExecutionProfile(data)
    assert profile.initializing == 0.0000028529999838156073
    assert profile.parsing == 0.000029285000010759177
    assert profile.optimizing_ast == 0.0000040699999885873694
    assert profile.loading_collections == 0.000012807000018710823
    assert profile.instantiating_plan == 0.00002348999998957879
    assert profile.optimizing_plan == 0.00006598600000984334
    assert profile.instantiating_executors == 0.000027471999999306718
    assert profile.executing == 0.7550992429999894
    assert profile.finalizing == 0.00004103500000951499


def test_QueryExecutionStats():
    data = {
        "writesExecuted": 10,
        "writesIgnored": 2,
        "scannedFull": 100,
        "scannedIndex": 50,
        "filtered": 20,
        "httpRequests": 5,
        "executionTime": 0.123,
        "peakMemoryUsage": 1024,
    }
    stats = QueryExecutionStats(data)
    assert stats.writes_executed == 10
    assert stats.writes_ignored == 2
    assert stats.scanned_full == 100
    assert stats.scanned_index == 50
    assert stats.filtered == 20
    assert stats.http_requests == 5
    assert stats.execution_time == 0.123
    assert stats.peak_memory_usage == 1024


def test_QueryExecutionExtra():
    data = {
        "plan": {
            "collections": [{"name": "test_collection"}],
            "estimatedCost": 10.5,
            "estimatedNrItems": 100,
            "isModificationQuery": False,
            "nodes": [{"type": "SingletonNode"}],
            "rules": ["rule1", "rule2"],
            "variables": [{"name": "var1"}],
        },
        "profile": {
            "initializing": 0.0000028529999838156073,
            "parsing": 0.000029285000010759177,
            "optimizing ast": 0.0000040699999885873694,
            "loading collections": 0.000012807000018710823,
            "instantiating plan": 0.00002348999998957879,
            "optimizing plan": 0.00006598600000984334,
            "instantiating executors": 0.000027471999999306718,
            "executing": 0.7550992429999894,
            "finalizing": 0.00004103500000951499,
        },
        "stats": {
            "writesExecuted": 10,
            "writesIgnored": 2,
            "scannedFull": 100,
            "scannedIndex": 50,
            "filtered": 20,
            "httpRequests": 5,
            "executionTime": 0.123,
            "peakMemoryUsage": 1024,
        },
        "warnings": [{"code": 123, "message": "test warning"}],
    }
    extra = QueryExecutionExtra(data)
    assert isinstance(extra.plan, QueryExecutionPlan)
    assert isinstance(extra.profile, QueryExecutionProfile)
    assert isinstance(extra.stats, QueryExecutionStats)
    assert extra.warnings == [{"code": 123, "message": "test warning"}]


def test_QueryTrackingConfiguration():
    data = {
        "enabled": True,
        "trackSlowQueries": True,
        "trackBindVars": True,
        "maxSlowQueries": 64,
        "slowQueryThreshold": 10,
        "slowStreamingQueryThreshold": 10,
        "maxQueryStringLength": 4096,
    }
    config = QueryTrackingConfiguration(data)
    assert config.enabled is True
    assert config.track_slow_queries is True
    assert config.track_bind_vars is True
    assert config.max_slow_queries == 64
    assert config.slow_query_threshold == 10
    assert config.slow_streaming_query_threshold == 10
    assert config.max_query_string_length == 4096


def test_QueryExplainOptions():
    options = QueryExplainOptions(
        all_plans=True, max_plans=5, optimizer={"rules": ["-all", "+use-index-range"]}
    )
    assert options.all_plans is True
    assert options.max_plans == 5
    assert options.optimizer == {"rules": ["-all", "+use-index-range"]}


def test_QueryCacheProperties():
    data = {
        "mode": "demand",
        "maxResults": 128,
        "maxEntrySize": 1024,
        "includeSystem": False,
    }
    cache_properties = QueryCacheProperties(data)
    assert cache_properties._data["mode"] == "demand"
    assert cache_properties._data["maxResults"] == 128
    assert cache_properties._data["maxEntrySize"] == 1024
    assert cache_properties._data["includeSystem"] is False
