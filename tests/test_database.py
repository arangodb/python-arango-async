import asyncio
import datetime
import json

import pytest
from packaging import version

from arangoasync.client import ArangoClient
from arangoasync.collection import StandardCollection
from arangoasync.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionKeyGeneratorsError,
    CollectionListError,
    DatabaseCompactError,
    DatabaseCreateError,
    DatabaseDeleteError,
    DatabaseListError,
    DatabasePropertiesError,
    DatabaseSupportInfoError,
    JWTSecretListError,
    JWTSecretReloadError,
    ReplicationApplierConfigError,
    ReplicationApplierStateError,
    ReplicationClusterInventoryError,
    ReplicationDumpError,
    ReplicationInventoryError,
    ReplicationLoggerStateError,
    ReplicationServerIDError,
    ServerApiCallsError,
    ServerAvailableOptionsGetError,
    ServerCheckAvailabilityError,
    ServerCurrentOptionsGetError,
    ServerEchoError,
    ServerEngineError,
    ServerExecuteError,
    ServerLicenseGetError,
    ServerLicenseSetError,
    ServerLogLevelError,
    ServerLogLevelResetError,
    ServerLogLevelSetError,
    ServerLogSettingError,
    ServerLogSettingSetError,
    ServerMetricsError,
    ServerModeError,
    ServerModeSetError,
    ServerReadLogError,
    ServerReloadRoutingError,
    ServerShutdownError,
    ServerShutdownProgressError,
    ServerStatusError,
    ServerTimeError,
    ServerVersionError,
)
from arangoasync.request import Method, Request
from arangoasync.typings import CollectionType, KeyOptions, UserInfo
from tests.helpers import generate_col_name, generate_db_name, generate_username


@pytest.mark.asyncio
async def test_database_misc_methods(
    sys_db, db, bad_db, cluster, db_version, url, sys_db_name, token, skip_tests
):
    # Status
    status = await sys_db.status()
    assert status["server"] == "arango"
    with pytest.raises(ServerStatusError):
        await bad_db.status()

    sys_properties, db_properties = await asyncio.gather(
        sys_db.properties(), db.properties()
    )
    assert sys_properties.is_system is True
    assert db_properties.is_system is False
    assert sys_properties.name == sys_db.name
    assert db_properties.name == db.name
    if cluster:
        assert db_properties.replication_factor == 3
        assert db_properties.write_concern == 2

    with pytest.raises(DatabasePropertiesError):
        await bad_db.properties()
    assert len(db_properties.format()) > 1

    # JWT secrets
    with pytest.raises(JWTSecretListError):
        await bad_db.jwt_secrets()
    with pytest.raises(JWTSecretReloadError):
        await bad_db.reload_jwt_secrets()

    # Version
    v = await sys_db.version()
    assert v["version"].startswith("3.")
    with pytest.raises(ServerVersionError):
        await bad_db.version()

    # key generators
    if db_version >= version.parse("3.12.0"):
        key_generators = await db.key_generators()
        assert isinstance(key_generators, list)
        with pytest.raises(CollectionKeyGeneratorsError):
            await bad_db.key_generators()

    # Administration
    with pytest.raises(ServerEngineError):
        await bad_db.engine()
    result = await db.engine()
    assert isinstance(result, dict)

    with pytest.raises(ServerTimeError):
        await bad_db.time()
    time = await db.time()
    assert isinstance(time, datetime.datetime)

    with pytest.raises(ServerCheckAvailabilityError):
        await bad_db.check_availability()
    assert isinstance(await db.check_availability(), str)

    with pytest.raises(DatabaseSupportInfoError):
        await bad_db.support_info()
    info = await sys_db.support_info()
    assert isinstance(info, dict)

    if db_version >= version.parse("3.12.0"):
        with pytest.raises(ServerCurrentOptionsGetError):
            await bad_db.options()
        options = await sys_db.options()
        assert isinstance(options, dict)
        with pytest.raises(ServerAvailableOptionsGetError):
            await bad_db.options_available()
        options_available = await sys_db.options_available()
        assert isinstance(options_available, dict)

    with pytest.raises(ServerModeError):
        await bad_db.mode()
    mode = await sys_db.mode()
    assert isinstance(mode, str)
    with pytest.raises(ServerModeSetError):
        await bad_db.set_mode("foo")
    mode = await sys_db.set_mode("default")
    assert isinstance(mode, str)

    with pytest.raises(ServerLicenseGetError):
        await bad_db.license()
    license = await sys_db.license()
    assert isinstance(license, dict)
    with pytest.raises(ServerLicenseSetError):
        await sys_db.set_license('"abc"')

    with pytest.raises(ServerShutdownError):
        await bad_db.shutdown()
    with pytest.raises(ServerShutdownProgressError):
        await bad_db.shutdown_progress()

    with pytest.raises(ServerReloadRoutingError):
        await bad_db.reload_routing()
    await sys_db.reload_routing()

    with pytest.raises(ServerEchoError):
        await bad_db.echo()
    result = await sys_db.echo()
    assert isinstance(result, dict)

    with pytest.raises(ServerExecuteError):
        await bad_db.execute("return 1")
    result = await sys_db.execute("return 1")
    assert result == 1

    with pytest.raises(DatabaseCompactError):
        await bad_db.compact()
    async with ArangoClient(hosts=url) as client:
        db = await client.db(
            sys_db_name, auth_method="superuser", token=token, verify=True
        )
        await db.compact()

    # Custom Request
    request = Request(
        method=Method.POST, endpoint="/_admin/execute", data="return 1".encode("utf-8")
    )
    response = await sys_db.request(request)
    assert json.loads(response.raw_body) == 1

    if "enterprise" not in skip_tests and db_version >= version.parse("3.12.0"):
        # API calls
        with pytest.raises(ServerApiCallsError):
            await bad_db.api_calls()
        result = await sys_db.api_calls()
        assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_metrics(db, bad_db):
    with pytest.raises(ServerMetricsError):
        await bad_db.metrics()
    metrics = await db.metrics()
    assert isinstance(metrics, str)


@pytest.mark.asyncio
async def test_replication(db, bad_db, cluster):
    with pytest.raises(ReplicationInventoryError):
        await bad_db.replication.inventory("id")
    with pytest.raises(ReplicationDumpError):
        await bad_db.replication.dump("test_collection")
    if cluster:
        with pytest.raises(ReplicationClusterInventoryError):
            await bad_db.replication.cluster_inventory()
        result = await db.replication.cluster_inventory()
        assert isinstance(result, dict)
    if not cluster:
        with pytest.raises(ReplicationLoggerStateError):
            await bad_db.replication.logger_state()
        result = await db.replication.logger_state()
        assert isinstance(result, dict)
        with pytest.raises(ReplicationApplierConfigError):
            await bad_db.replication.applier_config()
        result = await db.replication.applier_config()
        assert isinstance(result, dict)
        with pytest.raises(ReplicationApplierStateError):
            await bad_db.replication.applier_state()
        result = await db.replication.applier_state()
        assert isinstance(result, dict)
    with pytest.raises(ReplicationServerIDError):
        await bad_db.replication.server_id()
    result = await db.replication.server_id()
    assert isinstance(result, str)


@pytest.mark.asyncio
async def test_logs(sys_db, bad_db):
    with pytest.raises(ServerReadLogError):
        await bad_db.read_log_entries()
    result = await sys_db.read_log_entries()
    assert isinstance(result, dict)
    with pytest.raises(ServerLogLevelError):
        await bad_db.log_levels()
    result = await sys_db.log_levels()
    assert isinstance(result, dict)
    with pytest.raises(ServerLogLevelSetError):
        await bad_db.set_log_levels()
    new_levels = {"agency": "DEBUG", "engines": "INFO", "threads": "WARNING"}
    result = await sys_db.set_log_levels(**new_levels)
    assert isinstance(result, dict)
    with pytest.raises(ServerLogLevelResetError):
        await bad_db.reset_log_levels()
    result = await sys_db.reset_log_levels()
    assert isinstance(result, dict)
    with pytest.raises(ServerLogSettingError):
        await bad_db.log_settings()
    result = await sys_db.log_settings()
    assert isinstance(result, dict)
    with pytest.raises(ServerLogSettingSetError):
        await bad_db.set_log_settings()
    result = await sys_db.set_log_settings()
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_create_drop_database(
    arango_client,
    sys_db,
    db,
    bad_db,
    basic_auth_root,
    password,
    cluster,
):
    # Create a new database
    db_name = generate_db_name()
    db_kwargs = dict(
        name=db_name,
        users=[
            dict(username=generate_username(), password=password, active=True),
            UserInfo(user=generate_username(), password=password, active=True),
        ],
    )
    if cluster:
        db_kwargs["replication_factor"] = 3
        db_kwargs["write_concern"] = 2
        db_kwargs["sharding"] = "flexible"

    assert await sys_db.create_database(**db_kwargs) is True
    await arango_client.db(
        db_name, auth_method="basic", auth=basic_auth_root, verify=True
    )
    assert await sys_db.has_database(db_name) is True

    # Try to create a database without permissions
    with pytest.raises(DatabaseCreateError):
        await db.create_database(generate_db_name())

    # Try to create a database that already exists
    with pytest.raises(DatabaseCreateError):
        await sys_db.create_database(db_name)

    # List available databases
    dbs = await sys_db.databases()
    assert db_name in dbs
    assert "_system" in dbs
    dbs = await sys_db.databases_accessible_to_user()
    assert db_name in dbs
    assert "_system" in dbs
    dbs = await db.databases_accessible_to_user()
    assert db.name in dbs

    # Cannot list databases without permission
    with pytest.raises(DatabaseListError):
        await db.databases()
    with pytest.raises(DatabaseListError):
        await db.has_database(db_name)
    with pytest.raises(DatabaseListError):
        await bad_db.databases_accessible_to_user()

    # Databases can only be dropped from the system database
    with pytest.raises(DatabaseDeleteError):
        await db.delete_database(db_name)

    # Drop the newly created database
    assert await sys_db.delete_database(db_name) is True
    non_existent_db = generate_db_name()
    assert await sys_db.has_database(non_existent_db) is False
    assert await sys_db.delete_database(non_existent_db, ignore_missing=True) is False


@pytest.mark.asyncio
async def test_create_drop_collection(db, bad_db, cluster):
    # Create a new collection
    col_name = generate_col_name()
    col = await db.create_collection(col_name)
    assert isinstance(col, StandardCollection)
    assert await db.has_collection(col_name)
    cols = await db.collections()
    assert any(c.name == col_name for c in cols)

    # Try to create a collection that already exists
    with pytest.raises(CollectionCreateError):
        await db.create_collection(col_name)

    # Try collection methods from a non-existent db
    with pytest.raises(CollectionCreateError):
        await bad_db.create_collection(generate_col_name())
    with pytest.raises(CollectionListError):
        await bad_db.collections()
    with pytest.raises(CollectionListError):
        await bad_db.has_collection(col_name)

    # Try to create a collection with invalid args
    with pytest.raises(ValueError):
        await db.create_collection(generate_col_name(), col_type="invalid")
    with pytest.raises(ValueError):
        await db.create_collection(generate_col_name(), col_type=db)
    with pytest.raises(ValueError):
        await db.create_collection(generate_col_name(), key_options={})

    # Drop the newly created collection
    assert await db.delete_collection(col_name) is True
    assert not await db.has_collection(col_name)
    non_existent_col = generate_col_name()
    assert await db.has_collection(non_existent_col) is False
    assert await db.delete_collection(non_existent_col, ignore_missing=True) is False

    # Do not ignore missing collection
    with pytest.raises(CollectionDeleteError):
        await db.delete_collection(non_existent_col)

    # Multiple arguments in a cluster setup
    if cluster:
        schema = {
            "rule": {
                "type": "object",
                "properties": {
                    "test_attr:": {"type": "string"},
                },
                "required": ["test_attr"],
            },
            "level": "moderate",
            "message": "Schema Validation Failed.",
            "type": "json",
        }

        computed_values = [
            {
                "name": "foo",
                "expression": "RETURN 1",
                "computeOn": ["insert", "update", "replace"],
                "overwrite": True,
                "failOnWarning": False,
                "keepNull": True,
            }
        ]

        col = await db.create_collection(
            col_name,
            col_type=CollectionType.DOCUMENT,
            write_concern=2,
            wait_for_sync=True,
            number_of_shards=1,
            is_system=False,
            computed_values=computed_values,
            schema=schema,
            key_options=KeyOptions(
                allow_user_keys=True,
                generator_type="autoincrement",
                increment=5,
                offset=10,
            ),
        )
        assert col.name == col_name
        assert await db.delete_collection(col_name) is True
