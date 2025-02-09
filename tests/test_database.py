import asyncio

import pytest

from arangoasync.collection import StandardCollection
from arangoasync.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    DatabaseCreateError,
    DatabaseDeleteError,
    DatabaseListError,
    DatabasePropertiesError,
    JWTSecretListError,
    JWTSecretReloadError,
    ServerStatusError,
    ServerVersionError,
)
from arangoasync.typings import CollectionType, KeyOptions, UserInfo
from tests.helpers import generate_col_name, generate_db_name, generate_username


@pytest.mark.asyncio
async def test_database_misc_methods(sys_db, db, bad_db, cluster):
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
    version = await sys_db.version()
    assert version["version"].startswith("3.")
    with pytest.raises(ServerVersionError):
        await bad_db.version()


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
