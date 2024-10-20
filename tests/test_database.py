import pytest

from arangoasync.collection import StandardCollection
from tests.helpers import generate_col_name, generate_db_name


@pytest.mark.asyncio
async def test_database_misc_methods(sys_db):
    status = await sys_db.status()
    assert status["server"] == "arango"


@pytest.mark.asyncio
async def test_create_drop_database(arango_client, sys_db, basic_auth_root):
    # TODO also handle exceptions
    # TODO use more options (cluster must be enabled for that)

    # Create a new database
    db_name = generate_db_name()
    assert await sys_db.create_database(db_name) is True
    new_db = await arango_client.db(
        db_name, auth_method="basic", auth=basic_auth_root, verify=True
    )
    assert await sys_db.has_database(db_name) is True

    # List available databases
    dbs = await sys_db.databases()
    assert db_name in dbs
    assert "_system" in dbs

    # TODO move this to a separate test for documents
    col_name = generate_col_name()
    col = await new_db.create_collection(col_name)
    await col.insert({"_key": "1", "a": 1})
    doc = await col.get("1")
    assert doc["_key"] == "1"

    # Drop the newly created database
    assert await sys_db.delete_database(db_name) is True
    non_existent_db = generate_db_name()
    assert await sys_db.has_database(non_existent_db) is False
    assert await sys_db.delete_database(non_existent_db, ignore_missing=True) is False


@pytest.mark.asyncio
async def test_create_drop_collection(test_db):
    # TODO also handle exceptions

    # Create a new collection
    col_name = generate_col_name()
    col = await test_db.create_collection(col_name)
    assert isinstance(col, StandardCollection)
    assert await test_db.has_collection(col_name)
    cols = await test_db.collections()
    assert any(c.name == col_name for c in cols)

    # Drop the newly created collection
    assert await test_db.delete_collection(col_name) is True
    assert not await test_db.has_collection(col_name)
    non_existent_col = generate_col_name()
    assert await test_db.has_collection(non_existent_col) is False
    assert (
        await test_db.delete_collection(non_existent_col, ignore_missing=True) is False
    )
