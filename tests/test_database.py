import pytest

from arangoasync.auth import Auth
from arangoasync.client import ArangoClient
from arangoasync.collection import StandardCollection
from tests.helpers import generate_col_name, generate_db_name


@pytest.mark.asyncio
async def test_database_misc_methods(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO also handle exceptions
    async with ArangoClient(hosts=url) as client:
        db = await client.db(sys_db_name, auth_method="basic", auth=auth, verify=True)
        status = await db.status()
        assert status["server"] == "arango"


@pytest.mark.asyncio
async def test_create_drop_database(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO also handle exceptions
    # TODO use more options (cluster must be enabled for that)
    async with ArangoClient(hosts=url) as client:
        sys_db = await client.db(
            sys_db_name, auth_method="basic", auth=auth, verify=True
        )
        db_name = generate_db_name()
        assert await sys_db.create_database(db_name) is True
        await client.db(db_name, auth_method="basic", auth=auth, verify=True)
        assert await sys_db.has_database(db_name) is True
        assert await sys_db.delete_database(db_name) is True
        non_existent_db = generate_db_name()
        assert await sys_db.has_database(non_existent_db) is False
        assert (
            await sys_db.delete_database(non_existent_db, ignore_missing=True) is False
        )


@pytest.mark.asyncio
async def test_create_drop_collection(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO also handle exceptions
    async with ArangoClient(hosts=url) as client:
        db = await client.db(sys_db_name, auth_method="basic", auth=auth, verify=True)
        col_name = generate_col_name()
        col = await db.create_collection(col_name)
        assert isinstance(col, StandardCollection)
        assert await db.has_collection(col_name)
        assert await db.delete_collection(col_name) is True
        assert not await db.has_collection(col_name)
        non_existent_col = generate_col_name()
        assert await db.has_collection(non_existent_col) is False
        assert (
            await db.delete_collection(non_existent_col, ignore_missing=True) is False
        )
