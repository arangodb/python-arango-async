import pytest

from arangoasync.auth import Auth
from arangoasync.client import ArangoClient
from arangoasync.collection import StandardCollection
from tests.helpers import generate_col_name


@pytest.mark.asyncio
async def test_database_misc_methods(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO also handle exceptions
    async with ArangoClient(hosts=url) as client:
        db = await client.db(sys_db_name, auth_method="basic", auth=auth, verify=True)
        status = await db.status()
        assert status["server"] == "arango"


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
        await db.delete_collection(col_name)
        assert not await db.has_collection(col_name)
        non_existent_col = generate_col_name()
        assert await db.has_collection(non_existent_col) is False
