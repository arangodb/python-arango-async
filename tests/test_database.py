import pytest

from arangoasync.auth import Auth
from arangoasync.client import ArangoClient


@pytest.mark.asyncio
async def test_database_misc_methods(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO create a test database and user
    async with ArangoClient(hosts=url) as client:
        db = await client.db(sys_db_name, auth_method="basic", auth=auth, verify=True)
        status = await db.status()
        assert status["server"] == "arango"
