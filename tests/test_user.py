import pytest

from arangoasync.auth import Auth
from arangoasync.client import ArangoClient
from arangoasync.typings import UserInfo
from tests.helpers import generate_string, generate_username


@pytest.mark.asyncio
async def test_user_management(url, sys_db_name, root, password):
    auth = Auth(username=root, password=password)

    # TODO also handle exceptions
    async with ArangoClient(hosts=url) as client:
        db = await client.db(sys_db_name, auth_method="basic", auth=auth, verify=True)

        # Create a user
        username = generate_username()
        password = generate_string()
        users = await db.users()
        assert not any(user.user == username for user in users)
        assert await db.has_user(username) is False

        # Verify user creation
        new_user = await db.create_user(
            UserInfo(
                user=username,
                password=password,
                active=True,
                extra={"foo": "bar"},
            )
        )
        assert new_user.user == username
        assert new_user.active is True
        assert new_user.extra == {"foo": "bar"}
        users = await db.users()
        assert sum(user.user == username for user in users) == 1
        assert await db.has_user(username) is True
        user = await db.user(username)
        assert user.user == username
        assert user.active is True

        # Delete the newly created user
        assert await db.delete_user(username) is True
        users = await db.users()
        assert not any(user.user == username for user in users)
        assert await db.has_user(username) is False

        # Ignore missing user
        assert await db.delete_user(username, ignore_missing=True) is False
