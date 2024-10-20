import pytest

from arangoasync.exceptions import UserCreateError, UserDeleteError, UserListError
from arangoasync.typings import UserInfo
from tests.helpers import generate_string, generate_username


@pytest.mark.asyncio
async def test_user_management(sys_db, db, bad_db):
    # Create a user
    username = generate_username()
    password = generate_string()
    users = await sys_db.users()
    assert not any(user.user == username for user in users)
    assert await sys_db.has_user(username) is False

    # Should not be able to create a user without permission
    with pytest.raises(UserCreateError):
        await db.create_user(
            UserInfo(
                user=username,
                password=password,
                active=True,
                extra={"foo": "bar"},
            )
        )

    # Verify user creation
    new_user = await sys_db.create_user(
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
    users = await sys_db.users()
    assert sum(user.user == username for user in users) == 1
    assert await sys_db.has_user(username) is True
    user = await sys_db.user(username)
    assert user.user == username
    assert user.active is True

    # Create already existing user
    with pytest.raises(UserCreateError):
        await sys_db.create_user(
            UserInfo(
                user=username,
                password=password,
                active=True,
                extra={"foo": "bar"},
            )
        )

    # Delete the newly created user
    assert await sys_db.delete_user(username) is True
    users = await sys_db.users()
    assert not any(user.user == username for user in users)
    assert await sys_db.has_user(username) is False

    # Ignore missing user
    assert await sys_db.delete_user(username, ignore_missing=True) is False

    # Cannot delete user without permission
    with pytest.raises(UserDeleteError):
        await db.delete_user(username)

    # Cannot list users with a non-existing database
    with pytest.raises(UserListError):
        await bad_db.users()
    with pytest.raises(UserListError):
        await bad_db.has_user(username)
