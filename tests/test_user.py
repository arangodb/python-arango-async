import pytest

from arangoasync.auth import Auth
from arangoasync.errno import USER_NOT_FOUND
from arangoasync.exceptions import (
    CollectionCreateError,
    DocumentInsertError,
    PermissionResetError,
    PermissionUpdateError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
)
from arangoasync.typings import UserInfo
from tests.helpers import generate_col_name, generate_string, generate_username


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

    # Get non-existing user
    with pytest.raises(UserGetError):
        await sys_db.user(generate_username())

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

    # Update existing user
    new_user = await sys_db.update_user(
        UserInfo(
            user=username,
            password=password,
            active=False,
            extra={"bar": "baz"},
        )
    )
    assert new_user["user"] == username
    assert new_user["active"] is False
    assert new_user["extra"] == {"foo": "bar", "bar": "baz"}
    assert await sys_db.user(username) == new_user

    # Update missing user
    with pytest.raises(UserUpdateError) as err:
        await sys_db.update_user(
            UserInfo(user=generate_username(), password=generate_string())
        )
    assert err.value.error_code == USER_NOT_FOUND

    # Replace existing user
    new_user = await sys_db.replace_user(
        UserInfo(
            user=username,
            password=password,
            active=True,
            extra={"baz": "qux"},
        )
    )
    assert new_user["user"] == username
    assert new_user["active"] is True
    assert new_user["extra"] == {"baz": "qux"}
    assert await sys_db.user(username) == new_user

    # Replace missing user
    with pytest.raises(UserReplaceError) as err:
        await sys_db.replace_user(
            {"user": generate_username(), "password": generate_string()}
        )
    assert err.value.error_code == USER_NOT_FOUND

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


@pytest.mark.asyncio
async def test_user_change_permissions(sys_db, arango_client, db):
    username = generate_username()
    password = generate_string()
    auth = Auth(username, password)

    # Set read-only permissions
    await sys_db.create_user(UserInfo(username, password))

    # Should not be able to update permissions without permission
    with pytest.raises(PermissionUpdateError):
        await db.update_permission(username, "ro", db.name)

    await sys_db.update_permission(username, "ro", db.name)

    # Verify read-only permissions
    permission = await sys_db.permission(username, db.name)
    assert permission == "ro"

    # Should not be able to create a collection
    col_name = generate_col_name()
    db2 = await arango_client.db(db.name, auth=auth, verify=True)
    with pytest.raises(CollectionCreateError):
        await db2.create_collection(col_name)

    all_permissions = await sys_db.permissions(username)
    assert "_system" in all_permissions
    assert db.name in all_permissions
    all_permissions = await sys_db.permissions(username, full=False)
    assert all_permissions[db.name] == "ro"

    # Set read-write permissions
    await sys_db.update_permission(username, "rw", db.name)

    # Should be able to create collection
    col = await db2.create_collection(col_name)
    await col.insert({"_key": "test"})

    # Reset permissions
    with pytest.raises(PermissionResetError):
        await db.reset_permission(username, db.name)
    await sys_db.reset_permission(username, db.name)
    with pytest.raises(DocumentInsertError):
        await col.insert({"_key": "test"})

    # Allow rw access
    await sys_db.update_permission(username, "rw", db.name)
    await col.insert({"_key": "test2"})

    # No access to collection
    await sys_db.update_permission(username, "none", db.name, col_name)
    with pytest.raises(DocumentInsertError):
        await col.insert({"_key": "test"})

    await db.delete_collection(col_name)
