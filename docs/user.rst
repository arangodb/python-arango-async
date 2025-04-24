Users and Permissions
---------------------

ArangoDB provides operations for managing users and permissions. Most of
these operations can only be performed by admin users via ``_system`` database.

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.typings import UserInfo

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "_system" database as root user.
        sys_db = await client.db("_system", auth=auth)

        # List all users.
        users = await sys_db.users()

        johndoe = UserInfo(
            user="johndoe@gmail.com",
            password="first_password",
            active=True,
            extra={"team": "backend", "title": "engineer"}
        )

        # Create a new user.
        await sys_db.create_user(johndoe)

        # Check if a user exists.
        assert await sys_db.has_user(johndoe.user) is True
        assert await sys_db.has_user("johndoe@gmail.com") is True

        # Retrieve details of a user.
        user_info = await sys_db.user(johndoe.user)
        assert user_info.user == "johndoe@gmail.com"

        # Update an existing user.
        johndoe["password"] = "second_password"
        await sys_db.update_user(johndoe)

        # Replace an existing user.
        johndoe["password"] = "third_password"
        await sys_db.replace_user(johndoe)

        # Retrieve user permissions for all databases and collections.
        await sys_db.permissions(johndoe.user)

        # Retrieve user permission for "test" database.
        perm = await sys_db.permission(
            username="johndoe@gmail.com",
            database="test"
        )

        # Retrieve user permission for "students" collection in "test" database.
        perm = await sys_db.permission(
            username="johndoe@gmail.com",
            database="test",
            collection="students"
        )

        # Update user permission for "test" database.
        await sys_db.update_permission(
            username="johndoe@gmail.com",
            permission="rw",
            database="test"
        )

        # Update user permission for "students" collection in "test" database.
        await sys_db.update_permission(
            username="johndoe@gmail.com",
            permission="ro",
            database="test",
            collection="students"
        )

        # Reset user permission for "test" database.
        await sys_db.reset_permission(
            username="johndoe@gmail.com",
            database="test"
        )

        # Reset user permission for "students" collection in "test" database.
        await sys_db.reset_permission(
            username="johndoe@gmail.com",
            database="test",
            collection="students"
        )

See :class:`arangoasync.database.StandardDatabase` for API specification.
