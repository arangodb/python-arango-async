Foxx
----

**Foxx** is a microservice framework which lets you define custom HTTP endpoints
that extend ArangoDB's REST API. For more information, refer to `ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arango.ai

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the Foxx API wrapper.
        foxx = db.foxx

        # Define the test mount point.
        service_mount = "/test_mount"

        # List services.
        await foxx.services()

        # Create a service using a source file.
        # In this case, the server must have access to the URL.
        service = {
            "source": "/tests/static/service.zip",
            "configuration": {},
            "dependencies": {},
        }
        await foxx.create_service(
            mount=service_mount,
            service=service,
            development=True,
            setup=True,
            legacy=True
        )

        # Update (upgrade) a service.
        await db.foxx.update_service(
            mount=service_mount,
            service=service,
            teardown=True,
            setup=True,
            legacy=False
        )

        # Replace (overwrite) a service.
        await db.foxx.replace_service(
            mount=service_mount,
            service=service,
            teardown=True,
            setup=True,
            legacy=True,
            force=False
        )

        # Get service details.
        await foxx.service(service_mount)

        # Manage service configuration.
        await foxx.config(service_mount)
        await foxx.update_config(service_mount, options={})
        await foxx.replace_config(service_mount, options={})

        # Manage service dependencies.
        await foxx.dependencies(service_mount)
        await foxx.update_dependencies(service_mount, options={})
        await foxx.replace_dependencies(service_mount, options={})

        # Toggle development mode for a service.
        await foxx.enable_development(service_mount)
        await foxx.disable_development(service_mount)

        # Other miscellaneous functions.
        await foxx.readme(service_mount)
        await foxx.swagger(service_mount)
        await foxx.download(service_mount)
        await foxx.commit()
        await foxx.scripts(service_mount)
        await foxx.run_script(service_mount, "setup", {})
        await foxx.run_tests(service_mount, reporter="xunit", output_format="xml")

        # Delete a service.
        await foxx.delete_service(service_mount)

There are other ways to create, update, and replace services, such as
providing a file directly instead of a source URL. This is useful when you
want to deploy a service from a local file system without needing the
server to access the file directly. When using this method, you must provide
the appropriate content type in the headers, such as `application/zip` for ZIP files or
`multipart/form-data` for multipart uploads. The following example demonstrates how to do this:

.. code-block:: python

    import aiofiles
    import aiohttp
    import json
    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the Foxx API wrapper.
        foxx = db.foxx

        # Define the test mount points.
        mount_point = "/test_mount"

        # Create the service using multipart/form-data.
        service = aiohttp.FormData()
        service.add_field(
            "source",
            open("./tests/static/service.zip", "rb"),
            filename="service.zip",
            content_type="application/zip",
        )
        service.add_field("configuration", json.dumps({}))
        service.add_field("dependencies", json.dumps({}))
        service_info = await db.foxx.create_service(
            mount=mount_point, service=service, headers={"content-type": "multipart/form-data"}
        )

        # Replace the service using raw data.
        async with aiofiles.open("./tests/static/service.zip", mode="rb") as f:
            service = await f.read()
            service_info = await db.foxx.replace_service(
                mount=mount_point, service=service, headers={"content-type": "application/zip"}
            )

        # Delete the service.
        await db.foxx.delete_service(mount_point)

See :class:`arangoasync.foxx.Foxx` for API specification.
