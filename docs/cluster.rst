Clusters
--------

The cluster-specific API lets you get information about individual
cluster nodes and the cluster as a whole, as well as monitor and
administrate cluster deployments. For more information on the design
and architecture, refer to `ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arango.ai

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "_system" database as root user.
        db = await client.db("_system", auth=auth)
        cluster = db.cluster

        # Cluster health
        health = await cluster.health()

        # DB-Server statistics
        db_server = "PRMR-2716c9d0-4b22-4c66-ba3d-f9cd3143e52b"
        stats = await cluster.statistics(db_server)

        # Cluster endpoints
        endpoints = await cluster.endpoints()

        # Cluster server ID and role
        server_id = await cluster.server_id()
        server_role = await cluster.server_role()

        # Maintenance mode
        await cluster.toggle_maintenance_mode("on")
        await cluster.toggle_maintenance_mode("off")
        await cluster.toggle_server_maintenance_mode(
            db_server, "maintenance", timeout=30
        )
        status = await cluster.server_maintenance_mode(db_server)
        await cluster.toggle_server_maintenance_mode(db_server, "normal")

        # Rebalance
        result = await cluster.calculate_imbalance()
        result = await cluster.calculate_rebalance_plan()
        result = await cluster.execute_rebalance_plan(moves=[])
        result = await cluster.rebalance()

See :class:`arangoasync.cluster.Cluster` for API specification.
