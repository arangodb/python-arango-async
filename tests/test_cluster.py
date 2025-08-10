import pytest
from packaging import version

from arangoasync.client import ArangoClient
from arangoasync.exceptions import (
    ClusterEndpointsError,
    ClusterHealthError,
    ClusterMaintenanceModeError,
    ClusterRebalanceError,
    ClusterServerIDError,
    ClusterServerRoleError,
    ClusterStatisticsError,
)


@pytest.mark.asyncio
async def test_cluster(
    url, sys_db_name, bad_db, token, enterprise, cluster, db_version
):
    if not cluster:
        pytest.skip("Cluster API is only tested in cluster setups")
    if not enterprise or db_version < version.parse("3.12.0"):
        pytest.skip(
            "For simplicity, the cluster API is only tested in the latest versions"
        )

    # Test errors
    with pytest.raises(ClusterHealthError):
        await bad_db.cluster.health()
    with pytest.raises(ClusterStatisticsError):
        await bad_db.cluster.statistics("foo")
    with pytest.raises(ClusterEndpointsError):
        await bad_db.cluster.endpoints()
    with pytest.raises(ClusterServerIDError):
        await bad_db.cluster.server_id()
    with pytest.raises(ClusterServerRoleError):
        await bad_db.cluster.server_role()
    with pytest.raises(ClusterMaintenanceModeError):
        await bad_db.cluster.toggle_maintenance_mode("on")
    with pytest.raises(ClusterMaintenanceModeError):
        await bad_db.cluster.toggle_server_maintenance_mode("PRMR0001", "normal")
    with pytest.raises(ClusterMaintenanceModeError):
        await bad_db.cluster.server_maintenance_mode("PRMR0001")
    with pytest.raises(ClusterRebalanceError):
        await bad_db.cluster.calculate_imbalance()
    with pytest.raises(ClusterRebalanceError):
        await bad_db.cluster.rebalance()
    with pytest.raises(ClusterRebalanceError):
        await bad_db.cluster.calculate_rebalance_plan()
    with pytest.raises(ClusterRebalanceError):
        await bad_db.cluster.execute_rebalance_plan(moves=[])

    async with ArangoClient(hosts=url) as client:
        db = await client.db(
            sys_db_name, auth_method="superuser", token=token, verify=True
        )
        cluster = db.cluster

        # Cluster health
        health = await cluster.health()
        assert "Health" in health

        # DB-Server statistics
        db_server = None
        for server in health["Health"]:
            if server.startswith("PRMR"):
                db_server = server
                break
        assert db_server is not None, f"No DB server found in {health}"
        stats = await cluster.statistics(db_server)
        assert "enabled" in stats

        # Cluster endpoints
        endpoints = await cluster.endpoints()
        assert len(endpoints) > 0

        # Cluster server ID and role
        server_id = await cluster.server_id()
        assert isinstance(server_id, str)
        server_role = await cluster.server_role()
        assert isinstance(server_role, str)

        # Maintenance mode
        await cluster.toggle_maintenance_mode("on")
        await cluster.toggle_maintenance_mode("off")
        await cluster.toggle_server_maintenance_mode(
            db_server, "maintenance", timeout=30
        )
        status = await cluster.server_maintenance_mode(db_server)
        assert isinstance(status, dict)
        await cluster.toggle_server_maintenance_mode(db_server, "normal")

        # Rebalance
        result = await cluster.calculate_imbalance()
        assert isinstance(result, dict)
        result = await cluster.calculate_rebalance_plan()
        assert isinstance(result, dict)
        result = await cluster.execute_rebalance_plan(moves=[])
        assert result == 200
        result = await cluster.rebalance()
        assert isinstance(result, dict)
