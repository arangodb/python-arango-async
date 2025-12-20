__all__ = ["Cluster"]

from typing import List, Optional, cast

from arangoasync.exceptions import (
    ClusterEndpointsError,
    ClusterHealthError,
    ClusterMaintenanceModeError,
    ClusterRebalanceError,
    ClusterServerIDError,
    ClusterServerRoleError,
    ClusterStatisticsError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons, Params


class Cluster:
    """Cluster-specific endpoints."""

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    async def health(self) -> Result[Json]:
        """Queries the health of the cluster.

        Returns:
            dict: Health status of the cluster.

        Raises:
            ClusterHealthError: If retrieval fails.

        References:
            - `get-the-cluster-health <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-cluster-health>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_admin/cluster/health",
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterHealthError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return Response.format_body(result)

        return await self._executor.execute(request, response_handler)

    async def statistics(self, db_server: str) -> Result[Json]:
        """Queries the statistics of the given DB-Server.

        Args:
            db_server (str): The ID of the DB-Server.

        Returns:
            dict: Statistics of the DB-Server.

        Raises:
            ClusterStatisticsError: If retrieval fails.

        References:
            - `get-the-statistics-of-a-db-server <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-statistics-of-a-db-server>`__
        """  # noqa: E501
        params: Params = {"DBserver": db_server}

        request = Request(
            method=Method.GET,
            endpoint="/_admin/cluster/statistics",
            prefix_needed=False,
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterStatisticsError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return Response.format_body(result)

        return await self._executor.execute(request, response_handler)

    async def endpoints(self) -> Result[List[str]]:
        """Fetch all coordinator endpoints.

        Returns:
            list: List of coordinator endpoints.

        Raises:
            ClusterEndpointsError: If retrieval fails.

        References:
           - `list-all-coordinator-endpoints <https://docs.arango.ai/stable/develop/http-api/cluster/#list-all-coordinator-endpoints>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/cluster/endpoints",
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> List[str]:
            if not resp.is_success:
                raise ClusterEndpointsError(resp, request)
            body: Json = self.deserializer.loads(resp.raw_body)
            return [item["endpoint"] for item in body["endpoints"]]

        return await self._executor.execute(request, response_handler)

    async def server_id(self) -> Result[str]:
        """Get the ID of the current server.

        Returns:
            str: Server ID.

        Raises:
            ClusterServerIDError: If retrieval fails.

        References:
            - `get-the-server-id <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-server-id>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_admin/server/id",
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise ClusterServerIDError(resp, request)
            return str(self.deserializer.loads(resp.raw_body)["id"])

        return await self._executor.execute(request, response_handler)

    async def server_role(self) -> Result[str]:
        """Get the role of the current server

        Returns:
            str: Server role. Possible values: "SINGLE", "COORDINATOR", "PRIMARY", "SECONDARY", "AGENT", "UNDEFINED".

        Raises:
            ClusterServerRoleError: If retrieval fails.

        References:
            - `get-the-server-role <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-server-role>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_admin/server/role",
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise ClusterServerRoleError(resp, request)
            return str(self.deserializer.loads(resp.raw_body)["role"])

        return await self._executor.execute(request, response_handler)

    async def toggle_maintenance_mode(self, mode: str) -> Result[Json]:
        """Enable or disable the cluster supervision (agency) maintenance mode.

        Args:
            mode (str): Maintenance mode. Allowed values are "on" or "off".

        Returns:
            dict: Result of the operation.

        Raises:
            ClusterMaintenanceModeError: If the toggle operation fails.

        References:
            - `toggle-cluster-maintenance-mode <https://docs.arango.ai/stable/develop/http-api/cluster/#toggle-cluster-maintenance-mode>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint="/_admin/cluster/maintenance",
            prefix_needed=False,
            data=f'"{mode}"',
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterMaintenanceModeError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return Response.format_body(result)

        return await self._executor.execute(request, response_handler)

    async def server_maintenance_mode(self, server_id: str) -> Result[Json]:
        """Check whether the specified DB-Server is in maintenance mode and until when.

        Args:
            server_id (str): Server ID.

        Returns:
            dict: Maintenance status for the given server.

        Raises:
            ClusterMaintenanceModeError: If retrieval fails.

        References:
           - `get-the-maintenance-status-of-a-db-server <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-maintenance-status-of-a-db-server>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_admin/cluster/maintenance/{server_id}",
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterMaintenanceModeError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return Response.format_body(result)

        return await self._executor.execute(request, response_handler)

    async def toggle_server_maintenance_mode(
        self, server_id: str, mode: str, timeout: Optional[int] = None
    ) -> None:
        """Enable or disable the maintenance mode for the given server.

        Args:
            server_id (str): Server ID.
            mode (str): Maintenance mode. Allowed values are "normal" and "maintenance".
            timeout (int | None): After how many seconds the maintenance mode shall automatically end.

        Raises:
            ClusterMaintenanceModeError: If the operation fails.

        References:
            - `set-the-maintenance-status-of-a-db-server <https://docs.arango.ai/stable/develop/http-api/cluster/#set-the-maintenance-status-of-a-db-server>`__
        """  # noqa: E501
        data: Json = {"mode": mode}
        if timeout is not None:
            data["timeout"] = timeout

        request = Request(
            method=Method.PUT,
            endpoint=f"/_admin/cluster/maintenance/{server_id}",
            prefix_needed=False,
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise ClusterMaintenanceModeError(resp, request)

        await self._executor.execute(request, response_handler)

    async def calculate_imbalance(self) -> Result[Json]:
        """Computes the current cluster imbalance and returns the result.

        Returns:
            dict: Cluster imbalance information.

        Raises:
            ClusterRebalanceError: If retrieval fails.

        References:
           - `get-the-current-cluster-imbalance <https://docs.arango.ai/stable/develop/http-api/cluster/#get-the-current-cluster-imbalance>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_admin/cluster/rebalance")

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterRebalanceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return Response.format_body(result)

        return await self._executor.execute(request, response_handler)

    async def calculate_rebalance_plan(
        self,
        databases_excluded: Optional[List[str]] = None,
        exclude_system_collections: Optional[bool] = None,
        leader_changes: Optional[bool] = None,
        maximum_number_of_moves: Optional[int] = None,
        move_followers: Optional[bool] = None,
        move_leaders: Optional[bool] = None,
        pi_factor: Optional[float] = None,
        version: int = 1,
    ) -> Result[Json]:
        """Compute a set of move shard operations to improve balance.

        Args:
            databases_excluded (list | None): List of database names to be excluded from
                the analysis.
            exclude_system_collections (bool | None): Ignore system collections in the
                rebalance plan.
            leader_changes (bool | None): Allow leader changes without moving data.
            maximum_number_of_moves (int | None): Maximum number of moves to be computed.
            move_followers (bool | None): Allow moving shard followers.
            move_leaders (bool | None): Allow moving shard leaders.
            pi_factor (float | None): A weighting factor that should remain untouched.
            version (int): Must be set to 1.

        Returns:
            dict: Cluster rebalance plan.

        Raises:
            ClusterRebalanceError: If retrieval fails.

        References:
            - `compute-a-set-of-move-shard-operations-to-improve-balance <https://docs.arango.ai/stable/develop/http-api/cluster/#compute-a-set-of-move-shard-operations-to-improve-balance>`__
        """  # noqa: E501
        data: Json = dict(version=version)
        if databases_excluded is not None:
            data["databasesExcluded"] = databases_excluded
        if exclude_system_collections is not None:
            data["excludeSystemCollections"] = exclude_system_collections
        if leader_changes is not None:
            data["leaderChanges"] = leader_changes
        if maximum_number_of_moves is not None:
            data["maximumNumberOfMoves"] = maximum_number_of_moves
        if move_followers is not None:
            data["moveFollowers"] = move_followers
        if move_leaders is not None:
            data["moveLeaders"] = move_leaders
        if pi_factor is not None:
            data["piFactor"] = pi_factor

        request = Request(
            method=Method.POST,
            endpoint="/_admin/cluster/rebalance",
            prefix_needed=False,
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterRebalanceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def rebalance(
        self,
        databases_excluded: Optional[List[str]] = None,
        exclude_system_collections: Optional[bool] = None,
        leader_changes: Optional[bool] = None,
        maximum_number_of_moves: Optional[int] = None,
        move_followers: Optional[bool] = None,
        move_leaders: Optional[bool] = None,
        pi_factor: Optional[float] = None,
        version: int = 1,
    ) -> Result[Json]:
        """Compute and execute a set of move shard operations to improve balance.

        Args:
            databases_excluded (list | None): List of database names to be excluded from
                the analysis.
            exclude_system_collections (bool | None): Ignore system collections in the
                rebalance plan.
            leader_changes (bool | None): Allow leader changes without moving data.
            maximum_number_of_moves (int | None): Maximum number of moves to be computed.
            move_followers (bool | None): Allow moving shard followers.
            move_leaders (bool | None): Allow moving shard leaders.
            pi_factor (float | None): A weighting factor that should remain untouched.
            version (int): Must be set to 1.

        Returns:
            dict: Cluster rebalance plan.

        Raises:
            ClusterRebalanceError: If retrieval fails.

        References:
            - `compute-and-execute-a-set-of-move-shard-operations-to-improve-balance <https://docs.arango.ai/stable/develop/http-api/cluster/#compute-and-execute-a-set-of-move-shard-operations-to-improve-balance>`__
        """  # noqa: E501
        data: Json = dict(version=version)
        if databases_excluded is not None:
            data["databasesExcluded"] = databases_excluded
        if exclude_system_collections is not None:
            data["excludeSystemCollections"] = exclude_system_collections
        if leader_changes is not None:
            data["leaderChanges"] = leader_changes
        if maximum_number_of_moves is not None:
            data["maximumNumberOfMoves"] = maximum_number_of_moves
        if move_followers is not None:
            data["moveFollowers"] = move_followers
        if move_leaders is not None:
            data["moveLeaders"] = move_leaders
        if pi_factor is not None:
            data["piFactor"] = pi_factor

        request = Request(
            method=Method.PUT,
            endpoint="/_admin/cluster/rebalance",
            prefix_needed=False,
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ClusterRebalanceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Json, result["result"])

        return await self._executor.execute(request, response_handler)

    async def execute_rebalance_plan(
        self,
        moves: List[Json],
        version: int = 1,
    ) -> Result[int]:
        """Execute a set of move shard operations.

        Args:
            moves (list): List of move shard operations to be executed.
            version (int): Must be set to 1.

        Returns:
            int: Indicates whether the methods have been accepted and scheduled for execution.

        Raises:
            ClusterRebalanceError: If the execution fails.

        References:
            - `execute-a-set-of-move-shard-operations <https://docs.arango.ai/stable/develop/http-api/cluster/#execute-a-set-of-move-shard-operations>`__
        """  # noqa: E501
        data: Json = dict(version=version, moves=moves)

        request = Request(
            method=Method.POST,
            endpoint="/_admin/cluster/rebalance/execute",
            data=self.serializer.dumps(data),
            prefix_needed=False,
        )

        def response_handler(resp: Response) -> int:
            if not resp.is_success:
                raise ClusterRebalanceError(resp, request)
            result: int = self.deserializer.loads(resp.raw_body)["code"]
            return result

        return await self._executor.execute(request, response_handler)
