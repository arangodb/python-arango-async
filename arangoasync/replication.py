__all__ = ["Replication"]


from typing import Optional

from arangoasync.exceptions import (
    ReplicationApplierConfigError,
    ReplicationApplierStateError,
    ReplicationClusterInventoryError,
    ReplicationDumpError,
    ReplicationInventoryError,
    ReplicationLoggerStateError,
    ReplicationServerIDError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons, Params


class Replication:
    """Replication API wrapper."""

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

    async def inventory(
        self,
        batch_id: str,
        include_system: Optional[bool] = None,
        all_databases: Optional[bool] = None,
        collection: Optional[bool] = None,
        db_server: Optional[str] = None,
    ) -> Result[Json]:
        """
        Return an overview of collections and indexes.

        Args:
            batch_id (str): Batch ID.
            include_system (bool | None): Include system collections.
            all_databases (bool | None): Include all databases (only on "_system").
            collection (bool | None): If this parameter is set, the
                response will be restricted to a single collection (the one specified),
                and no views will be returned.
            db_server (str | None): On a Coordinator, this request must have a
                DBserver query parameter

        Returns:
            dict: Overview of collections and indexes.

        Raises:
            ReplicationInventoryError: If retrieval fails.

        References:
            - `get-a-replication-inventory <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-dump/#get-a-replication-inventory>`__
        """  # noqa: E501
        params: Params = dict()
        params["batchId"] = batch_id
        if include_system is not None:
            params["includeSystem"] = include_system
        if all_databases is not None:
            params["global"] = all_databases
        if collection is not None:
            params["collection"] = collection
        if db_server is not None:
            params["DBServer"] = db_server

        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/inventory",
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ReplicationInventoryError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def dump(
        self,
        collection: str,
        batch_id: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> Result[bytes]:
        """Return the events data of one collection.

        Args:
            collection (str): ID of the collection to dump.
            batch_id (str | None): Batch ID.
            chunk_size (int | None): Size of the result in bytes. This value is honored
                approximately only.

        Returns:
            bytes: Collection events data.

        Raises:
            ReplicationDumpError: If retrieval fails.

        References:
            - `get-a-replication-dump <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-dump/#get-a-replication-dump>`__
        """  # noqa: E501
        params: Params = dict()
        params["collection"] = collection
        if batch_id is not None:
            params["batchId"] = batch_id
        if chunk_size is not None:
            params["chunkSize"] = chunk_size

        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/dump",
            params=params,
        )

        def response_handler(resp: Response) -> bytes:
            if not resp.is_success:
                raise ReplicationDumpError(resp, request)
            return resp.raw_body

        return await self._executor.execute(request, response_handler)

    async def cluster_inventory(
        self, include_system: Optional[bool] = None
    ) -> Result[Json]:
        """Return an overview of collections and indexes in a cluster.

        Args:
            include_system (bool | None): Include system collections.

        Returns:
            dict: Overview of collections and indexes in the cluster.

        Raises:
            ReplicationClusterInventoryError: If retrieval fails.

        References:
            - `get-the-cluster-collections-and-indexes <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-dump/#get-the-cluster-collections-and-indexes>`__
        """  # noqa: E501
        params: Params = {}
        if include_system is not None:
            params["includeSystem"] = include_system

        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/clusterInventory",
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ReplicationClusterInventoryError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def logger_state(self) -> Result[Json]:
        """Return the state of the replication logger.

        Returns:
            dict: Logger state.

        Raises:
            ReplicationLoggerStateError: If retrieval fails.

        References:
            - `get-the-replication-logger-state <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-logger/#get-the-replication-logger-state>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/logger-state",
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ReplicationLoggerStateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def applier_config(self) -> Result[Json]:
        """Return the configuration of the replication applier.

        Returns:
            dict: Configuration of the replication applier.

        Raises:
            ReplicationApplierConfigError: If retrieval fails.

        References:
           - `get-the-replication-applier-configuration <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-applier/#get-the-replication-applier-configuration>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/applier-config",
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ReplicationApplierConfigError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def applier_state(self) -> Result[Json]:
        """Return the state of the replication applier.

        Returns:
            dict: State of the replication applier.

        Raises:
            ReplicationApplierStateError: If retrieval fails.

        References:
            - `get-the-replication-applier-state <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/replication-applier/#get-the-replication-applier-state>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/applier-state",
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ReplicationApplierStateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def server_id(self) -> Result[str]:
        """Return the current server's ID.

        Returns:
            str: Server ID.

        Raises:
            ReplicationServerIDError: If retrieval fails.

        References:
            - `get-the-replication-server-id <https://docs.arango.ai/arangodb/stable/develop/http-api/replication/other-replication-commands/#get-the-replication-server-id>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/replication/server-id",
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise ReplicationServerIDError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return str(result["serverId"])

        return await self._executor.execute(request, response_handler)
