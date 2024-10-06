__all__ = [
    "Database",
    "StandardDatabase",
]


from typing import List, Optional, Sequence, TypeVar, cast

from arangoasync.collection import StandardCollection
from arangoasync.connection import Connection
from arangoasync.errno import HTTP_FORBIDDEN, HTTP_NOT_FOUND
from arangoasync.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    DatabaseCreateError,
    DatabaseDeleteError,
    DatabaseListError,
    ServerStatusError,
)
from arangoasync.executor import ApiExecutor, DefaultApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    CollectionInfo,
    CollectionType,
    Json,
    Jsons,
    KeyOptions,
    Params,
    Result,
    ServerStatusInformation,
    UserInfo,
)

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class Database:
    """Database API.

    Args:
        executor: API executor.
            Responsible for executing requests and handling responses.
    """

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    @property
    def connection(self) -> Connection:
        """Return the HTTP connection."""
        return self._executor.connection

    @property
    def name(self) -> str:
        """Return the name of the current database."""
        return self.connection.db_name

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    async def status(self) -> Result[ServerStatusInformation]:
        """Query the server status.

        Returns:
            ServerStatusInformation: Server status.

        Raises:
            ServerSatusError: If retrieval fails.

        References:
            - `get-server-status-information <https://docs.arangodb.com/stable/develop/http-api/administration/#get-server-status-information>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_admin/status")

        def response_handler(resp: Response) -> ServerStatusInformation:
            if not resp.is_success:
                raise ServerStatusError(resp, request)
            return ServerStatusInformation(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def databases(self) -> Result[List[str]]:
        """Return the names of all databases.

        Note:
            This method can only be executed in the **_system** database.

        Returns:
            list: Database names.

        Raises:
            DatabaseListError: If retrieval fails.

        References:
            - `list-all-databases <https://docs.arangodb.com/stable/develop/http-api/databases/#list-all-databases>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/database")

        def response_handler(resp: Response) -> List[str]:
            if resp.is_success:
                body = self.deserializer.loads(resp.raw_body)
                return cast(List[str], body["result"])
            msg: Optional[str] = None
            if resp.status_code == HTTP_FORBIDDEN:
                msg = "This request can only be executed in the _system database."
            raise DatabaseListError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def has_database(self, name: str) -> Result[bool]:
        """Check if a database exists.

        Note:
            This method can only be executed from within the **_system** database.

        Args:
            name (str): Database name.

        Returns:
            bool: `True` if the database exists, `False` otherwise.

        Raises:
            DatabaseListError: If retrieval fails.
        """
        request = Request(method=Method.GET, endpoint="/_api/database")

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                body = self.deserializer.loads(resp.raw_body)
                return name in body["result"]
            msg: Optional[str] = None
            if resp.status_code == HTTP_FORBIDDEN:
                msg = "This request can only be executed in the _system database."
            raise DatabaseListError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def create_database(
        self,
        name: str,
        users: Optional[Sequence[Json | UserInfo]] = None,
        replication_factor: Optional[int | str] = None,
        write_concern: Optional[int] = None,
        sharding: Optional[bool] = None,
    ) -> Result[bool]:
        """Create a new database.

        Note:
            This method can only be executed from within the **_system** database.

        Args:
            name (str): Database name.
            users (list | None): Optional list of users with access to the new
                database, where each user is of :class:`User
                <arangoasync.wrapper.UserInfo>` type, or a dictionary with fields
                "username", "password" and "active". If not set, the default user
                **root** will be used to ensure that the new database will be
                accessible after it is created.
            replication_factor (int | str | None): Default replication factor for new
                collections created in this database. Special values include
                “satellite”, which will replicate the collection to every DB-Server
                (Enterprise Edition only), and 1, which disables replication. Used
                for clusters only.
            write_concern (int | None): Default write concern for collections created
                in this database. Determines how many copies of each shard are required
                to be in sync on different DB-Servers. If there are less than these many
                copies in the cluster a shard will refuse to write. Writes to shards with
                enough up-to-date copies will succeed at the same time, however. Value of
                this parameter can not be larger than the value of **replication_factor**.
                Used for clusters only.
            sharding (str | None): Sharding method used for new collections in this
                database. Allowed values are: "", "flexible" and "single". The first
                two are equivalent. Used for clusters only.

        Returns:
            bool: True if the database was created successfully.

        Raises:
            DatabaseCreateError: If creation fails.

        References:
            - `create-a-database <https://docs.arangodb.com/stable/develop/http-api/databases/#create-a-database>`__
        """  # noqa: E501
        data: Json = {"name": name}

        options: Json = {}
        if replication_factor is not None:
            options["replicationFactor"] = replication_factor
        if write_concern is not None:
            options["writeConcern"] = write_concern
        if sharding is not None:
            options["sharding"] = sharding
        if options:
            data["options"] = options

        if users is not None:
            data["users"] = [
                {
                    "username": user["username"],
                    "passwd": user["password"],
                    "active": user.get("active", True),
                    "extra": user.get("extra", {}),
                }
                for user in users
            ]

        request = Request(
            method=Method.POST,
            endpoint="/_api/database",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            msg: Optional[str] = None
            if resp.status_code == HTTP_FORBIDDEN:
                msg = "This request can only be executed in the _system database."
            raise DatabaseCreateError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def delete_database(
        self, name: str, ignore_missing: bool = False
    ) -> Result[bool]:
        """Delete a database.

        Note:
            This method can only be executed from within the **_system** database.

        Args:
            name (str): Database name.
            ignore_missing (bool): Do not raise an exception on missing database.

        Returns:
            bool: True if the database was deleted successfully, `False` if the
                database was not found but **ignore_missing** was set to `True`.

        Raises:
            DatabaseDeleteError: If deletion fails.

        References:
            - `drop-a-database <https://docs.arangodb.com/stable/develop/http-api/databases/#drop-a-database>`__
        """  # noqa: E501
        request = Request(method=Method.DELETE, endpoint=f"/_api/database/{name}")

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            msg: Optional[str] = None
            if resp.status_code == HTTP_FORBIDDEN:
                msg = "This request can only be executed in the _system database."
            raise DatabaseDeleteError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    def collection(
        self,
        name: str,
        doc_serializer: Optional[Serializer[T]] = None,
        doc_deserializer: Optional[Deserializer[U, V]] = None,
    ) -> StandardCollection[T, U, V]:
        """Return the collection API wrapper.

        Args:
            name (str): Collection name.
            doc_serializer (Serializer): Custom document serializer.
                This will be used only for document operations.
            doc_deserializer (Deserializer): Custom document deserializer.
                This will be used only for document operations.

        Returns:
            StandardCollection: Collection API wrapper.
        """
        if doc_serializer is None:
            serializer = cast(Serializer[T], self.serializer)
        else:
            serializer = doc_serializer
        if doc_deserializer is None:
            deserializer = cast(Deserializer[U, V], self.deserializer)
        else:
            deserializer = doc_deserializer

        return StandardCollection[T, U, V](
            self._executor, name, serializer, deserializer
        )

    async def collections(
        self,
        exclude_system: Optional[bool] = None,
    ) -> Result[List[CollectionInfo]]:
        """Returns basic information for all collections in the current database,
        optionally excluding system collections.

        Returns:
            list: Collection names.

        Raises:
            CollectionListError: If retrieval fails.

        References:
           - `list-all-collections <https://docs.arangodb.com/stable/develop/http-api/collections/#list-all-collections>`__
        """  # noqa: E501
        params: Params = {}
        if exclude_system is not None:
            params["excludeSystem"] = exclude_system

        request = Request(
            method=Method.GET,
            endpoint="/_api/collection",
            params=params,
        )

        def response_handler(resp: Response) -> List[CollectionInfo]:
            if not resp.is_success:
                raise CollectionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return [CollectionInfo(c) for c in body["result"]]

        return await self._executor.execute(request, response_handler)

    async def has_collection(self, name: str) -> Result[bool]:
        """Check if a collection exists in the database.

        Args:
            name (str): Collection name.

        Returns:
            bool: True if the collection exists, False otherwise.

        Raises:
            CollectionListError: If retrieval fails.
        """
        request = Request(method=Method.GET, endpoint=f"/_api/collection/{name}")

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND:
                return False
            raise CollectionListError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def create_collection(
        self,
        name: str,
        doc_serializer: Optional[Serializer[T]] = None,
        doc_deserializer: Optional[Deserializer[U, V]] = None,
        col_type: Optional[CollectionType] = None,
        write_concern: Optional[int] = None,
        wait_for_sync: Optional[bool] = None,
        number_of_shards: Optional[int] = None,
        replication_factor: Optional[int] = None,
        cache_enabled: Optional[bool] = None,
        computed_values: Optional[Jsons] = None,
        distribute_shards_like: Optional[str] = None,
        is_system: Optional[bool] = False,
        key_options: Optional[KeyOptions | Json] = None,
        schema: Optional[Json] = None,
        shard_keys: Optional[Sequence[str]] = None,
        sharding_strategy: Optional[str] = None,
        smart_graph_attribute: Optional[str] = None,
        smart_join_attribute: Optional[str] = None,
        wait_for_sync_replication: Optional[bool] = None,
        enforce_replication_factor: Optional[bool] = None,
    ) -> Result[StandardCollection[T, U, V]]:
        """Create a new collection.

        Args:
            name (str): Collection name.
            doc_serializer (Serializer): Custom document serializer.
                This will be used only for document operations.
            doc_deserializer (Deserializer): Custom document deserializer.
                This will be used only for document operations.
            col_type (CollectionType | None): Collection type.
            write_concern (int | None): Determines how many copies of each shard are
                required to be in sync on the different DB-Servers.
            wait_for_sync (bool | None): If `True`, the data is synchronised to disk
                before returning from a document create, update, replace or removal
                operation.
            number_of_shards (int | None): In a cluster, this value determines the
                number of shards to create for the collection.
            replication_factor (int | None): In a cluster, this attribute determines
                how many copies of each shard are kept on different DB-Servers.
            cache_enabled (bool | None): Whether the in-memory hash cache for
                documents should be enabled for this collection.
            computed_values (Jsons | None): An optional list of objects, each
                representing a computed value.
            distribute_shards_like (str | None): The name of another collection.
                If this property is set in a cluster, the collection copies the
                replicationFactor, numberOfShards and shardingStrategy properties
                from the specified collection (referred to as the prototype
                collection) and distributes the shards of this collection in the same
                way as the shards of the other collection.
            is_system (bool | None): If `True`, create a system collection.
                In this case, the collection name should start with an underscore.
            key_options (KeyOptions | dict | None): Additional options for key
                generation. You may use a :class:`KeyOptions
                <arangoasync.wrapper.KeyOptions>` object for easier configuration,
                or pass a dictionary directly.
            schema (dict | None): Optional object that specifies the collection
                level schema for documents.
            shard_keys (list | None): In a cluster, this attribute determines which
                document attributes are used to determine the target shard for
                documents.
            sharding_strategy (str | None): Name of the sharding strategy.
            smart_graph_attribute: (str | None): The attribute that is used for
                sharding: vertices with the same value of this attribute are placed
                in the same shard.
            smart_join_attribute: (str | None): Determines an attribute of the
                collection that must contain the shard key value of the referred-to
                SmartJoin collection.
            wait_for_sync_replication (bool | None): If `True`, the server only
                reports success back to the client when all replicas have created
                the collection. Set it to `False` if you want faster server
                responses and don’t care about full replication.
            enforce_replication_factor (bool | None): If `True`, the server checks
                if there are enough replicas available at creation time and bail out
                otherwise. Set it to `False` to disable this extra check.

        Returns:
            StandardCollection: Collection API wrapper.

        Raises:
            ValueError: If parameters are invalid.
            CollectionCreateError: If the operation fails.

        References:
            - `create-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#create-a-collection>`__
        """  # noqa: E501
        data: Json = {"name": name}
        if col_type is not None:
            data["type"] = col_type.value
        if write_concern is not None:
            data["writeConcern"] = write_concern
        if wait_for_sync is not None:
            data["waitForSync"] = wait_for_sync
        if number_of_shards is not None:
            data["numberOfShards"] = number_of_shards
        if replication_factor is not None:
            data["replicationFactor"] = replication_factor
        if cache_enabled is not None:
            data["cacheEnabled"] = cache_enabled
        if computed_values is not None:
            data["computedValues"] = computed_values
        if distribute_shards_like is not None:
            data["distributeShardsLike"] = distribute_shards_like
        if is_system is not None:
            data["isSystem"] = is_system
        if key_options is not None:
            if isinstance(key_options, dict):
                key_options = KeyOptions(data=key_options)
            key_options.validate()
            data["keyOptions"] = key_options.to_dict()
        if schema is not None:
            data["schema"] = schema
        if shard_keys is not None:
            data["shardKeys"] = shard_keys
        if sharding_strategy is not None:
            data["shardingStrategy"] = sharding_strategy
        if smart_graph_attribute is not None:
            data["smartGraphAttribute"] = smart_graph_attribute
        if smart_join_attribute is not None:
            data["smartJoinAttribute"] = smart_join_attribute

        params: Params = {}
        if wait_for_sync_replication is not None:
            params["waitForSyncReplication"] = wait_for_sync_replication
        if enforce_replication_factor is not None:
            params["enforceReplicationFactor"] = enforce_replication_factor

        request = Request(
            method=Method.POST,
            endpoint="/_api/collection",
            data=self.serializer.dumps(data),
            params=params,
        )

        def response_handler(resp: Response) -> StandardCollection[T, U, V]:
            nonlocal doc_serializer, doc_deserializer
            if not resp.is_success:
                raise CollectionCreateError(resp, request)
            if doc_serializer is None:
                serializer = cast(Serializer[T], self.serializer)
            else:
                serializer = doc_serializer
            if doc_deserializer is None:
                deserializer = cast(Deserializer[U, V], self.deserializer)
            else:
                deserializer = doc_deserializer
            return StandardCollection[T, U, V](
                self._executor, name, serializer, deserializer
            )

        return await self._executor.execute(request, response_handler)

    async def delete_collection(
        self,
        name: str,
        ignore_missing: bool = False,
        is_system: Optional[bool] = None,
    ) -> Result[bool]:
        """Delete a collection.

        Args:
            name (str): Collection name.
            ignore_missing (bool): Do not raise an exception on missing collection.
            is_system (bool | None): Whether to drop a system collection. This parameter
                must be set to `True` in order to drop a system collection.

        Returns:
            bool: True if the collection was deleted successfully, `False` if the
                collection was not found but **ignore_missing** was set to `True`.

        Raises:
            CollectionDeleteError: If the operation fails.

        References:
            - `drop-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#drop-a-collection>`__
        """  # noqa: E501
        params: Params = {}
        if is_system is not None:
            params["isSystem"] = is_system

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/collection/{name}",
            params=params,
        )

        def response_handler(resp: Response) -> bool:
            nonlocal ignore_missing
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            raise CollectionDeleteError(resp, request)

        return await self._executor.execute(request, response_handler)


class StandardDatabase(Database):
    """Standard database API wrapper."""

    def __init__(self, connection: Connection) -> None:
        super().__init__(DefaultApiExecutor(connection))
