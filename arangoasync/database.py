__all__ = [
    "Database",
    "StandardDatabase",
    "TransactionDatabase",
    "AsyncDatabase",
]


from typing import Any, List, Optional, Sequence, TypeVar, cast
from warnings import warn

from arangoasync.aql import AQL
from arangoasync.collection import StandardCollection
from arangoasync.connection import Connection
from arangoasync.errno import HTTP_FORBIDDEN, HTTP_NOT_FOUND
from arangoasync.exceptions import (
    AsyncJobClearError,
    AsyncJobListError,
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    DatabaseCreateError,
    DatabaseDeleteError,
    DatabaseListError,
    DatabasePropertiesError,
    JWTSecretListError,
    JWTSecretReloadError,
    PermissionGetError,
    PermissionListError,
    PermissionResetError,
    PermissionUpdateError,
    ServerStatusError,
    ServerVersionError,
    TransactionAbortError,
    TransactionCommitError,
    TransactionExecuteError,
    TransactionInitError,
    TransactionListError,
    TransactionStatusError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
)
from arangoasync.executor import (
    ApiExecutor,
    AsyncApiExecutor,
    DefaultApiExecutor,
    TransactionApiExecutor,
)
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    CollectionInfo,
    CollectionType,
    DatabaseProperties,
    Json,
    Jsons,
    KeyOptions,
    Params,
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
        return self._executor.db_name

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    @property
    def context(self) -> str:
        """Return the API execution context.

        Returns:
            str: API execution context. Possible values are "default", "transaction".
        """
        return self._executor.context

    @property
    def aql(self) -> AQL:
        """Return the AQL API wrapper.

        Returns:
            arangoasync.aql.AQL: AQL API wrapper.
        """
        return AQL(self._executor)

    async def properties(self) -> Result[DatabaseProperties]:
        """Return database properties.

        Returns:
            DatabaseProperties: Properties of the current database.

        Raises:
            DatabasePropertiesError: If retrieval fails.

        References:
            - `get-information-about-the-current-database <https://docs.arangodb.com/stable/develop/http-api/databases/#get-information-about-the-current-database>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/database/current")

        def response_handler(resp: Response) -> DatabaseProperties:
            if not resp.is_success:
                raise DatabasePropertiesError(resp, request)
            return DatabaseProperties(
                self.deserializer.loads(resp.raw_body), strip_result=True
            )

        return await self._executor.execute(request, response_handler)

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

    async def databases_accessible_to_user(self) -> Result[List[str]]:
        """Return the names of all databases accessible to the current user.

        Note:
            This method can only be executed in the **_system** database.

        Returns:
            list: Database names.

        Raises:
            DatabaseListError: If retrieval fails.

        References:
            - `list-the-accessible-databases <https://docs.arangodb.com/stable/develop/http-api/databases/#list-the-accessible-databases>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/database/user")

        def response_handler(resp: Response) -> List[str]:
            if resp.is_success:
                body = self.deserializer.loads(resp.raw_body)
                return cast(List[str], body["result"])
            raise DatabaseListError(resp, request)

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
                    "username": user["user"] if "user" in user else user["username"],
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
            col_type (CollectionType | int | str | None): Collection type.
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
            if isinstance(col_type, int):
                col_type = CollectionType.from_int(col_type)
            elif isinstance(col_type, str):
                col_type = CollectionType.from_str(col_type)
            elif not isinstance(col_type, CollectionType):
                raise ValueError("Invalid collection type")
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
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            raise CollectionDeleteError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def has_user(self, username: str) -> Result[bool]:
        """Check if a user exists.

        Args:
            username (str): Username.

        Returns:
            bool: True if the user exists, False otherwise.

        Raises:
            UserListError: If the operation fails.
        """
        request = Request(method=Method.GET, endpoint=f"/_api/user/{username}")

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND:
                return False
            raise UserListError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def user(self, username: str) -> Result[UserInfo]:
        """Fetches data about a user.

        Args:
            username (str): Username.

        Returns:
            UserInfo: User details.

        Raises:
            UserGetError: If the operation fails.

        References:
            - `get-a-user` <https://docs.arangodb.com/stable/develop/http-api/users/#get-a-user>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint=f"/_api/user/{username}")

        def response_handler(resp: Response) -> UserInfo:
            if not resp.is_success:
                raise UserGetError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return UserInfo(
                user=body["user"],
                active=cast(bool, body.get("active")),
                extra=body.get("extra"),
            )

        return await self._executor.execute(request, response_handler)

    async def users(self) -> Result[Sequence[UserInfo]]:
        """Fetches data about all users.

        Without the necessary permissions, you might only get data about the
        current user.

        Returns:
            list: User information.

        Raises:
            UserListError: If the operation fails.

        References:
            - `list-available-users <https://docs.arangodb.com/stable/develop/http-api/users/#list-available-users>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/user")

        def response_handler(resp: Response) -> Sequence[UserInfo]:
            if not resp.is_success:
                raise UserListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return [
                UserInfo(user=u["user"], active=u.get("active"), extra=u.get("extra"))
                for u in body["result"]
            ]

        return await self._executor.execute(request, response_handler)

    async def create_user(self, user: UserInfo | Json) -> Result[UserInfo]:
        """Create a new user.

        Args:
            user (UserInfo | dict): User information.

        Returns:
            UserInfo: New user details.

        Raises:
            ValueError: If the username is missing.
            UserCreateError: If the operation fails.

        Example:
            .. code-block:: python

                await db.create_user(UserInfo(user="john", password="secret"))
                await db.create_user({user="john", password="secret"})

        References:
            - `create-a-user <https://docs.arangodb.com/stable/develop/http-api/users/#create-a-user>`__
        """  # noqa: E501
        if isinstance(user, dict):
            user = UserInfo(**user)
        if not user.user:
            raise ValueError("Username is required.")

        data: Json = user.format(UserInfo.user_management_formatter)
        request = Request(
            method=Method.POST,
            endpoint="/_api/user",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> UserInfo:
            if not resp.is_success:
                raise UserCreateError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return UserInfo(
                user=body["user"],
                active=cast(bool, body.get("active")),
                extra=body.get("extra"),
            )

        return await self._executor.execute(request, response_handler)

    async def replace_user(self, user: UserInfo | Json) -> Result[UserInfo]:
        """Replace the data of an existing user.

        Args:
            user (UserInfo | dict): New user information.

        Returns:
            UserInfo: New user details.

        Raises:
            ValueError: If the username is missing.
            UserReplaceError: If the operation fails.

        References:
            - `replace-a-user <https://docs.arangodb.com/stable/develop/http-api/users/#replace-a-user>`__
        """  # noqa: E501
        if isinstance(user, dict):
            user = UserInfo(**user)
        if not user.user:
            raise ValueError("Username is required.")

        data: Json = user.format(UserInfo.user_management_formatter)
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/user/{user.user}",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> UserInfo:
            if not resp.is_success:
                raise UserReplaceError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return UserInfo(
                user=body["user"],
                active=cast(bool, body.get("active")),
                extra=body.get("extra"),
            )

        return await self._executor.execute(request, response_handler)

    async def update_user(self, user: UserInfo | Json) -> Result[UserInfo]:
        """Partially modifies the data of an existing user.

        Args:
            user (UserInfo | dict): User information.

        Returns:
            UserInfo: Updated user details.

        Raises:
            ValueError: If the username is missing.
            UserUpdateError: If the operation fails.

        References:
            - `update-a-user <https://docs.arangodb.com/stable/develop/http-api/users/#update-a-user>`__
        """  # noqa: E501
        if isinstance(user, dict):
            user = UserInfo(**user)
            if not user.user:
                raise ValueError("Username is required.")

        data: Json = user.format(UserInfo.user_management_formatter)
        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/user/{user.user}",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> UserInfo:
            if not resp.is_success:
                raise UserUpdateError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return UserInfo(
                user=body["user"],
                active=cast(bool, body.get("active")),
                extra=body.get("extra"),
            )

        return await self._executor.execute(request, response_handler)

    async def delete_user(
        self,
        username: str,
        ignore_missing: bool = False,
    ) -> Result[bool]:
        """Delete a user.

        Args:
            username (str): Username.
            ignore_missing (bool): Do not raise an exception on missing user.

        Returns:
            bool: True if the user was deleted successfully, `False` if the user was
                not found but **ignore_missing** was set to `True`.

        Raises:
            UserDeleteError: If the operation fails.

        References:
            - `remove-a-user <https://docs.arangodb.com/stable/develop/http-api/users/#remove-a-user>`__
        """  # noqa: E501
        request = Request(method=Method.DELETE, endpoint=f"/_api/user/{username}")

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            raise UserDeleteError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def permissions(self, username: str, full: bool = True) -> Result[Json]:
        """Return user permissions for all databases and collections.

        Args:
            username (str): Username.
            full (bool): If `True`, the result will contain the permissions for the
                databases as well as the permissions for the collections.

        Returns:
            dict: User permissions for all databases and (optionally) collections.

        Raises:
            PermissionListError: If the operation fails.

        References:
            - `list-a-users-accessible-databases <https://docs.arangodb.com/stable/develop/http-api/users/#list-a-users-accessible-databases>`__
        """  # noqa: 501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/user/{username}/database",
            params={"full": full},
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                result: Json = self.deserializer.loads(resp.raw_body)["result"]
                return result
            raise PermissionListError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def permission(
        self,
        username: str,
        database: str,
        collection: Optional[str] = None,
    ) -> Result[str]:
        """Return user permission for a specific database or collection.

        Args:
            username (str): Username.
            database (str): Database name.
            collection (str | None): Collection name.

        Returns:
            str: User access level.

        Raises:
            PermissionGetError: If the operation fails.

        References:
            - `get-a-users-database-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#get-a-users-database-access-level>`__
            - `get-a-users-collection-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#get-a-users-collection-access-level>`__
        """  # noqa: 501
        endpoint = f"/_api/user/{username}/database/{database}"
        if collection is not None:
            endpoint += f"/{collection}"
        request = Request(method=Method.GET, endpoint=endpoint)

        def response_handler(resp: Response) -> str:
            if resp.is_success:
                return cast(str, self.deserializer.loads(resp.raw_body)["result"])
            raise PermissionGetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def update_permission(
        self,
        username: str,
        permission: str,
        database: str,
        collection: Optional[str] = None,
        ignore_failure: bool = False,
    ) -> Result[bool]:
        """Update user permissions for a specific database or collection.

        Args:
            username (str): Username.
            permission (str): Allowed values are "rw" (administrate),
                "ro" (access) and "none" (no access).
            database (str): Database to set the access level for.
            collection (str | None): Collection to set the access level for.
            ignore_failure (bool): Do not raise an exception on failure.

        Returns:
            bool: `True` if the operation was successful.

        Raises:
            PermissionUpdateError: If the operation fails and `ignore_failure`
                is `False`.

        References:
            - `set-a-users-database-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#set-a-users-database-access-level>`__
            - `set-a-users-collection-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#set-a-users-collection-access-level>`__
        """  # noqa: E501
        endpoint = f"/_api/user/{username}/database/{database}"
        if collection is not None:
            endpoint += f"/{collection}"

        request = Request(
            method=Method.PUT,
            endpoint=endpoint,
            data=self.serializer.dumps({"grant": permission}),
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if ignore_failure:
                return False
            raise PermissionUpdateError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def reset_permission(
        self,
        username: str,
        database: str,
        collection: Optional[str] = None,
        ignore_failure: bool = False,
    ) -> Result[bool]:
        """Reset user permission for a specific database or collection.

        Args:
            username (str): Username.
            database (str): Database to reset the access level for.
            collection (str | None): Collection to reset the access level for.
            ignore_failure (bool): Do not raise an exception on failure.

        Returns:
            bool: `True` if the operation was successful.

        Raises:
            PermissionResetError: If the operation fails and `ignore_failure`
                is `False`.

        References:
            - `clear-a-users-database-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#clear-a-users-database-access-level>`__
            - `clear-a-users-collection-access-level <https://docs.arangodb.com/stable/develop/http-api/users/#clear-a-users-collection-access-level>`__
        """  # noqa: E501
        endpoint = f"/_api/user/{username}/database/{database}"
        if collection is not None:
            endpoint += f"/{collection}"

        request = Request(
            method=Method.DELETE,
            endpoint=endpoint,
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if ignore_failure:
                return False
            raise PermissionResetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def jwt_secrets(self) -> Result[Json]:
        """Return information on currently loaded JWT secrets.

        Returns:
            dict: JWT secrets.

        Raises:
            JWTSecretListError: If the operation fails.

        References:
            - `get-information-about-the-loaded-jwt-secrets <https://docs.arangodb.com/stable/develop/http-api/authentication/#get-information-about-the-loaded-jwt-secrets>`__
        """  # noqa: 501
        request = Request(method=Method.GET, endpoint="/_admin/server/jwt")

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise JWTSecretListError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def reload_jwt_secrets(self) -> Result[Json]:
        """Hot_reload JWT secrets from disk.

        Returns:
            dict: Information on reloaded JWT secrets.

        Raises:
            JWTSecretReloadError: If the operation fails.

        References:
            - `hot-reload-the-jwt-secrets-from-disk <https://docs.arangodb.com/stable/develop/http-api/authentication/#hot-reload-the-jwt-secrets-from-disk>`__
        """  # noqa: 501
        request = Request(method=Method.POST, endpoint="/_admin/server/jwt")

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise JWTSecretReloadError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def list_transactions(self) -> Result[Jsons]:
        """List all currently running stream transactions.

        Returns:
            list: List of transactions, with each transaction containing
                an "id" and a "state" field.

        Raises:
            TransactionListError: If the operation fails on the server side.
        """
        request = Request(method=Method.GET, endpoint="/_api/transaction")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise TransactionListError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return cast(Jsons, result["transactions"])

        return await self._executor.execute(request, response_handler)

    async def execute_transaction(
        self,
        command: str,
        params: Optional[Json] = None,
        read: Optional[str | Sequence[str]] = None,
        write: Optional[str | Sequence[str]] = None,
        exclusive: Optional[str | Sequence[str]] = None,
        allow_implicit: Optional[bool] = None,
        wait_for_sync: Optional[bool] = None,
        lock_timeout: Optional[int] = None,
        max_transaction_size: Optional[int] = None,
    ) -> Result[Any]:
        """Execute a JavaScript Transaction.

        Warning:
            JavaScript Transactions are deprecated from ArangoDB v3.12.0 onward and
            will be removed in a future version.

        Args:
            command (str): The actual transaction operations to be executed, in the
                form of stringified JavaScript code.
            params (dict): Optional parameters passed into the JavaScript command.
            read (str | list | None): Name(s) of collections read during transaction.
            write (str | list | None): Name(s) of collections written to during
                transaction with shared access.
            exclusive (str | list | None): Name(s) of collections written to during
                transaction with exclusive access.
            allow_implicit (bool | None): Allow reading from undeclared collections.
            wait_for_sync (bool | None): If `True`, will force the transaction to write
                all data to disk before returning.
            lock_timeout (int | None): Timeout for waiting on collection locks. Setting
                it to 0 will prevent ArangoDB from timing out while waiting for a lock.
            max_transaction_size (int | None): Transaction size limit in bytes.

        Returns:
            Any: Result of the transaction.

        Raises:
            TransactionExecuteError: If the operation fails on the server side.

        References:
            - `execute-a-javascript-transaction <https://docs.arangodb.com/stable/develop/http-api/transactions/javascript-transactions/#execute-a-javascript-transaction>`__
        """  # noqa: 501
        m = "JavaScript Transactions are deprecated from ArangoDB v3.12.0 onward and will be removed in a future version."  # noqa: E501
        warn(m, DeprecationWarning, stacklevel=2)

        collections = dict()
        if read is not None:
            collections["read"] = read
        if write is not None:
            collections["write"] = write
        if exclusive is not None:
            collections["exclusive"] = exclusive

        data: Json = dict(collections=collections, action=command)
        if params is not None:
            data["params"] = params
        if wait_for_sync is not None:
            data["waitForSync"] = wait_for_sync
        if allow_implicit is not None:
            data["allowImplicit"] = allow_implicit
        if lock_timeout is not None:
            data["lockTimeout"] = lock_timeout
        if max_transaction_size is not None:
            data["maxTransactionSize"] = max_transaction_size

        request = Request(
            method=Method.POST,
            endpoint="/_api/transaction",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> Any:
            if not resp.is_success:
                raise TransactionExecuteError(resp, request)
            return self.deserializer.loads(resp.raw_body)["result"]

        return await self._executor.execute(request, response_handler)

    async def version(self, details: bool = False) -> Result[Json]:
        """Return the server version information.

        Args:
            details (bool): If `True`, return detailed version information.

        Returns:
            dict: Server version information.

        Raises:
            ServerVersionError: If the operation fails on the server side.

        References:
            - `get-the-server-version <https://docs.arangodb.com/stable/develop/http-api/administration/#get-the-server-version>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET, endpoint="/_api/version", params={"details": details}
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise ServerVersionError(resp, request)
            return self.deserializer.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)


class StandardDatabase(Database):
    """Standard database API wrapper.

    Args:
        connection (Connection): Connection object to be used by the API executor.
    """

    def __init__(self, connection: Connection) -> None:
        super().__init__(DefaultApiExecutor(connection))

    def __repr__(self) -> str:
        return f"<StandardDatabase {self.name}>"

    async def begin_transaction(
        self,
        read: Optional[str | Sequence[str]] = None,
        write: Optional[str | Sequence[str]] = None,
        exclusive: Optional[str | Sequence[str]] = None,
        wait_for_sync: Optional[bool] = None,
        allow_implicit: Optional[bool] = None,
        lock_timeout: Optional[int] = None,
        max_transaction_size: Optional[int] = None,
        allow_dirty_read: Optional[bool] = None,
        skip_fast_lock_round: Optional[bool] = None,
    ) -> "TransactionDatabase":
        """Begin a Stream Transaction.

        Args:
            read (str | list | None): Name(s) of collections read during transaction.
                Read-only collections are added lazily but should be declared if
                possible to avoid deadlocks.
            write (str | list | None): Name(s) of collections written to during
                transaction with shared access.
            exclusive (str | list | None): Name(s) of collections written to during
                transaction with exclusive access.
            wait_for_sync (bool | None): If `True`, will force the transaction to write
                all data to disk before returning
            allow_implicit (bool | None): Allow reading from undeclared collections.
            lock_timeout (int | None): Timeout for waiting on collection locks. Setting
                it to 0 will prevent ArangoDB from timing out while waiting for a lock.
            max_transaction_size (int | None): Transaction size limit in bytes.
            allow_dirty_read (bool | None): If `True`, allows the Coordinator to ask any
                shard replica for the data, not only the shard leader. This may result
                in “dirty reads”. This setting decides about dirty reads for the entire
                transaction. Individual read operations, that are performed as part of
                the transaction, cannot override it.
            skip_fast_lock_round (bool | None): Whether to disable fast locking for
                write operations.

        Returns:
            TransactionDatabase: Database API wrapper specifically tailored for
                transactions.

        Raises:
            TransactionInitError: If the operation fails on the server side.

        References:
            - `begin-a-stream-transaction <https://docs.arangodb.com/stable/develop/http-api/transactions/stream-transactions/#begin-a-stream-transaction>`__
        """  # noqa: E501
        collections = dict()
        if read is not None:
            collections["read"] = read
        if write is not None:
            collections["write"] = write
        if exclusive is not None:
            collections["exclusive"] = exclusive

        data: Json = dict(collections=collections)
        if wait_for_sync is not None:
            data["waitForSync"] = wait_for_sync
        if allow_implicit is not None:
            data["allowImplicit"] = allow_implicit
        if lock_timeout is not None:
            data["lockTimeout"] = lock_timeout
        if max_transaction_size is not None:
            data["maxTransactionSize"] = max_transaction_size
        if skip_fast_lock_round is not None:
            data["skipFastLockRound"] = skip_fast_lock_round

        headers = dict()
        if allow_dirty_read is not None:
            headers["x-arango-allow-dirty-read"] = str(allow_dirty_read).lower()

        request = Request(
            method=Method.POST,
            endpoint="/_api/transaction/begin",
            data=self.serializer.dumps(data),
            headers=headers,
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise TransactionInitError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)["result"]
            return cast(str, result["id"])

        transaction_id = await self._executor.execute(request, response_handler)
        return TransactionDatabase(self.connection, cast(str, transaction_id))

    def fetch_transaction(self, transaction_id: str) -> "TransactionDatabase":
        """Fetch an existing transaction.

        Args:
            transaction_id (str): Transaction ID.

        Returns:
            TransactionDatabase: Database API wrapper specifically tailored for
                transactions.
        """
        return TransactionDatabase(self.connection, transaction_id)

    def begin_async_execution(self, return_result: bool = True) -> "AsyncDatabase":
        """Begin async execution.

        Args:
            return_result (bool): If set to `True`, API executions return instances of
                `arangoasync.job.AsyncJob`, which you can be used to retrieve
                results from server once available. Otherwise, API executions
                return `None` and no results are stored on server.

        Returns:
            AsyncDatabase: Database API wrapper tailored for async execution.
        """
        return AsyncDatabase(self.connection, return_result)

    async def async_jobs(
        self, status: str, count: Optional[int] = None
    ) -> Result[List[str]]:
        """Return IDs of async jobs with given status.

        Args:
            status (str): Job status (e.g. "pending", "done").
            count (int | None): Max number of job IDs to return.

        Returns:
            list: List of job IDs.

        Raises:
            AsyncJobListError: If retrieval fails.

        References:
            - `list-async-jobs-by-status-or-get-the-status-of-specific-job <https://docs.arangodb.com/stable/develop/http-api/jobs/#list-async-jobs-by-status-or-get-the-status-of-specific-job>`__
        """  # noqa: E501
        params: Params = {}
        if count is not None:
            params["count"] = count

        request = Request(
            method=Method.GET, endpoint=f"/_api/job/{status}", params=params
        )

        def response_handler(resp: Response) -> List[str]:
            if resp.is_success:
                return cast(List[str], self.deserializer.loads(resp.raw_body))
            raise AsyncJobListError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def clear_async_jobs(self, threshold: Optional[float] = None) -> None:
        """Clear async job results from the server.

        Async jobs that are still queued or running are not stopped.
        Clients can use this method to perform an eventual garbage
        collection of job results.

        Args:
            threshold (float | None): If specified, only the job results created
                prior to the threshold (a Unix timestamp) are deleted. Otherwise,
                all job results are deleted.

        Raises:
            AsyncJobClearError: If the operation fails.

        References:
            - `delete-async-job-results <https://docs.arangodb.com/stable/develop/http-api/jobs/#delete-async-job-results>`__
        """  # noqa: E501
        if threshold is None:
            request = Request(method=Method.DELETE, endpoint="/_api/job/all")
        else:
            request = Request(
                method=Method.DELETE,
                endpoint="/_api/job/expired",
                params={"stamp": threshold},
            )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise AsyncJobClearError(resp, request)

        await self._executor.execute(request, response_handler)


class TransactionDatabase(Database):
    """Database API tailored specifically for
    `Stream Transactions <https://docs.arangodb.com/stable/develop/http-api/transactions/stream-transactions/>`__.

    It allows you start a transaction, run multiple operations (eg. AQL queries) over a short period of time,
    and then commit or abort the transaction.

    See :func:`arangoasync.database.StandardDatabase.begin_transaction`.

    Args:
        connection (Connection): Connection object to be used by the API executor.
        transaction_id (str): Transaction ID.
    """  # noqa: E501

    def __init__(self, connection: Connection, transaction_id: str) -> None:
        super().__init__(TransactionApiExecutor(connection, transaction_id))
        self._standard_executor = DefaultApiExecutor(connection)
        self._transaction_id = transaction_id

    def __repr__(self) -> str:
        return f"<TransactionDatabase {self.name}>"

    @property
    def transaction_id(self) -> str:
        """Transaction ID."""
        return self._transaction_id

    async def transaction_status(self) -> str:
        """Get the status of the transaction.

        Returns:
            str: Transaction status: one of "running", "committed" or "aborted".

        Raises:
            TransactionStatusError: If the transaction is not found.

        References:
            - `get-the-status-of-a-stream-transaction <https://docs.arangodb.com/stable/develop/http-api/transactions/stream-transactions/#get-the-status-of-a-stream-transaction>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/transaction/{self.transaction_id}",
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise TransactionStatusError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)["result"]
            return cast(str, result["status"])

        return await self._standard_executor.execute(request, response_handler)

    async def commit_transaction(self) -> None:
        """Commit the transaction.

        Raises:
            TransactionCommitError: If the operation fails on the server side.

        References:
            - `commit-a-stream-transaction <https://docs.arangodb.com/stable/develop/http-api/transactions/stream-transactions/#commit-a-stream-transaction>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/transaction/{self.transaction_id}",
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise TransactionCommitError(resp, request)

        await self._standard_executor.execute(request, response_handler)

    async def abort_transaction(self) -> None:
        """Abort the transaction.

        Raises:
            TransactionAbortError: If the operation fails on the server side.

        References:
            - `abort-a-stream-transaction <https://docs.arangodb.com/stable/develop/http-api/transactions/stream-transactions/#abort-a-stream-transaction>`__
        """  # noqa: E501
        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/transaction/{self.transaction_id}",
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise TransactionAbortError(resp, request)

        await self._standard_executor.execute(request, response_handler)


class AsyncDatabase(Database):
    """Database API wrapper tailored specifically for async execution.

    See :func:`arangoasync.database.StandardDatabase.begin_async_execution`.

    Args:
        connection (Connection): HTTP connection.
        return_result (bool): If set to `True`, API executions return instances of
            :class:`arangoasync.job.AsyncJob`, which you can use to retrieve results
            from server once available. If set to `False`, API executions return `None`
            and no results are stored on server.

    References:
        - `jobs <https://docs.arangodb.com/stable/develop/http-api/jobs/>`__
    """  # noqa: E501

    def __init__(self, connection: Connection, return_result: bool) -> None:
        super().__init__(executor=AsyncApiExecutor(connection, return_result))

    def __repr__(self) -> str:
        return f"<AsyncDatabase {self.name}>"
