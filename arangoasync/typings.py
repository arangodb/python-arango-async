from enum import Enum
from numbers import Number
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Tuple,
    cast,
)

from multidict import CIMultiDictProxy, MultiDict

Json = Dict[str, Any]
Json.__doc__ = """Type definition for request/response body"""

Jsons = List[Json]
Jsons.__doc__ = """Type definition for a list of JSON objects"""

RequestHeaders = MutableMapping[str, str] | MultiDict[str]
RequestHeaders.__doc__ = """Type definition for request HTTP headers"""

ResponseHeaders = MutableMapping[str, str] | MultiDict[str] | CIMultiDictProxy[str]
ResponseHeaders.__doc__ = """Type definition for response HTTP headers"""

Params = MutableMapping[str, bool | int | str | float]
Params.__doc__ = """Type definition for URL (query) parameters"""

Formatter = Callable[[Json], Json]
Formatter.__doc__ = """Type definition for a JSON formatter"""


class CollectionType(Enum):
    """Collection types."""

    DOCUMENT = 2
    EDGE = 3

    @staticmethod
    def from_int(value: int) -> "CollectionType":
        """Return a collection type from its integer value.

        Args:
            value (int): Collection type integer value.

        Returns:
            CollectionType: Collection type.
        """
        if value == 2:
            return CollectionType.DOCUMENT
        elif value == 3:
            return CollectionType.EDGE
        else:
            raise ValueError(f"Invalid collection type value: {value}")

    @staticmethod
    def from_str(value: str) -> "CollectionType":
        """Return a collection type from its string value.

        Args:
            value (str): Collection type string value.

        Returns:
            CollectionType: Collection type.
        """
        value = value.lower()
        if value == "document":
            return CollectionType.DOCUMENT
        elif value == "edge":
            return CollectionType.EDGE
        else:
            raise ValueError(f"Invalid collection type value: {value}")

    def __str__(self) -> str:
        return self.name.lower()

    def __repr__(self) -> str:
        return self.name.lower()


class CollectionStatus(Enum):
    """Collection status."""

    NEW = 1
    UNLOADED = 2
    LOADED = 3
    UNLOADING = 4
    DELETED = 5
    LOADING = 6

    @staticmethod
    def from_int(value: int) -> "CollectionStatus":
        """Return a collection status from its integer value.

        Args:
            value (int): Collection status integer value.

        Returns:
            CollectionStatus: Collection status.
        """
        if value == 1:
            return CollectionStatus.NEW
        elif value == 2:
            return CollectionStatus.UNLOADED
        elif value == 3:
            return CollectionStatus.LOADED
        elif value == 4:
            return CollectionStatus.UNLOADING
        elif value == 5:
            return CollectionStatus.DELETED
        elif value == 6:
            return CollectionStatus.LOADING
        else:
            raise ValueError(f"Invalid collection status value: {value}")

    def __str__(self) -> str:
        return self.name.lower()

    def __repr__(self) -> str:
        return self.name.lower()


class JsonWrapper:
    """Wrapper over server request/response objects."""

    def __init__(self, data: Json) -> None:
        self._data = data
        for excluded in ("code", "error"):
            if excluded in self._data:
                self._data.pop(excluded)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, item: str) -> bool:
        return item in self._data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._data})"

    def __str__(self) -> str:
        return str(self._data)

    def __eq__(self, other: object) -> bool:
        return self._data == other

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Return the value for key if key is in the dictionary, else default."""
        return self._data.get(key, default)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """Return an iterator over the dictionary’s key-value pairs."""
        return iter(self._data.items())

    def to_dict(self) -> Json:
        """Return the dictionary."""
        return self._data

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        Returns the unmodified data by default. Should not modify the object in-place.
        """
        if formatter is not None:
            return formatter(self._data)
        return self._data

    @staticmethod
    def _strip_result(data: Json) -> Json:
        """Keep only the `result` key from a dict. Useful when parsing responses."""
        return data["result"]  # type: ignore[no-any-return]


class KeyOptions(JsonWrapper):
    """Additional options for key generation, used on collections.

    Args:
        allow_user_keys (bool): If set to `True`, then you are allowed to supply own
            key values in the `_key` attribute of documents. If set to `False`, then
            the key generator is solely responsible for generating keys and an error
            is raised if you supply own key values in the `_key` attribute of
            documents.
        generator_type (str): Specifies the type of the key generator. The currently
            available generators are "traditional", "autoincrement", "uuid" and
            "padded".
        increment (int | None): The increment value for the "autoincrement" key
            generator. Not allowed for other key generator types.
        offset (int | None): The initial offset value for the "autoincrement" key
            generator. Not allowed for other key generator types.
        data (dict | None): Key options. If this parameter is specified, the
            other parameters are ignored.

    Example:
        .. code-block:: json

            {
                "type": "autoincrement",
                "increment": 5,
                "allowUserKeys": true
            }

    References:
        - `create-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#create-a-collection_body_keyOptions>`__
    """  # noqa: E501

    def __init__(
        self,
        allow_user_keys: bool = True,
        generator_type: str = "traditional",
        increment: Optional[int] = None,
        offset: Optional[int] = None,
        data: Optional[Json] = None,
    ) -> None:
        if data is None:
            data = {
                "allowUserKeys": allow_user_keys,
                "type": generator_type,
            }
            if increment is not None:
                data["increment"] = increment
            if offset is not None:
                data["offset"] = offset
        super().__init__(data)

    def validate(self) -> None:
        """Validate key options."""
        if "type" not in self:
            raise ValueError('"type" value is required for key options')
        if "allowUserKeys" not in self:
            raise ValueError('"allowUserKeys" value is required for key options')

        allowed_types = {"autoincrement", "uuid", "padded", "traditional"}
        if self["type"] not in allowed_types:
            raise ValueError(
                f"Invalid key generator type '{self['type']}', "
                f"expected one of {allowed_types}"
            )

        if self.get("increment") is not None and self["type"] != "autoincrement":
            raise ValueError(
                '"increment" value is only allowed for "autoincrement" ' "key generator"
            )
        if self.get("offset") is not None and self["type"] != "autoincrement":
            raise ValueError(
                '"offset" value is only allowed for "autoincrement" ' "key generator"
            )

    @staticmethod
    def compatibility_formatter(data: Json) -> Json:
        """python-arango compatibility formatter."""
        result: Json = {}
        if "type" in data:
            result["key_generator"] = data["type"]
        if "increment" in data:
            result["key_increment"] = data["increment"]
        if "offset" in data:
            result["key_offset"] = data["offset"]
        if "allowUserKeys" in data:
            result["user_keys"] = data["allowUserKeys"]
        if "lastValue" in data:
            result["key_last_value"] = data["lastValue"]
        return result

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return self.compatibility_formatter(self._data)


class CollectionInfo(JsonWrapper):
    """Collection information.

    Example:
        .. code-block:: json

            {
                "id" : "151",
                "name" : "animals",
                "status" : 3,
                "type" : 2,
                "isSystem" : false,
                "globallyUniqueId" : "hDA74058C1843/151"
            }

    References:
        - `get-the-collection-information <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-collection-information>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def globally_unique_id(self) -> str:
        """ "A unique identifier of the collection (internal property)."""
        return cast(str, self._data["globallyUniqueId"])

    @property
    def is_system(self) -> bool:
        """Whether the collection is a system collection."""
        return cast(bool, self._data["isSystem"])

    @property
    def name(self) -> str:
        """Return the name of the collection."""
        return cast(str, self._data["name"])

    @property
    def status(self) -> CollectionStatus:
        """Return the status of the collection."""
        return CollectionStatus.from_int(self._data["status"])

    @property
    def col_type(self) -> CollectionType:
        """Return the type of the collection."""
        return CollectionType.from_int(self._data["type"])

    @staticmethod
    def compatibility_formatter(data: Json) -> Json:
        """python-arango compatibility formatter."""
        return {
            "id": data["id"],
            "name": data["name"],
            "system": data["isSystem"],
            "type": str(CollectionType.from_int(data["type"])),
            "status": str(CollectionStatus.from_int(data["status"])),
        }

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return self.compatibility_formatter(self._data)


class UserInfo(JsonWrapper):
    """User information.

    Args:
        user (str): The name of the user.
        password (str | None): The user password as a string. Note that user
            password is not returned back by the server.
        active (bool): `True` if user is active, `False` otherwise.
        extra (dict | None): Additional user information. For internal use only.
            Should not be set or modified by end users.

    Example:
        .. code-block:: json

            {
                "user": "john",
                "password": "secret",
                "active": true,
                "extra": {}
            }

    References:
        - `create-a-user <https://docs.arangodb.com/stable/develop/http-api/users/#create-a-user>`__
    """  # noqa: E501

    def __init__(
        self,
        user: str,
        password: Optional[str] = None,
        active: bool = True,
        extra: Optional[Json] = None,
    ) -> None:
        data = {"user": user, "active": active}
        if password is not None:
            data["password"] = password
        if extra is not None:
            data["extra"] = extra
        super().__init__(data)

    @property
    def user(self) -> str:
        return self._data["user"]  # type: ignore[no-any-return]

    @property
    def password(self) -> Optional[str]:
        return self._data.get("password")

    @property
    def active(self) -> bool:
        return self._data["active"]  # type: ignore[no-any-return]

    @property
    def extra(self) -> Optional[Json]:
        return self._data.get("extra")

    @staticmethod
    def user_management_formatter(data: Json) -> Json:
        """Request formatter."""
        result: Json = dict(user=data["user"])
        if "password" in data:
            result["passwd"] = data["password"]
        if "active" in data:
            result["active"] = data["active"]
        if "extra" in data:
            result["extra"] = data["extra"]
        return result

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data."""
        if formatter is not None:
            return super().format(formatter)
        return self._data


class ServerStatusInformation(JsonWrapper):
    """Status information about the server.

    Example:
        .. code-block:: json

            {
              "server" : "arango",
              "version" : "3.12.2",
              "pid" : 244,
              "license" : "enterprise",
              "mode" : "server",
              "operationMode" : "server",
              "foxxApi" : true,
              "host" : "localhost",
              "hostname" : "ebd1509c9185",
              "serverInfo" : {
                "progress" : {
                  "phase" : "in wait",
                  "feature" : "",
                  "recoveryTick" : 0
                },
                "maintenance" : false,
                "role" : "COORDINATOR",
                "writeOpsEnabled" : true,
                "readOnly" : false,
                "persistedId" : "CRDN-329cfc20-071f-4faf-9727-7e48a7aed1e5",
                "rebootId" : 1,
                "address" : "tcp://localhost:8529",
                "serverId" : "CRDN-329cfc20-071f-4faf-9727-7e48a7aed1e5",
                "state" : "SERVING"
              },
              "coordinator" : {
                "foxxmaster" : "CRDN-0ed76822-3e64-47ed-a61b-510f2a696175",
                "isFoxxmaster" : false
              },
              "agency" : {
                "agencyComm" : {
                  "endpoints" : [
                    "tcp://localhost:8551",
                    "tcp://localhost:8541",
                    "tcp://localhost:8531"
                  ]
                }
              }
            }

    References:
        - `get-server-status-information <https://docs.arangodb.com/stable/develop/http-api/administration/#get-server-status-information>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def server(self) -> Optional[str]:
        return self._data.get("server")

    @property
    def version(self) -> Optional[str]:
        return self._data.get("version")

    @property
    def pid(self) -> Optional[int]:
        return self._data.get("pid")

    @property
    def license(self) -> Optional[str]:
        return self._data.get("license")

    @property
    def mode(self) -> Optional[str]:
        return self._data.get("mode")

    @property
    def operation_mode(self) -> Optional[str]:
        return self._data.get("operationMode")

    @property
    def foxx_api(self) -> Optional[bool]:
        return self._data.get("foxxApi")

    @property
    def host(self) -> Optional[str]:
        return self._data.get("host")

    @property
    def hostname(self) -> Optional[str]:
        return self._data.get("hostname")

    @property
    def server_info(self) -> Optional[Json]:
        return self._data.get("serverInfo")

    @property
    def coordinator(self) -> Optional[Json]:
        return self._data.get("coordinator")

    @property
    def agency(self) -> Optional[Json]:
        return self._data.get("agency")


class DatabaseProperties(JsonWrapper):
    """Properties of the database.

    References:
        - `get-information-about-the-current-database <https://docs.arangodb.com/stable/develop/http-api/databases/#get-information-about-the-current-database>`__
    """  # noqa: E501

    def __init__(self, data: Json, strip_result: bool = False) -> None:
        super().__init__(self._strip_result(data) if strip_result else data)

    @property
    def name(self) -> str:
        """The name of the current database."""
        return self._data["name"]  # type: ignore[no-any-return]

    @property
    def id(self) -> str:
        """The id of the current database."""
        return self._data["id"]  # type: ignore[no-any-return]

    @property
    def path(self) -> Optional[str]:
        """The filesystem path of the current database."""
        return self._data.get("path")

    @property
    def is_system(self) -> bool:
        """Whether the database is the `_system` database."""
        return self._data["isSystem"]  # type: ignore[no-any-return]

    @property
    def sharding(self) -> Optional[str]:
        """The default sharding method for collections."""
        return self._data.get("sharding")

    @property
    def replication_factor(self) -> Optional[int]:
        """The default replication factor for collections."""
        return self._data.get("replicationFactor")

    @property
    def write_concern(self) -> Optional[int]:
        """The default write concern for collections."""
        return self._data.get("writeConcern")

    @staticmethod
    def compatibility_formatter(data: Json) -> Json:
        """python-arango compatibility formatter."""
        result: Json = {}
        if "id" in data:
            result["id"] = data["id"]
        if "name" in data:
            result["name"] = data["name"]
        if "path" in data:
            result["path"] = data["path"]
        if "system" in data:
            result["system"] = data["system"]
        if "isSystem" in data:
            result["system"] = data["isSystem"]
        if "sharding" in data:
            result["sharding"] = data["sharding"]
        if "replicationFactor" in data:
            result["replication_factor"] = data["replicationFactor"]
        if "writeConcern" in data:
            result["write_concern"] = data["writeConcern"]
        if "replicationVersion" in data:
            result["replication_version"] = data["replicationVersion"]
        return result

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return self.compatibility_formatter(self._data)


class CollectionProperties(JsonWrapper):
    """Properties of a collection.

    Example:
        .. code-block:: json

            {
              "writeConcern" : 1,
              "waitForSync" : true,
              "usesRevisionsAsDocumentIds" : true,
              "syncByRevision" : true,
              "statusString" : "loaded",
              "id" : "68452",
              "isSmartChild" : false,
              "schema" : null,
              "name" : "products",
              "type" : 2,
              "status" : 3,
              "cacheEnabled" : false,
              "isSystem" : false,
              "internalValidatorType" : 0,
              "globallyUniqueId" : "hDA74058C1843/68452",
              "keyOptions" : {
                "allowUserKeys" : true,
                "type" : "traditional",
                "lastValue" : 0
              },
              "computedValues" : null,
              "objectId" : "68453"
            }

    References:
        - `get-the-properties-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-properties-of-a-collection>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def write_concern(self) -> Optional[int]:
        return self._data.get("writeConcern")

    @property
    def wait_for_sync(self) -> Optional[bool]:
        return self._data.get("waitForSync")

    @property
    def use_revisions_as_document_ids(self) -> Optional[bool]:
        return self._data.get("usesRevisionsAsDocumentIds")

    @property
    def sync_by_revision(self) -> Optional[bool]:
        return self._data.get("syncByRevision")

    @property
    def status_string(self) -> Optional[str]:
        return self._data.get("statusString")

    @property
    def id(self) -> str:
        return self._data["id"]  # type: ignore[no-any-return]

    @property
    def is_smart_child(self) -> bool:
        return self._data["isSmartChild"]  # type: ignore[no-any-return]

    @property
    def schema(self) -> Optional[Json]:
        return self._data.get("schema")

    @property
    def name(self) -> str:
        return self._data["name"]  # type: ignore[no-any-return]

    @property
    def type(self) -> CollectionType:
        return CollectionType.from_int(self._data["type"])

    @property
    def status(self) -> CollectionStatus:
        return CollectionStatus.from_int(self._data["status"])

    @property
    def cache_enabled(self) -> Optional[bool]:
        return self._data.get("cacheEnabled")

    @property
    def is_system(self) -> bool:
        return self._data["isSystem"]  # type: ignore[no-any-return]

    @property
    def internal_validator_type(self) -> Optional[int]:
        return self._data.get("internalValidatorType")

    @property
    def globally_unique_id(self) -> str:
        return self._data["globallyUniqueId"]  # type: ignore[no-any-return]

    @property
    def key_options(self) -> KeyOptions:
        return KeyOptions(self._data["keyOptions"])

    @property
    def computed_values(self) -> Optional[Json]:
        return self._data.get("computedValues")

    @property
    def object_id(self) -> str:
        return self._data["objectId"]  # type: ignore[no-any-return]

    @staticmethod
    def compatibility_formatter(data: Json) -> Json:
        """python-arango compatibility formatter."""
        result: Json = {}
        if "id" in data:
            result["id"] = data["id"]
        if "objectId" in data:
            result["object_id"] = data["objectId"]
        if "name" in data:
            result["name"] = data["name"]
        if "isSystem" in data:
            result["system"] = data["isSystem"]
        if "isSmart" in data:
            result["smart"] = data["isSmart"]
        if "type" in data:
            result["type"] = data["type"]
            result["edge"] = data["type"] == 3
        if "waitForSync" in data:
            result["sync"] = data["waitForSync"]
        if "status" in data:
            result["status"] = data["status"]
        if "statusString" in data:
            result["status_string"] = data["statusString"]
        if "globallyUniqueId" in data:
            result["global_id"] = data["globallyUniqueId"]
        if "cacheEnabled" in data:
            result["cache"] = data["cacheEnabled"]
        if "replicationFactor" in data:
            result["replication_factor"] = data["replicationFactor"]
        if "minReplicationFactor" in data:
            result["min_replication_factor"] = data["minReplicationFactor"]
        if "writeConcern" in data:
            result["write_concern"] = data["writeConcern"]
        if "shards" in data:
            result["shards"] = data["shards"]
        if "replicationFactor" in data:
            result["replication_factor"] = data["replicationFactor"]
        if "numberOfShards" in data:
            result["shard_count"] = data["numberOfShards"]
        if "shardKeys" in data:
            result["shard_fields"] = data["shardKeys"]
        if "distributeShardsLike" in data:
            result["shard_like"] = data["distributeShardsLike"]
        if "shardingStrategy" in data:
            result["sharding_strategy"] = data["shardingStrategy"]
        if "smartJoinAttribute" in data:
            result["smart_join_attribute"] = data["smartJoinAttribute"]
        if "keyOptions" in data:
            result["key_options"] = KeyOptions.compatibility_formatter(
                data["keyOptions"]
            )
        if "cid" in data:
            result["cid"] = data["cid"]
        if "version" in data:
            result["version"] = data["version"]
        if "allowUserKeys" in data:
            result["user_keys"] = data["allowUserKeys"]
        if "planId" in data:
            result["plan_id"] = data["planId"]
        if "deleted" in data:
            result["deleted"] = data["deleted"]
        if "syncByRevision" in data:
            result["sync_by_revision"] = data["syncByRevision"]
        if "tempObjectId" in data:
            result["temp_object_id"] = data["tempObjectId"]
        if "usesRevisionsAsDocumentIds" in data:
            result["rev_as_id"] = data["usesRevisionsAsDocumentIds"]
        if "isDisjoint" in data:
            result["disjoint"] = data["isDisjoint"]
        if "isSmartChild" in data:
            result["smart_child"] = data["isSmartChild"]
        if "minRevision" in data:
            result["min_revision"] = data["minRevision"]
        if "schema" in data:
            result["schema"] = data["schema"]
        if data.get("computedValues") is not None:
            result["computedValues"] = data["computedValues"]
        if "internalValidatorType" in data:
            result["internal_validator_type"] = data["internalValidatorType"]
        return result

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return self.compatibility_formatter(self._data)


class IndexProperties(JsonWrapper):
    """Properties of an index.

    Example:
        .. code-block:: json

            {
              "fields" : [
                "_key"
              ],
              "id" : "products/0",
              "name" : "primary",
              "selectivityEstimate" : 1,
              "sparse" : false,
              "type" : "primary",
              "unique" : true,
            }

    References:
        - `get-an-index <https://docs.arangodb.com/stable/develop/http-api/indexes/#get-an-index>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def id(self) -> str:
        return self._data["id"]  # type: ignore[no-any-return]

    @property
    def numeric_id(self) -> int:
        return int(self._data["id"].split("/", 1)[-1])

    @property
    def type(self) -> str:
        return self._data["type"]  # type: ignore[no-any-return]

    @property
    def fields(self) -> Json | List[str]:
        return self._data["fields"]  # type: ignore[no-any-return]

    @property
    def name(self) -> Optional[str]:
        return self._data.get("name")

    @property
    def deduplicate(self) -> Optional[bool]:
        return self._data.get("deduplicate")

    @property
    def sparse(self) -> Optional[bool]:
        return self._data.get("sparse")

    @property
    def unique(self) -> Optional[bool]:
        return self._data.get("unique")

    @property
    def geo_json(self) -> Optional[bool]:
        return self._data.get("geoJson")

    @property
    def selectivity_estimate(self) -> Optional[float]:
        return self._data.get("selectivityEstimate")

    @property
    def is_newly_created(self) -> Optional[bool]:
        return self._data.get("isNewlyCreated")

    @property
    def expire_after(self) -> Optional[int]:
        return self._data.get("expireAfter")

    @property
    def in_background(self) -> Optional[bool]:
        return self._data.get("inBackground")

    @property
    def max_num_cover_cells(self) -> Optional[int]:
        return self._data.get("maxNumCoverCells")

    @property
    def cache_enabled(self) -> Optional[bool]:
        return self._data.get("cacheEnabled")

    @property
    def legacy_polygons(self) -> Optional[bool]:
        return self._data.get("legacyPolygons")

    @property
    def estimates(self) -> Optional[bool]:
        return self._data.get("estimates")

    @property
    def analyzer(self) -> Optional[str]:
        return self._data.get("analyzer")

    @property
    def cleanup_interval_step(self) -> Optional[int]:
        return self._data.get("cleanupIntervalStep")

    @property
    def commit_interval_msec(self) -> Optional[int]:
        return self._data.get("commitIntervalMsec")

    @property
    def consolidation_interval_msec(self) -> Optional[int]:
        return self._data.get("consolidationIntervalMsec")

    @property
    def consolidation_policy(self) -> Optional[Json]:
        return self._data.get("consolidationPolicy")

    @property
    def primary_sort(self) -> Optional[Json]:
        return self._data.get("primarySort")

    @property
    def stored_values(self) -> Optional[List[Any]]:
        return self._data.get("storedValues")

    @property
    def write_buffer_active(self) -> Optional[int]:
        return self._data.get("writeBufferActive")

    @property
    def write_buffer_idle(self) -> Optional[int]:
        return self._data.get("writeBufferIdle")

    @property
    def write_buffer_size_max(self) -> Optional[int]:
        return self._data.get("writeBufferSizeMax")

    @property
    def primary_key_cache(self) -> Optional[bool]:
        return self._data.get("primaryKeyCache")

    @property
    def parallelism(self) -> Optional[int]:
        return self._data.get("parallelism")

    @property
    def optimize_top_k(self) -> Optional[List[str]]:
        return self._data.get("optimizeTopK")

    @property
    def track_list_positions(self) -> Optional[bool]:
        return self._data.get("trackListPositions")

    @property
    def version(self) -> Optional[int]:
        return self._data.get("version")

    @property
    def include_all_fields(self) -> Optional[bool]:
        return self._data.get("includeAllFields")

    @property
    def features(self) -> Optional[List[str]]:
        return self._data.get("features")

    @staticmethod
    def compatibility_formatter(data: Json) -> Json:
        """python-arango compatibility formatter."""
        result = {"id": data["id"].split("/", 1)[-1], "fields": data["fields"]}
        if "type" in data:
            result["type"] = data["type"]
        if "name" in data:
            result["name"] = data["name"]
        if "deduplicate" in data:
            result["deduplicate"] = data["deduplicate"]
        if "sparse" in data:
            result["sparse"] = data["sparse"]
        if "unique" in data:
            result["unique"] = data["unique"]
        if "geoJson" in data:
            result["geo_json"] = data["geoJson"]
        if "selectivityEstimate" in data:
            result["selectivity"] = data["selectivityEstimate"]
        if "isNewlyCreated" in data:
            result["new"] = data["isNewlyCreated"]
        if "expireAfter" in data:
            result["expiry_time"] = data["expireAfter"]
        if "inBackground" in data:
            result["in_background"] = data["inBackground"]
        if "maxNumCoverCells" in data:
            result["max_num_cover_cells"] = data["maxNumCoverCells"]
        if "storedValues" in data:
            result["storedValues"] = data["storedValues"]
        if "legacyPolygons" in data:
            result["legacyPolygons"] = data["legacyPolygons"]
        if "estimates" in data:
            result["estimates"] = data["estimates"]
        if "analyzer" in data:
            result["analyzer"] = data["analyzer"]
        if "cleanupIntervalStep" in data:
            result["cleanup_interval_step"] = data["cleanupIntervalStep"]
        if "commitIntervalMsec" in data:
            result["commit_interval_msec"] = data["commitIntervalMsec"]
        if "consolidationIntervalMsec" in data:
            result["consolidation_interval_msec"] = data["consolidationIntervalMsec"]
        if "consolidationPolicy" in data:
            result["consolidation_policy"] = data["consolidationPolicy"]
        if "features" in data:
            result["features"] = data["features"]
        if "primarySort" in data:
            result["primary_sort"] = data["primarySort"]
        if "trackListPositions" in data:
            result["track_list_positions"] = data["trackListPositions"]
        if "version" in data:
            result["version"] = data["version"]
        if "writebufferIdle" in data:
            result["writebuffer_idle"] = data["writebufferIdle"]
        if "writebufferActive" in data:
            result["writebuffer_active"] = data["writebufferActive"]
        if "writebufferSizeMax" in data:
            result["writebuffer_max_size"] = data["writebufferSizeMax"]
        if "optimizeTopK" in data:
            result["optimizeTopK"] = data["optimizeTopK"]
        return result

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return self.compatibility_formatter(self._data)


class QueryProperties(JsonWrapper):
    """Extra options for AQL queries.

    Args:
        allow_dirty_reads (bool | None): If set to `True`, when executing the query
            against a cluster deployment, the Coordinator is allowed to read from any
            shard replica and not only from the leader.
        allow_retry (bool | None): Setting it to `True` makes it possible to retry
            fetching the latest batch from a cursor.
        fail_on_warning (bool | None): If set to `True`, the query will throw an
            exception and abort instead of producing a warning.
        fill_block_cache (bool | None): If set to `True`, it will make the query
            store the data it reads via the RocksDB storage engine in the RocksDB
            block cache.
        full_count (bool | None): If set to `True` and the query contains a LIMIT
            clause, then the result will have some extra attributes.
        intermediate_commit_count (int | None): The maximum number of operations
            after which an intermediate commit is performed automatically.
        intermediate_commit_size (int | None): The maximum total size of operations
            after which an intermediate commit is performed automatically.
        max_dnf_condition_members (int | None): A threshold for the maximum number of
            OR sub-nodes in the internal representation of an AQL FILTER condition.
        max_nodes_per_callstack (int | None): The number of execution nodes in the
            query plan after that stack splitting is performed to avoid a potential
            stack overflow.
        max_number_of_plans (int | None): Limits the maximum number of plans that
            are created by the AQL query optimizer.
        max_runtime (float | None): The query has to be executed within the given
            runtime or it is killed. The value is specified in seconds. If unspecified,
            there will be no timeout.
        max_transaction_size (int | None): The maximum transaction size in bytes.
        max_warning_count (int | None): Limits the maximum number of warnings a
            query will return.
        optimizer (dict | None): Options related to the query optimizer.
        profile (int | None): Return additional profiling information in the query
            result. Can be set to 1 or 2 (for more detailed profiling information).
        satellite_sync_wait (flat | None): How long a DB-Server has time to bring
            the SatelliteCollections involved in the query into sync (in seconds).
        skip_inaccessible_collections (bool | None): Treat collections to which a user
            has no access rights for as if these collections are empty.
        spill_over_threshold_memory_usage (int | None): This option allows queries to
            store intermediate and final results temporarily on disk if the amount of
            memory used (in bytes) exceeds the specified value.
        spill_over_threshold_num_rows (int | None): This option allows queries to
            store intermediate and final results temporarily on disk if the number
            of rows produced by the query exceeds the specified value.
        stream (bool | None): Can be enabled to execute the query lazily.
        use_plan_cache (bool | None): Set this option to `True` to utilize
            a cached query plan or add the execution plan of this query to the
            cache if it’s not in the cache yet.

    Example:
        .. code-block:: json

            {
              "maxPlans": 1,
              "optimizer": {
                "rules": [
                  "-all",
                  "+remove-unnecessary-filters"
                ]
              }
            }

    References:
        - `create-a-cursor <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor_body_options>`__
    """  # noqa: E501

    def __init__(
        self,
        allow_dirty_reads: Optional[bool] = None,
        allow_retry: Optional[bool] = None,
        fail_on_warning: Optional[bool] = None,
        fill_block_cache: Optional[bool] = None,
        full_count: Optional[bool] = None,
        intermediate_commit_count: Optional[int] = None,
        intermediate_commit_size: Optional[int] = None,
        max_dnf_condition_members: Optional[int] = None,
        max_nodes_per_callstack: Optional[int] = None,
        max_number_of_plans: Optional[int] = None,
        max_runtime: Optional[Number] = None,
        max_transaction_size: Optional[int] = None,
        max_warning_count: Optional[int] = None,
        optimizer: Optional[Json] = None,
        profile: Optional[int] = None,
        satellite_sync_wait: Optional[Number] = None,
        skip_inaccessible_collections: Optional[bool] = None,
        spill_over_threshold_memory_usage: Optional[int] = None,
        spill_over_threshold_num_rows: Optional[int] = None,
        stream: Optional[bool] = None,
        use_plan_cache: Optional[bool] = None,
    ) -> None:
        data: Json = dict()
        if allow_dirty_reads is not None:
            data["allowDirtyReads"] = allow_dirty_reads
        if allow_retry is not None:
            data["allowRetry"] = allow_retry
        if fail_on_warning is not None:
            data["failOnWarning"] = fail_on_warning
        if fill_block_cache is not None:
            data["fillBlockCache"] = fill_block_cache
        if full_count is not None:
            data["fullCount"] = full_count
        if intermediate_commit_count is not None:
            data["intermediateCommitCount"] = intermediate_commit_count
        if intermediate_commit_size is not None:
            data["intermediateCommitSize"] = intermediate_commit_size
        if max_dnf_condition_members is not None:
            data["maxDNFConditionMembers"] = max_dnf_condition_members
        if max_nodes_per_callstack is not None:
            data["maxNodesPerCallstack"] = max_nodes_per_callstack
        if max_number_of_plans is not None:
            data["maxNumberOfPlans"] = max_number_of_plans
        if max_runtime is not None:
            data["maxRuntime"] = max_runtime
        if max_transaction_size is not None:
            data["maxTransactionSize"] = max_transaction_size
        if max_warning_count is not None:
            data["maxWarningCount"] = max_warning_count
        if optimizer is not None:
            data["optimizer"] = optimizer
        if profile is not None:
            data["profile"] = profile
        if satellite_sync_wait is not None:
            data["satelliteSyncWait"] = satellite_sync_wait
        if skip_inaccessible_collections is not None:
            data["skipInaccessibleCollections"] = skip_inaccessible_collections
        if spill_over_threshold_memory_usage is not None:
            data["spillOverThresholdMemoryUsage"] = spill_over_threshold_memory_usage
        if spill_over_threshold_num_rows is not None:
            data["spillOverThresholdNumRows"] = spill_over_threshold_num_rows
        if stream is not None:
            data["stream"] = stream
        if use_plan_cache is not None:
            data["usePlanCache"] = use_plan_cache
        super().__init__(data)

    @property
    def allow_dirty_reads(self) -> Optional[bool]:
        return self._data.get("allowDirtyReads")

    @property
    def allow_retry(self) -> Optional[bool]:
        return self._data.get("allowRetry")

    @property
    def fail_on_warning(self) -> Optional[bool]:
        return self._data.get("failOnWarning")

    @property
    def fill_block_cache(self) -> Optional[bool]:
        return self._data.get("fillBlockCache")

    @property
    def full_count(self) -> Optional[bool]:
        return self._data.get("fullCount")

    @property
    def intermediate_commit_count(self) -> Optional[int]:
        return self._data.get("intermediateCommitCount")

    @property
    def intermediate_commit_size(self) -> Optional[int]:
        return self._data.get("intermediateCommitSize")

    @property
    def max_dnf_condition_members(self) -> Optional[int]:
        return self._data.get("maxDNFConditionMembers")

    @property
    def max_nodes_per_callstack(self) -> Optional[int]:
        return self._data.get("maxNodesPerCallstack")

    @property
    def max_number_of_plans(self) -> Optional[int]:
        return self._data.get("maxNumberOfPlans")

    @property
    def max_runtime(self) -> Optional[Number]:
        return self._data.get("maxRuntime")

    @property
    def max_transaction_size(self) -> Optional[int]:
        return self._data.get("maxTransactionSize")

    @property
    def max_warning_count(self) -> Optional[int]:
        return self._data.get("maxWarningCount")

    @property
    def optimizer(self) -> Optional[Json]:
        return self._data.get("optimizer")

    @property
    def profile(self) -> Optional[int]:
        return self._data.get("profile")

    @property
    def satellite_sync_wait(self) -> Optional[Number]:
        return self._data.get("satelliteSyncWait")

    @property
    def skip_inaccessible_collections(self) -> Optional[bool]:
        return self._data.get("skipInaccessibleCollections")

    @property
    def spill_over_threshold_memory_usage(self) -> Optional[int]:
        return self._data.get("spillOverThresholdMemoryUsage")

    @property
    def spill_over_threshold_num_rows(self) -> Optional[int]:
        return self._data.get("spillOverThresholdNumRows")

    @property
    def stream(self) -> Optional[bool]:
        return self._data.get("stream")

    @property
    def use_plan_cache(self) -> Optional[bool]:
        return self._data.get("usePlanCache")


class QueryExecutionPlan(JsonWrapper):
    """The execution plan of an AQL query.

    References:
        - `plan <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor_res_201_extra_plan>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def collections(self) -> Optional[Jsons]:
        return self._data.get("collections")

    @property
    def estimated_cost(self) -> Optional[float]:
        return self._data.get("estimatedCost")

    @property
    def estimated_nr_items(self) -> Optional[int]:
        return self._data.get("estimatedNrItems")

    @property
    def is_modification_query(self) -> Optional[bool]:
        return self._data.get("isModificationQuery")

    @property
    def nodes(self) -> Optional[Jsons]:
        return self._data.get("nodes")

    @property
    def rules(self) -> Optional[List[str]]:
        return self._data.get("rules")

    @property
    def variables(self) -> Optional[Jsons]:
        return self._data.get("variables")


class QueryExecutionProfile(JsonWrapper):
    """The duration of the different query execution phases in seconds.

    Example:
        .. code-block:: json

            {
              "initializing" : 0.0000028529999838156073,
              "parsing" : 0.000029285000010759177,
              "optimizing ast" : 0.0000040699999885873694,
              "loading collections" : 0.000012807000018710823,
              "instantiating plan" : 0.00002348999998957879,
              "optimizing plan" : 0.00006598600000984334,
              "instantiating executors" : 0.000027471999999306718,
              "executing" : 0.7550992429999894,
              "finalizing" : 0.00004103500000951499
            }

    References:
        - `profile <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor_res_201_extra_profile>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def executing(self) -> Optional[float]:
        return self._data.get("executing")

    @property
    def finalizing(self) -> Optional[float]:
        return self._data.get("finalizing")

    @property
    def initializing(self) -> Optional[float]:
        return self._data.get("initializing")

    @property
    def instantiating_executors(self) -> Optional[float]:
        return self._data.get("instantiating executors")

    @property
    def instantiating_plan(self) -> Optional[float]:
        return self._data.get("instantiating plan")

    @property
    def loading_collections(self) -> Optional[float]:
        return self._data.get("loading collections")

    @property
    def optimizing_ast(self) -> Optional[float]:
        return self._data.get("optimizing ast")

    @property
    def optimizing_plan(self) -> Optional[float]:
        return self._data.get("optimizing plan")

    @property
    def parsing(self) -> Optional[float]:
        return self._data.get("parsing")


class QueryExecutionStats(JsonWrapper):
    """Statistics of an AQL query.

    Example:
        .. code-block:: json

            {
              "writesExecuted" : 0,
              "writesIgnored" : 0,
              "documentLookups" : 0,
              "seeks" : 0,
              "scannedFull" : 2,
              "scannedIndex" : 0,
              "cursorsCreated" : 0,
              "cursorsRearmed" : 0,
              "cacheHits" : 0,
              "cacheMisses" : 0,
              "filtered" : 0,
              "httpRequests" : 0,
              "executionTime" : 0.00019362399999067748,
              "peakMemoryUsage" : 0,
              "intermediateCommits" : 0
            }

    References:
        - `stats <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor_res_201_extra_stats>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def cache_hits(self) -> Optional[int]:
        return self._data.get("cacheHits")

    @property
    def cache_misses(self) -> Optional[int]:
        return self._data.get("cacheMisses")

    @property
    def cursors_created(self) -> Optional[int]:
        return self._data.get("cursorsCreated")

    @property
    def cursors_rearmed(self) -> Optional[int]:
        return self._data.get("cursorsRearmed")

    @property
    def document_lookups(self) -> Optional[int]:
        return self._data.get("documentLookups")

    @property
    def execution_time(self) -> Optional[float]:
        return self._data.get("executionTime")

    @property
    def filtered(self) -> Optional[int]:
        return self._data.get("filtered")

    @property
    def full_count(self) -> Optional[int]:
        return self._data.get("fullCount")

    @property
    def http_requests(self) -> Optional[int]:
        return self._data.get("httpRequests")

    @property
    def intermediate_commits(self) -> Optional[int]:
        return self._data.get("intermediateCommits")

    @property
    def nodes(self) -> Optional[Jsons]:
        return self._data.get("nodes")

    @property
    def peak_memory_usage(self) -> Optional[int]:
        return self._data.get("peakMemoryUsage")

    @property
    def scanned_full(self) -> Optional[int]:
        return self._data.get("scannedFull")

    @property
    def scanned_index(self) -> Optional[int]:
        return self._data.get("scannedIndex")

    @property
    def seeks(self) -> Optional[int]:
        return self._data.get("seeks")

    @property
    def writes_executed(self) -> Optional[int]:
        return self._data.get("writesExecuted")

    @property
    def writes_ignored(self) -> Optional[int]:
        return self._data.get("writesIgnored")


class QueryExecutionExtra(JsonWrapper):
    """Extra information about the query result.

    References:
        - `extra <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor_res_201_extra>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)
        self._plan = QueryExecutionPlan(data.get("plan", dict()))
        self._profile = QueryExecutionProfile(data.get("profile", dict()))
        self._stats = QueryExecutionStats(data.get("stats", dict()))
        self._warnings: Jsons = data.get("warnings", list())

    @property
    def plan(self) -> QueryExecutionPlan:
        return self._plan

    @property
    def profile(self) -> QueryExecutionProfile:
        return self._profile

    @property
    def stats(self) -> QueryExecutionStats:
        return self._stats

    @property
    def warnings(self) -> Jsons:
        return self._warnings


class QueryTrackingConfiguration(JsonWrapper):
    """AQL query tracking configuration.

    Example:
        .. code-block:: json

            {
                "enabled": true,
                "trackSlowQueries": true,
                "trackBindVars": true,
                "maxSlowQueries": 64,
                "slowQueryThreshold": 10,
                "slowStreamingQueryThreshold": 10,
                "maxQueryStringLength": 4096
            }

    References:
        - `get-the-aql-query-tracking-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#get-the-aql-query-tracking-configuration>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def enabled(self) -> bool:
        return cast(bool, self._data["enabled"])

    @property
    def track_slow_queries(self) -> bool:
        return cast(bool, self._data["trackSlowQueries"])

    @property
    def track_bind_vars(self) -> bool:
        return cast(bool, self._data["trackBindVars"])

    @property
    def max_slow_queries(self) -> int:
        return cast(int, self._data["maxSlowQueries"])

    @property
    def slow_query_threshold(self) -> int:
        return cast(int, self._data["slowQueryThreshold"])

    @property
    def slow_streaming_query_threshold(self) -> Optional[int]:
        return self._data.get("slowStreamingQueryThreshold")

    @property
    def max_query_string_length(self) -> int:
        return cast(int, self._data["maxQueryStringLength"])


class QueryExplainOptions(JsonWrapper):
    """Options for explaining an AQL query.

    Args:
        all_plans (bool | None): If set to `True`, all possible execution plans are
            returned.
        max_plans (int | None): The maximum number of plans to return.
        optimizer (dict | None): Options related to the query optimizer.

    Example:
        .. code-block:: json

            {
              "allPlans" : false,
              "maxNumberOfPlans" : 1,
              "optimizer" : {
                "rules" : [
                  "-all",
                  "+use-indexe-for-sort"
                ]
              }
            }

    References:
        - `explain-an-aql-query <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#explain-an-aql-query>`__
    """  # noqa: E501

    def __init__(
        self,
        all_plans: Optional[bool] = None,
        max_plans: Optional[int] = None,
        optimizer: Optional[Json] = None,
    ) -> None:
        data: Json = dict()
        if all_plans is not None:
            data["allPlans"] = all_plans
        if max_plans is not None:
            data["maxNumberOfPlans"] = max_plans
        if optimizer is not None:
            data["optimizer"] = optimizer
        super().__init__(data)

    @property
    def all_plans(self) -> Optional[bool]:
        return self._data.get("allPlans")

    @property
    def max_plans(self) -> Optional[int]:
        return self._data.get("maxNumberOfPlans")

    @property
    def optimizer(self) -> Optional[Json]:
        return self._data.get("optimizer")


class QueryCacheProperties(JsonWrapper):
    """AQL Cache Configuration.

    Example:
        .. code-block:: json

           {
             "mode" : "demand",
             "maxResults" : 128,
             "maxResultsSize" : 268435456,
             "maxEntrySize" : 16777216,
             "includeSystem" : false
           }

    References:
        - `get-the-aql-query-results-cache-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#get-the-aql-query-results-cache-configuration>`__
        - `set-the-aql-query-results-cache-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#set-the-aql-query-results-cache-configuration>`__
    """  # noqa: E501

    def __init__(self, data: Json) -> None:
        super().__init__(data)

    @property
    def mode(self) -> str:
        return cast(str, self._data.get("mode", ""))

    @property
    def max_results(self) -> int:
        return cast(int, self._data.get("maxResults", 0))

    @property
    def max_results_size(self) -> int:
        return cast(int, self._data.get("maxResultsSize", 0))

    @property
    def max_entry_size(self) -> int:
        return cast(int, self._data.get("maxEntrySize", 0))

    @property
    def include_system(self) -> bool:
        return cast(bool, self._data.get("includeSystem", False))
