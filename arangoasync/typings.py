from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from multidict import CIMultiDictProxy, MultiDict

from arangoasync.job import AsyncJob

Json = Dict[str, Any]
Json.__doc__ = """Type definition for request/response body"""

Jsons = List[Json]
Jsons.__doc__ = """Type definition for a list of JSON objects"""

RequestHeaders = MutableMapping[str, str] | MultiDict[str]
RequestHeaders.__doc__ = """Type definition for request HTTP headers"""

ResponseHeaders = MutableMapping[str, str] | MultiDict[str] | CIMultiDictProxy[str]
ResponseHeaders.__doc__ = """Type definition for response HTTP headers"""

Params = MutableMapping[str, bool | int | str]
Params.__doc__ = """Type definition for URL (query) parameters"""

Formatter = Callable[[Json], Json]
Formatter.__doc__ = """Type definition for a JSON formatter"""

T = TypeVar("T")
Result = Union[T, AsyncJob[T]]


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
        """Apply a formatter to the data. Returns the unmodified data by default."""
        if formatter is not None:
            return formatter(self._data)
        return self._data


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

    def format(self, formatter: Optional[Formatter] = None) -> Json:
        """Apply a formatter to the data.

        By default, the python-arango compatibility formatter is applied.
        """
        if formatter is not None:
            return super().format(formatter)
        return {
            "id": self._data["id"],
            "name": self.name,
            "system": self.is_system,
            "type": str(self.col_type),
            "status": str(self.status),
        }


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
        return self._data.get("user")  # type: ignore[return-value]

    @property
    def password(self) -> Optional[str]:
        return self._data.get("password")

    @property
    def active(self) -> bool:
        return self._data.get("active")  # type: ignore[return-value]

    @property
    def extra(self) -> Optional[Json]:
        return self._data.get("extra")

    def to_dict(self) -> Json:
        """Return the dictionary."""
        return dict(
            user=self.user,
            password=self.password,
            active=self.active,
            extra=self.extra,
        )


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
