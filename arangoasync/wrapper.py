from typing import Any, Iterator, Optional, Tuple

from arangoasync.typings import Json


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


class KeyOptions(JsonWrapper):
    """Additional options for key generation, used on collections.

    https://docs.arangodb.com/stable/develop/http-api/collections/#create-a-collection_body_keyOptions

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
    """

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


class User(JsonWrapper):
    """User information.

    https://docs.arangodb.com/stable/develop/http-api/users/#get-a-user

    Args:
        username (str): The name of the user.
        password (str | None): The user password as a string. Note that user
            password is not returned back by the server.
        active (bool): `True` if user is active, `False` otherwise.
        extra (dict | None): Additional user information. For internal use only.
            Should not be set or modified by end users.

    Example:
        .. code-block:: json
            {
                "username": "john",
                "password": "secret",
                "active": true,
                "extra": {}
            }
    """

    def __init__(
        self,
        username: str,
        password: Optional[str] = None,
        active: bool = True,
        extra: Optional[Json] = None,
    ) -> None:
        data = {"username": username, "active": active}
        if password is not None:
            data["password"] = password
        if extra is not None:
            data["extra"] = extra
        super().__init__(data)

    @property
    def username(self) -> str:
        return self._data.get("username")  # type: ignore[return-value]

    @property
    def password(self) -> Optional[str]:
        return self._data.get("password")

    @property
    def active(self) -> bool:
        return self._data.get("active")  # type: ignore[return-value]

    @property
    def extra(self) -> Optional[Json]:
        return self._data.get("extra")


class ServerStatusInformation(JsonWrapper):
    """Status information about the server.

    https://docs.arangodb.com/stable/develop/http-api/administration/#get-server-status-information

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
    """

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
