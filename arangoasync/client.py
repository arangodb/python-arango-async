__all__ = ["ArangoClient"]

import asyncio
from typing import Any, Optional, Sequence

from arangoasync.auth import Auth, JwtToken
from arangoasync.compression import CompressionManager
from arangoasync.connection import (
    BasicConnection,
    Connection,
    JwtConnection,
    JwtSuperuserConnection,
)
from arangoasync.database import StandardDatabase
from arangoasync.http import DefaultHTTPClient, HTTPClient
from arangoasync.resolver import HostResolver, get_resolver
from arangoasync.serialization import (
    DefaultDeserializer,
    DefaultSerializer,
    Deserializer,
    Serializer,
)
from arangoasync.typings import Json, Jsons
from arangoasync.version import __version__


class ArangoClient:
    """ArangoDB client.

    Args:
        hosts (str | Sequence[str]): Host URL or list of URL's.
            In case of a cluster, this would be the list of coordinators.
            Which coordinator to use is determined by the `host_resolver`.
        host_resolver (str | HostResolver): Host resolver strategy.
            This determines how the client will choose which server to use.
            Passing a string would configure a resolver with the default settings.
            See :class:`DefaultHostResolver <arangoasync.resolver.DefaultHostResolver>`
            and :func:`get_resolver <arangoasync.resolver.get_resolver>`
            for more information.
            If you need more customization, pass a subclass of
            :class:`HostResolver <arangoasync.resolver.HostResolver>`.
        http_client (HTTPClient | None): HTTP client implementation.
            This is the core component that sends requests to the ArangoDB server.
            Defaults to :class:`DefaultHttpClient <arangoasync.http.DefaultHTTPClient>`,
            but you can fully customize its parameters or even use a different
            implementation by subclassing
            :class:`HTTPClient <arangoasync.http.HTTPClient>`.
        compression (CompressionManager | None): Disabled by default.
            Used to compress requests to the server or instruct the server to compress
            responses. Enable it by passing an instance of
            :class:`DefaultCompressionManager
            <arangoasync.compression.DefaultCompressionManager>`
            or a custom subclass of :class:`CompressionManager
            <arangoasync.compression.CompressionManager>`.
        serializer (Serializer | None): Custom JSON serializer implementation.
            Leave as `None` to use the default serializer.
            See :class:`DefaultSerializer
            <arangoasync.serialization.DefaultSerializer>`.
            For custom serialization of collection documents, see :class:`Collection
            <arangoasync.collection.Collection>`.
        deserializer (Deserializer | None): Custom JSON deserializer implementation.
            Leave as `None` to use the default deserializer.
            See :class:`DefaultDeserializer
            <arangoasync.serialization.DefaultDeserializer>`.
            For custom deserialization of collection documents, see :class:`Collection
            <arangoasync.collection.Collection>`.

    Raises:
        ValueError: If the `host_resolver` is not supported.
    """

    def __init__(
        self,
        hosts: str | Sequence[str] = "http://127.0.0.1:8529",
        host_resolver: str | HostResolver = "default",
        http_client: Optional[HTTPClient] = None,
        compression: Optional[CompressionManager] = None,
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
    ) -> None:
        self._hosts = [hosts] if isinstance(hosts, str) else hosts
        self._host_resolver = (
            get_resolver(host_resolver, len(self._hosts))
            if isinstance(host_resolver, str)
            else host_resolver
        )
        self._http_client = http_client or DefaultHTTPClient()
        self._sessions = [
            self._http_client.create_session(host) for host in self._hosts
        ]
        self._compression = compression
        self._serializer: Serializer[Json] = serializer or DefaultSerializer()
        self._deserializer: Deserializer[Json, Jsons] = (
            deserializer or DefaultDeserializer()
        )

    def __repr__(self) -> str:
        return f"<ArangoClient {','.join(self._hosts)}>"

    async def __aenter__(self) -> "ArangoClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    @property
    def hosts(self) -> Sequence[str]:
        """Return the list of hosts."""
        return self._hosts

    @property
    def host_resolver(self) -> HostResolver:
        """Return the host resolver."""
        return self._host_resolver

    @property
    def compression(self) -> Optional[CompressionManager]:
        """Return the compression manager."""
        return self._compression

    @property
    def sessions(self) -> Sequence[Any]:
        """Return the list of sessions.

        You may use this to customize sessions on the fly (for example,
        adjust the timeout). Not recommended unless you know what you are doing.

        Warning:
            Modifying only a subset of sessions may lead to unexpected behavior.
            In order to keep the client in a consistent state, you should make sure
            all sessions are configured in the same way.
        """
        return self._sessions

    @property
    def version(self) -> str:
        """Return the version of the client."""
        return __version__

    async def close(self) -> None:
        """Close HTTP sessions."""
        await asyncio.gather(
            *(self._http_client.close_session(session) for session in self._sessions)
        )

    async def db(
        self,
        name: str,
        auth_method: str = "basic",
        auth: Optional[Auth] = None,
        token: Optional[JwtToken] = None,
        verify: bool = False,
        compression: Optional[CompressionManager] = None,
        serializer: Optional[Serializer[Json]] = None,
        deserializer: Optional[Deserializer[Json, Jsons]] = None,
    ) -> StandardDatabase:
        """Connects to a database and returns and API wrapper.

        Args:
            name (str): Database name.
            auth_method (str): The following methods are supported:

                - "basic": HTTP authentication.
                    Requires the `auth` parameter. The `token` parameter is ignored.
                - "jwt": User JWT authentication.
                    At least one of the `auth` or `token` parameters are required.
                    If `auth` is provided, but the `token` is not, the token will be
                    refreshed automatically. This assumes that the clocks of the server
                    and client are synchronized.
                - "superuser": Superuser JWT authentication.
                    The `token` parameter is required. The `auth` parameter is ignored.
            auth (Auth | None): Login information.
            token (JwtToken | None): JWT token.
            verify (bool): Verify the connection by sending a test request.
            compression (CompressionManager | None): If set, supersedes the
                client-level compression settings.
            serializer (Serializer | None): If set, supersedes the client-level
                serializer.
            deserializer (Deserializer | None): If set, supersedes the client-level
                deserializer.

        Returns:
            StandardDatabase: Database API wrapper.

        Raises:
            ValueError: If the authentication is invalid.
            ServerConnectionError: If `verify` is `True` and the connection fails.
        """
        connection: Connection

        if auth_method == "basic":
            if auth is None:
                raise ValueError("Basic authentication requires the `auth` parameter")
            connection = BasicConnection(
                sessions=self._sessions,
                host_resolver=self._host_resolver,
                http_client=self._http_client,
                db_name=name,
                compression=compression or self._compression,
                serializer=serializer or self._serializer,
                deserializer=deserializer or self._deserializer,
                auth=auth,
            )
        elif auth_method == "jwt":
            if auth is None and token is None:
                raise ValueError(
                    "JWT authentication requires the `auth` or `token` parameter"
                )
            connection = JwtConnection(
                sessions=self._sessions,
                host_resolver=self._host_resolver,
                http_client=self._http_client,
                db_name=name,
                compression=compression or self._compression,
                serializer=serializer or self._serializer,
                deserializer=deserializer or self._deserializer,
                auth=auth,
                token=token,
            )
        elif auth_method == "superuser":
            if token is None:
                raise ValueError(
                    "Superuser JWT authentication requires the `token` parameter"
                )
            connection = JwtSuperuserConnection(
                sessions=self._sessions,
                host_resolver=self._host_resolver,
                http_client=self._http_client,
                db_name=name,
                compression=compression or self._compression,
                serializer=serializer or self._serializer,
                deserializer=deserializer or self._deserializer,
                token=token,
            )
        else:
            raise ValueError(f"Invalid authentication method: {auth_method}")

        if verify:
            await connection.ping()

        return StandardDatabase(connection)
