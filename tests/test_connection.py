import zlib

import pytest

from arangoasync.auth import Auth
from arangoasync.compression import AcceptEncoding, DefaultCompressionManager
from arangoasync.connection import BasicConnection, JwtConnection
from arangoasync.exceptions import (
    ClientConnectionError,
    ConnectionAbortedError,
    ServerConnectionError,
)
from arangoasync.http import AioHTTPClient
from arangoasync.request import Method, Request
from arangoasync.resolver import DefaultHostResolver
from arangoasync.response import Response


@pytest.mark.asyncio
async def test_BasicConnection_ping_failed(client_session, url, sys_db_name):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
    )

    with pytest.raises(ServerConnectionError):
        await connection.ping()


@pytest.mark.asyncio
async def test_BasicConnection_ping_success(
    client_session, url, sys_db_name, root, password
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )

    assert connection.db_name == sys_db_name
    status_code = await connection.ping()
    assert status_code == 200


@pytest.mark.asyncio
async def test_BasicConnection_with_compression(
    client_session, url, sys_db_name, root, password
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)
    compression = DefaultCompressionManager(
        threshold=2, level=5, accept=AcceptEncoding.DEFLATE
    )

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
        compression=compression,
    )

    data = b"a" * 100
    request = Request(method=Method.GET, endpoint="/_api/collection", data=data)
    _ = await connection.send_request(request)
    assert len(request.data) < len(data)
    assert zlib.decompress(request.data) == data
    assert request.headers["content-encoding"] == "deflate"
    assert request.headers["accept-encoding"] == "deflate"


@pytest.mark.asyncio
async def test_BasicConnection_prep_response_bad_response(
    client_session, url, sys_db_name
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
    )

    request = Request(method=Method.GET, endpoint="/_api/collection")
    response = Response(Method.GET, url, {}, 0, "ERROR", b"")

    with pytest.raises(ServerConnectionError):
        connection.prep_response(request, response)


@pytest.mark.asyncio
async def test_BasicConnection_process_request_connection_aborted(
    monkeypatch, client_session, url, sys_db_name, root, password
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1, 1)

    request = Request(method=Method.GET, endpoint="/_api/collection")

    async def mock_send_request(*args, **kwargs):
        raise ClientConnectionError("test")

    monkeypatch.setattr(client, "send_request", mock_send_request)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )

    with pytest.raises(ConnectionAbortedError):
        await connection.process_request(request)


@pytest.mark.asyncio
async def test_JwtConnection_ping_success(
    client_session, url, sys_db_name, root, password
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection1 = JwtConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )
    assert connection1.db_name == sys_db_name
    status_code = await connection1.ping()
    assert status_code == 200

    connection2 = JwtConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        token=connection1.token,
    )
    assert connection2.db_name == sys_db_name
    status_code = await connection2.ping()
    assert status_code == 200
