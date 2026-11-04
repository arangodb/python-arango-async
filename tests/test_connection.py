import time
import zlib
from datetime import datetime, timezone
from unittest.mock import patch

import jwt
import pytest

from arangoasync.auth import Auth, JwtToken
from arangoasync.compression import AcceptEncoding, DefaultCompressionManager
from arangoasync.connection import (
    BasicConnection,
    JwtConnection,
    JwtSuperuserConnection,
)
from arangoasync.exceptions import (
    ClientConnectionAbortedError,
    ClientConnectionError,
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
    connection.prep_response(request, response)
    assert response.is_success is False
    with pytest.raises(ServerConnectionError):
        connection.raise_for_status(request, response)

    error = b'{"error": true, "errorMessage": "msg", "errorNum": 1234}'
    response = Response(Method.GET, url, {}, 0, "ERROR", error)
    connection.prep_response(request, response)
    assert response.error_code == 1234
    assert response.error_message == "msg"


@pytest.mark.asyncio
async def test_BasicConnection_process_request_connection_aborted(
    monkeypatch, client_session, url, sys_db_name, root, password
):
    client = AioHTTPClient()
    session = client_session(client, url)
    max_tries = 4
    resolver = DefaultHostResolver(1, max_tries=max_tries)

    request = Request(method=Method.GET, endpoint="/_api/collection")

    tries = 0

    async def mock_send_request(*args, **kwargs):
        nonlocal tries
        tries += 1
        raise ClientConnectionError("test")

    monkeypatch.setattr(client, "send_request", mock_send_request)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )

    with pytest.raises(ClientConnectionAbortedError):
        await connection.process_request(request)
    assert tries == max_tries


@pytest.mark.asyncio
async def test_JwtConnection_no_auth(client_session, url, sys_db_name):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)
    with pytest.raises(ValueError):
        _ = JwtConnection(
            sessions=[session],
            host_resolver=resolver,
            http_client=client,
            db_name=sys_db_name,
        )


@pytest.mark.asyncio
async def test_JwtConnection_invalid_token(client_session, url, sys_db_name):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    invalid_token = JwtToken.generate_token("invalid token")
    connection = JwtConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        token=invalid_token,
    )
    with pytest.raises(ServerConnectionError):
        await connection.ping()


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

    # Test reusing the token
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

    connection3 = JwtConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )
    connection3.token = connection1.token
    status_code = await connection1.ping()
    assert status_code == 200

    # Refresh the token
    await connection3.refresh_token()
    status_code = await connection1.ping()
    assert status_code == 200
    assert connection3.token != connection1.token


@pytest.mark.asyncio
async def test_JwtConnection_expired_token_recovery(
    client_session, url, sys_db_name, root, password, pytestconfig
):
    client = AioHTTPClient()
    session = client_session(client, url)
    issued_at = int(time.time()) - 120
    encoded_token = jwt.encode(
        {
            "preferred_username": root,
            "iss": "arangodb",
            "iat": issued_at,
            "exp": issued_at + 60,
        },
        key=pytestconfig.getoption("secret"),
        algorithm="HS256",
    )

    # Accept the token while valid, then restore the clock before dispatch.
    with patch("jwt.api_jwt.datetime") as clock:
        clock.now.return_value = datetime.fromtimestamp(issued_at, timezone.utc)
        expired_token = JwtToken(encoded_token)

    assert expired_token.needs_refresh()
    connection = JwtConnection(
        sessions=[session],
        host_resolver=DefaultHostResolver(1),
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
        token=expired_token,
    )

    # Expect that the token is renewed automatically.
    assert await connection.ping() == 200
    assert connection.token.token != expired_token.token
    assert not connection.token.needs_refresh()


@pytest.mark.asyncio
async def test_JwtSuperuserConnection_ping_success(
    client_session, url, sys_db_name, token
):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection = JwtSuperuserConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        token=token,
    )
    assert connection.db_name == sys_db_name
    status_code = await connection.ping()
    assert status_code == 200


@pytest.mark.asyncio
async def test_JwtSuperuserConnection_ping_failed(client_session, url, sys_db_name):
    client = AioHTTPClient()
    session = client_session(client, url)
    resolver = DefaultHostResolver(1)

    connection = JwtSuperuserConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        token=JwtToken.generate_token("invalid token"),
    )
    with pytest.raises(ServerConnectionError):
        await connection.ping()
