import pytest

from arangoasync.auth import Auth
from arangoasync.connection import BasicConnection
from arangoasync.exceptions import ServerConnectionError
from arangoasync.http import AioHTTPClient
from arangoasync.resolver import DefaultHostResolver


@pytest.mark.asyncio
async def test_BasicConnection_ping_failed(url, sys_db_name):
    client = AioHTTPClient()
    session = client.create_session(url)
    resolver = DefaultHostResolver(1)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
    )

    with pytest.raises(ServerConnectionError):
        await connection.ping()
    await session.close()


@pytest.mark.asyncio
async def test_BasicConnection_ping_success(url, sys_db_name, root, password):
    client = AioHTTPClient()
    session = client.create_session(url)
    resolver = DefaultHostResolver(1)

    connection = BasicConnection(
        sessions=[session],
        host_resolver=resolver,
        http_client=client,
        db_name=sys_db_name,
        auth=Auth(username=root, password=password),
    )

    status_code = await connection.ping()
    assert status_code == 200
    await session.close()
