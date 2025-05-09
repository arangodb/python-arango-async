import pytest

from arangoasync.auth import JwtToken
from arangoasync.client import ArangoClient
from arangoasync.compression import DefaultCompressionManager
from arangoasync.http import DefaultHTTPClient
from arangoasync.resolver import DefaultHostResolver, RoundRobinHostResolver
from arangoasync.version import __version__


@pytest.mark.asyncio
async def test_client_attributes(monkeypatch):
    hosts = ["http://127.0.0.1:8529", "http://localhost:8529"]

    async with ArangoClient(hosts=hosts[0]) as client:
        assert client.version == __version__
        assert client.hosts == [hosts[0]]
        assert repr(client) == f"<ArangoClient {hosts[0]}>"
        assert isinstance(client.host_resolver, DefaultHostResolver)
        assert client.compression is None
        assert len(client.sessions) == 1

    with pytest.raises(ValueError):
        async with ArangoClient(hosts=hosts, host_resolver="invalid") as _:
            pass

    http_client = DefaultHTTPClient()
    create_session = 0
    close_session = 0

    class MockSession:
        async def close(self):
            nonlocal close_session
            close_session += 1

    def mock_method(*args, **kwargs):
        nonlocal create_session
        create_session += 1
        return MockSession()

    monkeypatch.setattr(http_client, "create_session", mock_method)
    async with ArangoClient(
        hosts=hosts,
        host_resolver="roundrobin",
        http_client=http_client,
        compression=DefaultCompressionManager(threshold=5000),
    ) as client:
        assert repr(client) == f"<ArangoClient {hosts[0]},{hosts[1]}>"
        assert isinstance(client.host_resolver, RoundRobinHostResolver)
        assert isinstance(client.compression, DefaultCompressionManager)
        assert client.compression.threshold == 5000
        assert len(client.sessions) == len(hosts)
        assert create_session == 2
    assert close_session == 2


@pytest.mark.asyncio
async def test_client_bad_auth_method(url, sys_db_name):
    async with ArangoClient(hosts=url) as client:
        with pytest.raises(ValueError):
            await client.db(sys_db_name, auth_method="invalid")


@pytest.mark.asyncio
async def test_client_basic_auth(url, sys_db_name, basic_auth_root):
    # successful authentication
    async with ArangoClient(hosts=url) as client:
        await client.db(
            sys_db_name,
            auth_method="basic",
            auth=basic_auth_root,
            verify=True,
        )

    # auth missing
    async with ArangoClient(hosts=url) as client:
        with pytest.raises(ValueError):
            await client.db(
                sys_db_name,
                auth_method="basic",
                auth=None,
                token=JwtToken.generate_token("test"),
                verify=True,
            )


@pytest.mark.asyncio
async def test_client_jwt_auth(url, sys_db_name, basic_auth_root):
    token: JwtToken

    # successful authentication with auth only
    async with ArangoClient(hosts=url) as client:
        db = await client.db(
            sys_db_name,
            auth_method="jwt",
            auth=basic_auth_root,
            verify=True,
        )
        token = db.connection.token

    # successful authentication with token only
    async with ArangoClient(hosts=url) as client:
        await client.db(sys_db_name, auth_method="jwt", token=token, verify=True)

    # successful authentication with both
    async with ArangoClient(hosts=url) as client:
        await client.db(
            sys_db_name,
            auth_method="jwt",
            auth=basic_auth_root,
            token=token,
            verify=True,
        )

    # auth and token missing
    async with ArangoClient(hosts=url) as client:
        with pytest.raises(ValueError):
            await client.db(sys_db_name, auth_method="jwt", verify=True)


@pytest.mark.asyncio
async def test_client_jwt_superuser_auth(
    url, sys_db_name, basic_auth_root, token, enterprise
):
    # successful authentication
    async with ArangoClient(hosts=url) as client:
        db = await client.db(
            sys_db_name, auth_method="superuser", token=token, verify=True
        )
        if enterprise:
            await db.jwt_secrets()
            await db.reload_jwt_secrets()

    # token missing
    async with ArangoClient(hosts=url) as client:
        with pytest.raises(ValueError):
            await client.db(
                sys_db_name, auth_method="superuser", auth=basic_auth_root, verify=True
            )
