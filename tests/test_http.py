import pytest

from arangoasync.auth import Auth
from arangoasync.exceptions import ClientConnectionError
from arangoasync.http import AioHTTPClient, DefaultHTTPClient
from arangoasync.request import Method, Request


def test_DefaultHTTPClient():
    # This test is here in case to prevent accidental changes to the DefaultHTTPClient.
    # Changed should be pushed only after the new HTTP client is covered by tests.
    assert DefaultHTTPClient == AioHTTPClient


@pytest.mark.asyncio
async def test_AioHTTPClient_wrong_url():
    client = AioHTTPClient()
    session = client.create_session("http://www.fasdfdsafadawe3523523532plmcom.tgzs")
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
    )
    with pytest.raises(ClientConnectionError):
        await client.send_request(session, request)
    await session.close()


@pytest.mark.asyncio
async def test_AioHTTPClient_simple_request(url):
    client = AioHTTPClient()
    session = client.create_session(url)
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
    )
    response = await client.send_request(session, request)
    assert response.method == Method.GET
    assert response.url == f"{url}/_api/version"
    assert response.status_code == 401
    assert response.status_text == "Unauthorized"
    await session.close()


@pytest.mark.asyncio
async def test_AioHTTPClient_auth_pass(url, root, password):
    client = AioHTTPClient()
    session = client.create_session(url)
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
        auth=Auth(username=root, password=password),
    )
    response = await client.send_request(session, request)
    assert response.method == Method.GET
    assert response.url == f"{url}/_api/version"
    assert response.status_code == 200
    assert response.status_text == "OK"
    await session.close()
