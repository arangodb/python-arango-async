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
async def test_AioHTTPClient_wrong_url(client_session):
    client = AioHTTPClient()
    session = client_session(client, "http://localhost:0000")
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
    )
    with pytest.raises(ClientConnectionError):
        await client.send_request(session, request)


@pytest.mark.asyncio
async def test_AioHTTPClient_simple_request(client_session, url):
    client = AioHTTPClient()
    session = client_session(client, url)
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
    )
    response = await client.send_request(session, request)
    assert response.method == Method.GET
    assert response.url == f"{url}/_api/version"
    assert response.status_code == 401
    assert response.status_text == "Unauthorized"


@pytest.mark.asyncio
async def test_AioHTTPClient_auth_pass(client_session, url, root, password):
    client = AioHTTPClient()
    session = client_session(client, url)
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
