import pytest

from arangoasync.auth import Auth
from arangoasync.http import AioHTTPClient
from arangoasync.request import Method, Request


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
    client = AioHTTPClient(auth=Auth(root, password))
    session = client.create_session(url)
    request = Request(
        method=Method.GET,
        endpoint="/_api/version",
    )
    response = await client.send_request(session, request)
    assert response.method == Method.GET
    assert response.url == f"{url}/_api/version"
    assert response.status_code == 200
    assert response.status_text == "OK"
    await session.close()
