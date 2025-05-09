.. _HTTP:

HTTP
----

You can define your own HTTP client for sending requests to
ArangoDB server. The default implementation uses the aiohttp_ library.

Your HTTP client must inherit :class:`arangoasync.http.HTTPClient` and implement the
following abstract methods:

* :func:`arangoasync.http.HTTPClient.create_session`
* :func:`arangoasync.http.HTTPClient.close_session`
* :func:`arangoasync.http.HTTPClient.send_request`

Let's take for example, the default implementation of :class:`arangoasync.http.AioHTTPClient`:

* The **create_session** method returns a :class:`aiohttp.ClientSession` instance per
  connected host (coordinator). The session objects are stored in the client.
* The **close_session** method performs the necessary cleanup for a :class:`aiohttp.ClientSession` instance.
  This is usually called only by the client.
* The **send_request** method must uses the session to send an HTTP request, and
  returns a fully populated instance of :class:`arangoasync.response.Response`.

**Example:**

Suppose you're working on a project that uses httpx_ as a dependency and you want your
own HTTP client implementation on top of :class:`httpx.AsyncClient`. Your ``HttpxHTTPClient``
class might look something like this:

.. code-block:: python

    import httpx
    import ssl
    from typing import Any, Optional
    from arangoasync.exceptions import ClientConnectionError
    from arangoasync.http import  HTTPClient
    from arangoasync.request import Request
    from arangoasync.response import  Response

    class HttpxHTTPClient(HTTPClient):
        """HTTP client implementation on top of httpx.AsyncClient.

        Args:
            limits (httpx.Limits | None): Connection pool limits.n
            timeout (httpx.Timeout | float | None): Request timeout settings.
            ssl_context (ssl.SSLContext | bool): SSL validation mode.
                `True` (default) uses httpx’s default validation (system CAs).
                `False` disables SSL checks.
                Or pass a custom `ssl.SSLContext`.
        """

        def __init__(
            self,
            limits: Optional[httpx.Limits] = None,
            timeout: Optional[httpx.Timeout | float] = None,
            ssl_context: bool | ssl.SSLContext = True,
        ) -> None:
            self._limits = limits or httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20
            )
            self._timeout = timeout or httpx.Timeout(300.0, connect=60.0)
            if ssl_context is True:
                self._verify: bool | ssl.SSLContext = True
            elif ssl_context is False:
                self._verify = False
            else:
                self._verify = ssl_context

        def create_session(self, host: str) -> httpx.AsyncClient:
            return httpx.AsyncClient(
                base_url=host,
                limits=self._limits,
                timeout=self._timeout,
                verify=self._verify,
            )

        async def close_session(self, session: httpx.AsyncClient) -> None:
            await session.aclose()

        async def send_request(
            self,
            session: httpx.AsyncClient,
            request: Request,
        ) -> Response:
            auth: Any = None
            if request.auth is not None:
                auth = httpx.BasicAuth(
                    username=request.auth.username,
                    password=request.auth.password,
                )

            try:
                resp = await session.request(
                    method=request.method.name,
                    url=request.endpoint,
                    headers=request.normalized_headers(),
                    params=request.normalized_params(),
                    content=request.data,
                    auth=auth,
                )
                raw_body = resp.content
                return Response(
                    method=request.method,
                    url=str(resp.url),
                    headers=resp.headers,
                    status_code=resp.status_code,
                    status_text=resp.reason_phrase,
                    raw_body=raw_body,
                )
            except httpx.HTTPError as e:
                raise ClientConnectionError(str(e)) from e

Then you would inject your client as follows:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(
            hosts="http://localhost:8529",
            http_client=HttpxHTTPClient(),
    ) as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth, verify=True)

        # List all collections.
        cols = await db.collections()

.. _aiohttp: https://docs.aiohttp.org/en/stable/
.. _httpx: https://www.python-httpx.org/
