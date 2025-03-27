Async API Execution
-------------------

In **asynchronous API executions**, python-arango-async sends API requests to ArangoDB in
fire-and-forget style. The server processes the requests in the background, and
the results can be retrieved once available via `AsyncJob` objects.
