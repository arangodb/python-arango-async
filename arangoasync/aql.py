__all__ = ["AQL"]


from typing import Optional

from arangoasync.cursor import Cursor
from arangoasync.exceptions import AQLQueryExecuteError
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons, QueryProperties, Result


class AQL:
    """AQL (ArangoDB Query Language) API wrapper.

    Allows you to execute, track, kill, explain, and validate queries written
    in ArangoDB’s query language.

    Args:
        executor: API executor. Required to execute the API requests.
    """

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:
        """Return the name of the current database."""
        return self._executor.db_name

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    def __repr__(self) -> str:
        return f"<AQL in {self.name}>"

    async def execute(
        self,
        query: str,
        count: Optional[bool] = None,
        batch_size: Optional[int] = None,
        bind_vars: Optional[Json] = None,
        cache: Optional[bool] = None,
        memory_limit: Optional[int] = None,
        ttl: Optional[int] = None,
        allow_dirty_read: Optional[bool] = None,
        options: Optional[QueryProperties | Json] = None,
    ) -> Result[Cursor]:
        """Execute the query and return the result cursor.

        Args:
            query (str): Query string to be executed.
            count (bool | None): If set to `True`, the total document count is
                calculated and included in the result cursor.
            batch_size (int | None): Maximum number of result documents to be
                transferred from the server to the client in one roundtrip.
            bind_vars (dict | None): An object with key/value pairs representing
                the bind parameters.
            cache (bool | None): Flag to determine whether the AQL query results
                cache shall be used.
            memory_limit (int | None): Maximum memory (in bytes) that the query is
                allowed to use.
            ttl (int | None): The time-to-live for the cursor (in seconds). The cursor
                will be removed on the server automatically after the specified amount
                of time.
            allow_dirty_read (bool | None): Allow reads from followers in a cluster.
            options (QueryProperties | dict | None): Extra options for the query.

        References:
            - `create-a-cursor <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#create-a-cursor>`__
        """  # noqa: E501
        data: Json = dict(query=query)
        if count is not None:
            data["count"] = count
        if batch_size is not None:
            data["batchSize"] = batch_size
        if bind_vars is not None:
            data["bindVars"] = bind_vars
        if cache is not None:
            data["cache"] = cache
        if memory_limit is not None:
            data["memoryLimit"] = memory_limit
        if ttl is not None:
            data["ttl"] = ttl
        if options is not None:
            if isinstance(options, QueryProperties):
                options = options.to_dict()
            data["options"] = options

        headers = dict()
        if allow_dirty_read is not None:
            headers["x-arango-allow-dirty-read"] = str(allow_dirty_read).lower()

        request = Request(
            method=Method.POST,
            endpoint="/_api/cursor",
            data=self.serializer.dumps(data),
            headers=headers,
        )

        def response_handler(resp: Response) -> Cursor:
            if not resp.is_success:
                raise AQLQueryExecuteError(resp, request)
            return Cursor(self._executor, self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)
