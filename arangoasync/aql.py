__all__ = ["AQL", "AQLQueryCache"]


from typing import Optional, cast

from arangoasync.cursor import Cursor
from arangoasync.errno import HTTP_NOT_FOUND
from arangoasync.exceptions import (
    AQLCacheClearError,
    AQLCacheConfigureError,
    AQLCacheEntriesError,
    AQLCachePropertiesError,
    AQLFunctionCreateError,
    AQLFunctionDeleteError,
    AQLFunctionListError,
    AQLQueryClearError,
    AQLQueryExecuteError,
    AQLQueryExplainError,
    AQLQueryKillError,
    AQLQueryListError,
    AQLQueryRulesGetError,
    AQLQueryTrackingGetError,
    AQLQueryTrackingSetError,
    AQLQueryValidateError,
)
from arangoasync.executor import ApiExecutor, DefaultApiExecutor, NonAsyncExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    Json,
    Jsons,
    QueryCacheProperties,
    QueryExplainOptions,
    QueryProperties,
    QueryTrackingConfiguration,
)


class AQLQueryCache:
    """AQL Query Cache API wrapper.

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
        return f"<AQLQueryCache in {self.name}>"

    async def entries(self) -> Result[Jsons]:
        """Return a list of all AQL query results cache entries.


        Returns:
            list: List of AQL query results cache entries.

        Raises:
            AQLCacheEntriesError: If retrieval fails.

        References:
            - `list-the-entries-of-the-aql-query-results-cache <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#list-the-entries-of-the-aql-query-results-cache>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/query-cache/entries")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLCacheEntriesError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def plan_entries(self) -> Result[Jsons]:
        """Return a list of all AQL query plan cache entries.

        Returns:
            list: List of AQL query plan cache entries.

        Raises:
            AQLCacheEntriesError: If retrieval fails.

        References:
            - `list-the-entries-of-the-aql-query-plan-cache <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-plan-cache/#list-the-entries-of-the-aql-query-plan-cache>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/query-plan-cache")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLCacheEntriesError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def clear(self) -> Result[None]:
        """Clear the AQL query results cache.

        Raises:
            AQLCacheClearError: If clearing the cache fails.

        References:
            - `clear-the-aql-query-results-cache <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#clear-the-aql-query-results-cache>`__
        """  # noqa: E501
        request = Request(method=Method.DELETE, endpoint="/_api/query-cache")

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise AQLCacheClearError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def clear_plan(self) -> Result[None]:
        """Clear the AQL query plan cache.

        Raises:
            AQLCacheClearError: If clearing the cache fails.

        References:
            - `clear-the-aql-query-plan-cache <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-plan-cache/#clear-the-aql-query-plan-cache>`__
        """  # noqa: E501
        request = Request(method=Method.DELETE, endpoint="/_api/query-plan-cache")

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise AQLCacheClearError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def properties(self) -> Result[QueryCacheProperties]:
        """Return the current AQL query results cache configuration.

        Returns:
            QueryCacheProperties: Current AQL query cache properties.

        Raises:
            AQLCachePropertiesError: If retrieval fails.

        References:
            - `get-the-aql-query-results-cache-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#get-the-aql-query-results-cache-configuration>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/query-cache/properties")

        def response_handler(resp: Response) -> QueryCacheProperties:
            if not resp.is_success:
                raise AQLCachePropertiesError(resp, request)
            return QueryCacheProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def configure(
        self,
        mode: Optional[str] = None,
        max_results: Optional[int] = None,
        max_results_size: Optional[int] = None,
        max_entry_size: Optional[int] = None,
        include_system: Optional[bool] = None,
    ) -> Result[QueryCacheProperties]:
        """Configure the AQL query results cache.

        Args:
            mode (str | None): Cache mode. Allowed values are `"off"`, `"on"`,
                and `"demand"`.
            max_results (int | None): Max number of query results stored per
                database-specific cache.
            max_results_size (int | None): Max cumulative size of query results stored
                per database-specific cache.
            max_entry_size (int | None): Max entry size of each query result stored per
                database-specific cache.
            include_system (bool | None): Store results of queries in system collections.

        Returns:
            QueryCacheProperties: Updated AQL query cache properties.

        Raises:
            AQLCacheConfigureError: If setting the configuration fails.

        References:
            - `set-the-aql-query-results-cache-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-query-results-cache/#set-the-aql-query-results-cache-configuration>`__
        """  # noqa: E501
        data: Json = dict()
        if mode is not None:
            data["mode"] = mode
        if max_results is not None:
            data["maxResults"] = max_results
        if max_results_size is not None:
            data["maxResultsSize"] = max_results_size
        if max_entry_size is not None:
            data["maxEntrySize"] = max_entry_size
        if include_system is not None:
            data["includeSystem"] = include_system

        request = Request(
            method=Method.PUT,
            endpoint="/_api/query-cache/properties",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> QueryCacheProperties:
            if not resp.is_success:
                raise AQLCacheConfigureError(resp, request)
            return QueryCacheProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)


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
    def context(self) -> str:
        """Return the current API execution context."""
        return self._executor.context

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    @property
    def cache(self) -> AQLQueryCache:
        """Return the AQL Query Cache API wrapper."""
        return AQLQueryCache(self._executor)

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

        Returns:
            Cursor: Result cursor.

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
            if self._executor.context == "async":
                # We cannot have a cursor giving back async jobs
                executor: NonAsyncExecutor = DefaultApiExecutor(
                    self._executor.connection
                )
            else:
                executor = cast(NonAsyncExecutor, self._executor)
            return Cursor(executor, self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def tracking(self) -> Result[QueryTrackingConfiguration]:
        """Returns the current query tracking configuration.

        Returns:
            QueryTrackingConfiguration: Returns the current query tracking configuration.

        Raises:
            AQLQueryTrackingGetError: If retrieval fails.

        References:
            - `get-the-aql-query-tracking-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#get-the-aql-query-tracking-configuration>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/query/properties")

        def response_handler(resp: Response) -> QueryTrackingConfiguration:
            if not resp.is_success:
                raise AQLQueryTrackingGetError(resp, request)
            return QueryTrackingConfiguration(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def set_tracking(
        self,
        enabled: Optional[bool] = None,
        max_slow_queries: Optional[int] = None,
        slow_query_threshold: Optional[int] = None,
        max_query_string_length: Optional[int] = None,
        track_bind_vars: Optional[bool] = None,
        track_slow_queries: Optional[int] = None,
    ) -> Result[QueryTrackingConfiguration]:
        """Configure AQL query tracking properties.

        Args:
            enabled (bool | None): If set to `True`, then queries will be tracked.
                If set to `False`, neither queries nor slow queries will be tracked.
            max_slow_queries (int | None): Maximum number of slow queries to track. Oldest
                entries are discarded first.
            slow_query_threshold (int | None): Runtime threshold (in seconds) for treating a
                query as slow.
            max_query_string_length (int | None): The maximum query string length (in bytes)
                to keep in the list of queries.
            track_bind_vars (bool | None): If set to `True`, track bind variables used in
                queries.
            track_slow_queries (int | None): If set to `True`, then slow queries will be
                tracked in the list of slow queries if their runtime exceeds the
                value set in `slowQueryThreshold`.

        Returns:
            QueryTrackingConfiguration: Returns the updated query tracking configuration.

        Raises:
            AQLQueryTrackingSetError: If setting the configuration fails.

        References:
            - `update-the-aql-query-tracking-configuration <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#update-the-aql-query-tracking-configuration>`__
        """  # noqa: E501
        data: Json = dict()

        if enabled is not None:
            data["enabled"] = enabled
        if max_slow_queries is not None:
            data["maxSlowQueries"] = max_slow_queries
        if max_query_string_length is not None:
            data["maxQueryStringLength"] = max_query_string_length
        if slow_query_threshold is not None:
            data["slowQueryThreshold"] = slow_query_threshold
        if track_bind_vars is not None:
            data["trackBindVars"] = track_bind_vars
        if track_slow_queries is not None:
            data["trackSlowQueries"] = track_slow_queries

        request = Request(
            method=Method.PUT,
            endpoint="/_api/query/properties",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> QueryTrackingConfiguration:
            if not resp.is_success:
                raise AQLQueryTrackingSetError(resp, request)
            return QueryTrackingConfiguration(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def queries(self, all_queries: bool = False) -> Result[Jsons]:
        """Return a list of currently running queries.

        Args:
            all_queries (bool): If set to `True`, will return the currently
                running queries in all databases, not just the selected one.
                Using the parameter is only allowed in the `_system` database
                and with superuser privileges.

        Returns:
            list: List of currently running queries and their properties.

        Raises:
            AQLQueryListError: If retrieval fails.

        References:
            - `list-the-running-queries <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#list-the-running-queries>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/query/current",
            params={"all": all_queries},
        )

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLQueryListError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def slow_queries(self, all_queries: bool = False) -> Result[Jsons]:
        """Returns a list containing the last AQL queries that are finished and
        have exceeded the slow query threshold in the selected database.

        Args:
            all_queries (bool): If set to `True`, will return the slow queries
                in all databases, not just the selected one. Using the parameter
                is only allowed in the `_system` database and with superuser privileges.

        Returns:
            list: List of slow queries.

        Raises:
            AQLQueryListError: If retrieval fails.

        References:
            - `list-the-slow-aql-queries <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#list-the-slow-aql-queries>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/query/slow",
            params={"all": all_queries},
        )

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLQueryListError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def clear_slow_queries(self, all_queries: bool = False) -> Result[None]:
        """Clears the list of slow queries.

        Args:
            all_queries (bool): If set to `True`, will clear the slow queries
                in all databases, not just the selected one. Using the parameter
                is only allowed in the `_system` database and with superuser privileges.

        Returns:
            dict: Empty dictionary.

        Raises:
            AQLQueryClearError: If retrieval fails.

        References:
            - `clear-the-list-of-slow-aql-queries <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#clear-the-list-of-slow-aql-queries>`__
        """  # noqa: E501
        request = Request(
            method=Method.DELETE,
            endpoint="/_api/query/slow",
            params={"all": all_queries},
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise AQLQueryClearError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def kill(
        self,
        query_id: str,
        ignore_missing: bool = False,
        all_queries: bool = False,
    ) -> Result[bool]:
        """Kill a running query.

        Args:
            query_id (str): Thea ID of the query to kill.
            ignore_missing (bool): If set to `True`, will not raise an exception
                if the query is not found.
            all_queries (bool): If set to `True`, will kill the query in all databases,
                not just the selected one. Using the parameter is only allowed in the
                `_system` database and with superuser privileges.

        Returns:
            bool: `True` if the query was killed successfully.

        Raises:
            AQLQueryKillError: If killing the query fails.

        References:
            - `kill-a-running-aql-query <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#kill-a-running-aql-query>`__
        """  # noqa: E501
        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/query/{query_id}",
            params={"all": all_queries},
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            if resp.status_code == HTTP_NOT_FOUND and ignore_missing:
                return False
            raise AQLQueryKillError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def explain(
        self,
        query: str,
        bind_vars: Optional[Json] = None,
        options: Optional[QueryExplainOptions | Json] = None,
    ) -> Result[Json]:
        """Inspect the query and return its metadata without executing it.

        Args:
            query (str): Query string to be explained.
            bind_vars (dict | None): An object with key/value pairs representing
                the bind parameters.
            options (QueryExplainOptions | dict | None): Extra options for the query.

        Returns:
            dict: Query execution plan.

        Raises:
            AQLQueryExplainError: If retrieval fails.

        References:
            - `explain-an-aql-query <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#explain-an-aql-query>`__
        """  # noqa: E501
        data: Json = dict(query=query)
        if bind_vars is not None:
            data["bindVars"] = bind_vars
        if options is not None:
            if isinstance(options, QueryExplainOptions):
                options = options.to_dict()
            data["options"] = options

        request = Request(
            method=Method.POST,
            endpoint="/_api/explain",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise AQLQueryExplainError(resp, request)
            return self.deserializer.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def validate(self, query: str) -> Result[Json]:
        """Parse and validate the query without executing it.

        Args:
            query (str): Query string to be validated.

        Returns:
            dict: Query information.

        Raises:
            AQLQueryValidateError: If validation fails.

        References:
            - `parse-an-aql-query <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#parse-an-aql-query>`__
        """  # noqa: E501
        request = Request(
            method=Method.POST,
            endpoint="/_api/query",
            data=self.serializer.dumps(dict(query=query)),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise AQLQueryValidateError(resp, request)
            return self.deserializer.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def query_rules(self) -> Result[Jsons]:
        """A list of all optimizer rules and their properties.

        Returns:
            list: Available optimizer rules.

        Raises:
            AQLQueryRulesGetError: If retrieval fails.

        References:
            - `list-all-aql-optimizer-rules <https://docs.arangodb.com/stable/develop/http-api/queries/aql-queries/#list-all-aql-optimizer-rules>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint="/_api/query/rules")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLQueryRulesGetError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def functions(self, namespace: Optional[str] = None) -> Result[Jsons]:
        """List the registered used-defined AQL functions.

        Args:
            namespace (str | None): Returns all registered AQL user functions from
                the specified namespace.

        Returns:
            list: List of the AQL functions defined in the database.

        Raises:
            AQLFunctionListError: If retrieval fails.

        References:
            - `list-the-registered-user-defined-aql-functions <https://docs.arangodb.com/stable/develop/http-api/queries/user-defined-aql-functions/#list-the-registered-user-defined-aql-functions>`__
        """  # noqa: E501
        params: Json = dict()
        if namespace is not None:
            params["namespace"] = namespace
        request = Request(
            method=Method.GET,
            endpoint="/_api/aqlfunction",
            params=params,
        )

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise AQLFunctionListError(resp, request)
            result = cast(Jsons, self.deserializer.loads(resp.raw_body).get("result"))
            if result is None:
                raise AQLFunctionListError(resp, request)
            return result

        return await self._executor.execute(request, response_handler)

    async def create_function(
        self,
        name: str,
        code: str,
        is_deterministic: Optional[bool] = None,
    ) -> Result[Json]:
        """Registers a user-defined AQL function (UDF) written in JavaScript.

        Args:
            name (str): Name of the function.
            code (str): JavaScript code of the function.
            is_deterministic (bool | None): If set to `True`, the function is
                deterministic.

        Returns:
            dict: Information about the registered function.

        Raises:
            AQLFunctionCreateError: If registration fails.

        References:
            - `create-a-user-defined-aql-function <https://docs.arangodb.com/stable/develop/http-api/queries/user-defined-aql-functions/#create-a-user-defined-aql-function>`__
        """  # noqa: E501
        request = Request(
            method=Method.POST,
            endpoint="/_api/aqlfunction",
            data=self.serializer.dumps(
                dict(name=name, code=code, isDeterministic=is_deterministic)
            ),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise AQLFunctionCreateError(resp, request)
            return self.deserializer.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def delete_function(
        self,
        name: str,
        group: Optional[bool] = None,
        ignore_missing: bool = False,
    ) -> Result[Json]:
        """Remove a user-defined AQL function.

        Args:
            name (str): Name of the function.
            group (bool | None): If set to `True`, the function name is treated
                as a namespace prefix.
            ignore_missing (bool): If set to `True`, will not raise an exception
                if the function is not found.

        Returns:
            dict: Information about the removed functions (their count).

        Raises:
            AQLFunctionDeleteError: If removal fails.

        References:
            - `remove-a-user-defined-aql-function <https://docs.arangodb.com/stable/develop/http-api/queries/user-defined-aql-functions/#remove-a-user-defined-aql-function>`__
        """  # noqa: E501
        params: Json = dict()
        if group is not None:
            params["group"] = group
        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/aqlfunction/{name}",
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                if not (resp.status_code == HTTP_NOT_FOUND and ignore_missing):
                    raise AQLFunctionDeleteError(resp, request)
            return self.deserializer.loads(resp.raw_body)

        return await self._executor.execute(request, response_handler)
