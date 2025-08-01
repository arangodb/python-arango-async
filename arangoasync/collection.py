__all__ = [
    "Collection",
    "EdgeCollection",
    "StandardCollection",
    "VertexCollection",
]


from typing import Any, Generic, List, Literal, Optional, Sequence, TypeVar, cast

from arangoasync.cursor import Cursor
from arangoasync.errno import (
    DOCUMENT_NOT_FOUND,
    HTTP_BAD_PARAMETER,
    HTTP_NOT_FOUND,
    HTTP_PRECONDITION_FAILED,
)
from arangoasync.exceptions import (
    CollectionChecksumError,
    CollectionCompactError,
    CollectionConfigureError,
    CollectionPropertiesError,
    CollectionRecalculateCountError,
    CollectionRenameError,
    CollectionResponsibleShardError,
    CollectionRevisionError,
    CollectionShardsError,
    CollectionStatisticsError,
    CollectionTruncateError,
    DocumentCountError,
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    EdgeListError,
    IndexCreateError,
    IndexDeleteError,
    IndexGetError,
    IndexListError,
    IndexLoadError,
    SortValidationError,
)
from arangoasync.executor import ApiExecutor, DefaultApiExecutor, NonAsyncExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    CollectionInfo,
    CollectionProperties,
    CollectionStatistics,
    IndexProperties,
    Json,
    Jsons,
    Params,
    RequestHeaders,
)

T = TypeVar("T")  # Serializer type
U = TypeVar("U")  # Deserializer loads
V = TypeVar("V")  # Deserializer loads_many


class Collection(Generic[T, U, V]):
    """Base class for collection API wrappers.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        self._executor = executor
        self._name = name
        self._doc_serializer = doc_serializer
        self._doc_deserializer = doc_deserializer
        self._id_prefix = f"{self._name}/"

    @staticmethod
    def get_col_name(doc: str | Json) -> str:
        """Extract the collection name from the document.

        Args:
            doc (str | dict): Document ID or body with "_id" field.

        Returns:
            str: Collection name.

        Raises:
            DocumentParseError: If document ID is missing.
        """
        try:
            doc_id: str = doc if isinstance(doc, str) else doc["_id"]
        except KeyError:
            raise DocumentParseError('field "_id" required')
        else:
            return doc_id.split("/", 1)[0]

    def _validate_id(self, doc_id: str) -> str:
        """Check the collection name in the document ID.

        Args:
            doc_id (str): Document ID.

        Returns:
            str: Verified document ID.

        Raises:
            DocumentParseError: On bad collection name.
        """
        if not doc_id.startswith(self._id_prefix):
            raise DocumentParseError(f'Bad collection name in document ID "{doc_id}"')
        return doc_id

    def _extract_id(self, body: Json, validate: bool = True) -> str:
        """Extract the document ID from document body.

        Args:
            body (dict): Document body.
            validate (bool): Whether to validate the document ID,
                checking if it belongs to the current collection.

        Returns:
            str: Document ID.

        Raises:
            DocumentParseError: On missing ID and key.
        """
        try:
            if "_id" in body:
                if validate:
                    return self._validate_id(body["_id"])
                else:
                    return cast(str, body["_id"])
            else:
                key: str = body["_key"]
                return self._id_prefix + key
        except KeyError:
            raise DocumentParseError('Field "_key" or "_id" required')

    def _ensure_key_from_id(self, body: Json) -> Json:
        """Return the body with "_key" field if it has "_id" field.

        Args:
            body (dict): Document body.

        Returns:
            dict: Document body with "_key" field if it has "_id" field.

        Raises:
            DocumentParseError: If document is malformed.
        """
        if "_id" in body and "_key" not in body:
            doc_id = self._validate_id(body["_id"])
            body = body.copy()
            body["_key"] = doc_id[len(self._id_prefix) :]
        return body

    def _get_doc_id(self, document: str | Json, validate: bool = True) -> str:
        """Prepare document ID before a query.

        Args:
            document (str | dict): Document ID, key or body.
            validate (bool): Whether to validate the document ID,
                checking if it belongs to the current collection.

        Returns:
            Document ID and request headers.

        Raises:
            DocumentParseError: On missing ID and key.
        """
        if isinstance(document, str):
            if "/" in document:
                if validate:
                    doc_id = self._validate_id(document)
                else:
                    doc_id = document
            else:
                doc_id = self._id_prefix + document
        else:
            doc_id = self._extract_id(document, validate)

        return doc_id

    def _build_filter_conditions(self, filters: Optional[Json]) -> str:
        """Build filter conditions for an AQL query.

        Args:
            filters (dict | None): Document filters.

        Returns:
            str: The complete AQL filter condition.
        """
        if not filters:
            return ""

        conditions = []
        for k, v in filters.items():
            field = k if "." in k else f"`{k}`"
            conditions.append(f"doc.{field} == {self.serializer.dumps(v)}")

        return "FILTER " + " AND ".join(conditions)

    @staticmethod
    def _is_none_or_int(obj: Any) -> bool:
        """Check if obj is `None` or a positive integer.

        Args:
            obj: Object to check.

        Returns:
            bool: `True` if object is `None` or a positive integer.
        """
        return obj is None or isinstance(obj, int) and obj >= 0

    @staticmethod
    def _is_none_or_dict(obj: Any) -> bool:
        """Check if obj is `None` or a dict.

        Args:
            obj: Object to check.

        Returns:
            bool: `True` if object is `None` or a dict.
        """
        return obj is None or isinstance(obj, dict)

    @staticmethod
    def _validate_sort_parameters(sort: Optional[Jsons]) -> None:
        """Validate sort parameters for an AQL query.

        Args:
            sort (list | None): Document sort parameters.

        Raises:
            SortValidationError: If sort parameters are invalid.
        """
        if not sort:
            return

        for param in sort:
            if "sort_by" not in param or "sort_order" not in param:
                raise SortValidationError(
                    "Each sort parameter must have 'sort_by' and 'sort_order'."
                )
            if param["sort_order"].upper() not in ["ASC", "DESC"]:
                raise SortValidationError("'sort_order' must be either 'ASC' or 'DESC'")

    @staticmethod
    def _build_sort_expression(sort: Optional[Jsons]) -> str:
        """Build a sort condition for an AQL query.

        Args:
            sort (list | None): Document sort parameters.

        Returns:
            str: The complete AQL sort condition.
        """
        if not sort:
            return ""

        sort_chunks = []
        for sort_param in sort:
            chunk = f"doc.{sort_param['sort_by']} {sort_param['sort_order']}"
            sort_chunks.append(chunk)

        return "SORT " + ", ".join(sort_chunks)

    @property
    def name(self) -> str:
        """Return the name of the collection.

        Returns:
            str: Collection name.
        """
        return self._name

    @property
    def context(self) -> str:
        """Return the context of the collection.

        Returns:
            str: Context.
        """
        return self._executor.context

    @property
    def db_name(self) -> str:
        """Return the name of the current database.

        Returns:
            str: Database name.
        """
        return self._executor.db_name

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    async def indexes(
        self,
        with_stats: Optional[bool] = None,
        with_hidden: Optional[bool] = None,
    ) -> Result[List[IndexProperties]]:
        """Fetch all index descriptions for the given collection.

        Args:
            with_stats (bool | None): Whether to include figures and estimates in the result.
            with_hidden (bool | None): Whether to include hidden indexes in the result.

        Returns:
            list: List of index properties.

        Raises:
            IndexListError: If retrieval fails.

        References:
            - `list-all-indexes-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/indexes/#list-all-indexes-of-a-collection>`__
        """  # noqa: E501
        params: Params = dict(collection=self._name)
        if with_stats is not None:
            params["withStats"] = with_stats
        if with_hidden is not None:
            params["withHidden"] = with_hidden

        request = Request(
            method=Method.GET,
            endpoint="/_api/index",
            params=params,
        )

        def response_handler(resp: Response) -> List[IndexProperties]:
            if not resp.is_success:
                raise IndexListError(resp, request)
            data = self.deserializer.loads(resp.raw_body)
            return [IndexProperties(item) for item in data["indexes"]]

        return await self._executor.execute(request, response_handler)

    async def get_index(self, id: str | int) -> Result[IndexProperties]:
        """Return the properties of an index.

        Args:
            id (str): Index ID. Could be either the full ID or just the index number.

        Returns:
            IndexProperties: Index properties.

        Raises:
            IndexGetError: If retrieval fails.

        References:
            `get-an-index <https://docs.arangodb.com/stable/develop/http-api/indexes/#get-an-index>`__
        """  # noqa: E501
        if isinstance(id, int):
            full_id = f"{self._name}/{id}"
        else:
            full_id = id if "/" in id else f"{self._name}/{id}"

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/index/{full_id}",
        )

        def response_handler(resp: Response) -> IndexProperties:
            if not resp.is_success:
                raise IndexGetError(resp, request)
            return IndexProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def add_index(
        self,
        type: str,
        fields: Json | List[str],
        options: Optional[Json] = None,
    ) -> Result[IndexProperties]:
        """Create an index.

        Args:
            type (str): Type attribute (ex. "persistent", "inverted", "ttl", "mdi",
                "geo").
            fields (dict | list): Fields to index.
            options (dict | None): Additional index options.

        Returns:
            IndexProperties: New index properties.

        Raises:
            IndexCreateError: If index creation fails.

        References:
            - `create-an-index <https://docs.arangodb.com/stable/develop/http-api/indexes/#create-an-index>`__
            - `create-a-persistent-index <https://docs.arangodb.com/stable/develop/http-api/indexes/persistent/#create-a-persistent-index>`__
            - `create-an-inverted-index <https://docs.arangodb.com/stable/develop/http-api/indexes/inverted/#create-an-inverted-index>`__
            - `create-a-ttl-index <https://docs.arangodb.com/stable/develop/http-api/indexes/ttl/#create-a-ttl-index>`__
            - `create-a-multi-dimensional-index <https://docs.arangodb.com/stable/develop/http-api/indexes/multi-dimensional/#create-a-multi-dimensional-index>`__
            - `create-a-geo-spatial-index <https://docs.arangodb.com/stable/develop/http-api/indexes/geo-spatial/#create-a-geo-spatial-index>`__
        """  # noqa: E501
        options = options or {}
        request = Request(
            method=Method.POST,
            endpoint="/_api/index",
            data=self.serializer.dumps(dict(type=type, fields=fields) | options),
            params=dict(collection=self._name),
        )

        def response_handler(resp: Response) -> IndexProperties:
            if not resp.is_success:
                raise IndexCreateError(resp, request)
            return IndexProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def delete_index(
        self, id: str | int, ignore_missing: bool = False
    ) -> Result[bool]:
        """Delete an index.

        Args:
            id (str): Index ID. Could be either the full ID or just the index number.
            ignore_missing (bool): Do not raise an exception on missing index.

        Returns:
            bool: `True` if the operation was successful. `False` if the index was not
                found and **ignore_missing** was set to `True`.

        Raises:
            IndexDeleteError: If deletion fails.

        References:
            - `delete-an-index <https://docs.arangodb.com/stable/develop/http-api/indexes/#delete-an-index>`__
        """  # noqa: E501
        if isinstance(id, int):
            full_id = f"{self._name}/{id}"
        else:
            full_id = id if "/" in id else f"{self._name}/{id}"

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/index/{full_id}",
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            elif ignore_missing and resp.status_code == HTTP_NOT_FOUND:
                return False
            raise IndexDeleteError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def load_indexes(self) -> Result[bool]:
        """Cache this collection’s index entries in the main memory.

        Returns:
            bool: `True` if the operation was successful.

        Raises:
            IndexLoadError: If loading fails.

        References:
            - `load-collection-indexes-into-memory <https://docs.arangodb.com/stable/develop/http-api/collections/#load-collection-indexes-into-memory>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self._name}/loadIndexesIntoMemory",
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            raise IndexLoadError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def recalculate_count(self) -> None:
        """Recalculate the document count.

        Raises:
            CollectionRecalculateCountError: If re-calculation fails.

        References:
            - `recalculate-the-document-count-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#recalculate-the-document-count-of-a-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/recalculateCount",
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise CollectionRecalculateCountError(resp, request)

        await self._executor.execute(request, response_handler)

    async def properties(self) -> Result[CollectionProperties]:
        """Return the full properties of the current collection.

        Returns:
            CollectionProperties: Properties.

        Raises:
            CollectionPropertiesError: If retrieval fails.

        References:
            - `get-the-properties-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-properties-of-a-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/collection/{self.name}/properties",
        )

        def response_handler(resp: Response) -> CollectionProperties:
            if not resp.is_success:
                raise CollectionPropertiesError(resp, request)
            return CollectionProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def configure(
        self,
        cache_enabled: Optional[bool] = None,
        computed_values: Optional[Jsons] = None,
        replication_factor: Optional[int | str] = None,
        schema: Optional[Json] = None,
        wait_for_sync: Optional[bool] = None,
        write_concern: Optional[int] = None,
    ) -> Result[CollectionProperties]:
        """Changes the properties of a collection.

        Only the provided attributes are updated.

        Args:
            cache_enabled (bool | None): Whether the in-memory hash cache
                for documents should be enabled for this collection.
            computed_values (list | None): An optional list of objects, each
                representing a computed value.
            replication_factor (int | None): In a cluster, this attribute determines
                how many copies of each shard are kept on different DB-Servers.
                For SatelliteCollections, it needs to be the string "satellite".
            schema (dict | None): The configuration of the collection-level schema
                validation for documents.
            wait_for_sync (bool | None): If set to `True`, the data is synchronized
                to disk before returning from a document create, update, replace or
                removal operation.
            write_concern (int | None): Determines how many copies of each shard are
                required to be in sync on the different DB-Servers.

        Returns:
            CollectionProperties: Properties.

        Raises:
            CollectionConfigureError: If configuration fails.

        References:
            - `change-the-properties-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#change-the-properties-of-a-collection>`__
        """  # noqa: E501
        data: Json = {}
        if cache_enabled is not None:
            data["cacheEnabled"] = cache_enabled
        if computed_values is not None:
            data["computedValues"] = computed_values
        if replication_factor is not None:
            data["replicationFactor"] = replication_factor
        if schema is not None:
            data["schema"] = schema
        if wait_for_sync is not None:
            data["waitForSync"] = wait_for_sync
        if write_concern is not None:
            data["writeConcern"] = write_concern
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/properties",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> CollectionProperties:
            if not resp.is_success:
                raise CollectionConfigureError(resp, request)
            return CollectionProperties(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def rename(self, new_name: str) -> None:
        """Rename the collection.

        Renames may not be reflected immediately in async execution, batch
        execution or transactions. It is recommended to initialize new API
        wrappers after a rename.

        Note:
            Renaming collections is not supported in cluster deployments.

        Args:
            new_name (str): New collection name.

        Raises:
            CollectionRenameError: If rename fails.

        References:
            - `rename-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#rename-a-collection>`__
        """  # noqa: E501
        data: Json = {"name": new_name}
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/rename",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise CollectionRenameError(resp, request)
            self._name = new_name
            self._id_prefix = f"{new_name}/"

        await self._executor.execute(request, response_handler)

    async def compact(self) -> Result[CollectionInfo]:
        """Compact a collection.

        Returns:
            CollectionInfo: Collection information.

        Raises:
            CollectionCompactError: If compaction fails.

        References:
            - `compact-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#compact-a-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/compact",
        )

        def response_handler(resp: Response) -> CollectionInfo:
            if not resp.is_success:
                raise CollectionCompactError(resp, request)
            return CollectionInfo(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def truncate(
        self,
        wait_for_sync: Optional[bool] = None,
        compact: Optional[bool] = None,
    ) -> None:
        """Removes all documents, but leaves indexes intact.

        Args:
            wait_for_sync (bool | None): If set to `True`, the data is synchronized
                to disk before returning from the truncate operation.
            compact (bool | None): If set to `True`, the storage engine is told to
                start a compaction in order to free up disk space. This can be
                resource intensive. If the only intention is to start over with an
                empty collection, specify `False`.

        Raises:
            CollectionTruncateError: If truncation fails.

        References:
            - `truncate-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#truncate-a-collection>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if compact is not None:
            params["compact"] = compact

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/truncate",
            params=params,
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise CollectionTruncateError(resp, request)

        await self._executor.execute(request, response_handler)

    async def count(self) -> Result[int]:
        """Return the total document count.

        Returns:
            int: Total document count.

        Raises:
            DocumentCountError: If retrieval fails.

        References:
            - `get-the-document-count-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-document-count-of-a-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET, endpoint=f"/_api/collection/{self.name}/count"
        )

        def response_handler(resp: Response) -> int:
            if resp.is_success:
                result: int = self.deserializer.loads(resp.raw_body)["count"]
                return result
            raise DocumentCountError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def statistics(self) -> Result[CollectionStatistics]:
        """Get additional statistical information about the collection.

        Returns:
            CollectionStatistics: Collection statistics.

        Raises:
            CollectionStatisticsError: If retrieval fails.

        References:
            - `get-the-collection-statistics <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-collection-statistics>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/collection/{self.name}/figures",
        )

        def response_handler(resp: Response) -> CollectionStatistics:
            if not resp.is_success:
                raise CollectionStatisticsError(resp, request)
            return CollectionStatistics(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def responsible_shard(self, document: Json) -> Result[str]:
        """Return the ID of the shard responsible for given document.

        If the document does not exist, return the shard that would be
        responsible.

        Args:
            document (dict): Document body with "_key" field.

        Returns:
            str: Shard ID.

        Raises:
            CollectionResponsibleShardError: If retrieval fails.

        References:
            - `get-the-responsible-shard-for-a-document <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-responsible-shard-for-a-document>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/collection/{self.name}/responsibleShard",
            data=self.serializer.dumps(document),
        )

        def response_handler(resp: Response) -> str:
            if resp.is_success:
                body = self.deserializer.loads(resp.raw_body)
                return cast(str, body["shardId"])
            raise CollectionResponsibleShardError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def shards(self, details: Optional[bool] = None) -> Result[Json]:
        """Return collection shards and properties.

        Available only in a cluster setup.

        Args:
            details (bool | None): If set to `True`, include responsible
                servers for these shards.

        Returns:
            dict: Collection shards.

        Raises:
            CollectionShardsError: If retrieval fails.

        References:
            - `get-the-shard-ids-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-shard-ids-of-a-collection>`__
        """  # noqa: E501
        params: Params = {}
        if details is not None:
            params["details"] = details

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/collection/{self.name}/shards",
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise CollectionShardsError(resp, request)
            return cast(Json, self.deserializer.loads(resp.raw_body)["shards"])

        return await self._executor.execute(request, response_handler)

    async def revision(self) -> Result[str]:
        """Return collection revision.

        Returns:
            str: Collection revision.

        Raises:
            CollectionRevisionError: If retrieval fails.

        References:
            - `get-the-collection-revision-id <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-collection-revision-id>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/collection/{self.name}/revision",
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise CollectionRevisionError(resp, request)
            return cast(str, self.deserializer.loads(resp.raw_body)["revision"])

        return await self._executor.execute(request, response_handler)

    async def checksum(
        self, with_rev: Optional[bool] = None, with_data: Optional[bool] = None
    ) -> Result[str]:
        """Calculate collection checksum.

        Args:
            with_rev (bool | None): Include document revisions in checksum calculation.
            with_data (bool | None): Include document data in checksum calculation.

        Returns:
            str: Collection checksum.

        Raises:
            CollectionChecksumError: If retrieval fails.

        References:
            - `get-the-collection-checksum <https://docs.arangodb.com/stable/develop/http-api/collections/#get-the-collection-checksum>`__
        """  # noqa: E501
        params: Params = {}
        if with_rev is not None:
            params["withRevision"] = with_rev
        if with_data is not None:
            params["withData"] = with_data

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/collection/{self.name}/checksum",
            params=params,
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise CollectionChecksumError(resp, request)
            return cast(str, self.deserializer.loads(resp.raw_body)["checksum"])

        return await self._executor.execute(request, response_handler)

    async def has(
        self,
        document: str | Json,
        allow_dirty_read: bool = False,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[bool]:
        """Check if a document exists in the collection.

        Args:
            document (str | dict): Document ID, key or body.
                Document body must contain the "_id" or "_key" field.
            allow_dirty_read (bool):  Allow reads from followers in a cluster.
            if_match (str | None): The document is returned, if it has the same
                revision as the given ETag.
            if_none_match (str | None): The document is returned, if it has a
                different revision than the given ETag.

        Returns:
            `True` if the document exists, `False` otherwise.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.

        References:
            - `get-a-document-header <https://docs.arangodb.com/stable/develop/http-api/documents/#get-a-document-header>`__
        """  # noqa: E501
        handle = self._get_doc_id(document)

        headers: RequestHeaders = {}
        if allow_dirty_read:
            headers["x-arango-allow-dirty-read"] = "true"
        if if_match is not None:
            headers["If-Match"] = if_match
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        request = Request(
            method=Method.HEAD,
            endpoint=f"/_api/document/{handle}",
            headers=headers,
        )

        def response_handler(resp: Response) -> bool:
            if resp.is_success:
                return True
            elif resp.status_code == HTTP_NOT_FOUND:
                return False
            elif resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            else:
                raise DocumentGetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def get_many(
        self,
        documents: Sequence[str | T],
        allow_dirty_read: Optional[bool] = None,
        ignore_revs: Optional[bool] = None,
    ) -> Result[V]:
        """Return multiple documents ignoring any missing ones.

        Args:
            documents (list): List of document IDs, keys or bodies. A search document
                must contain at least a value for the `_key` field. A value for `_rev`
                may be specified to verify whether the document has the same revision
                value, unless `ignoreRevs` is set to false.
            allow_dirty_read (bool | None): Allow reads from followers in a cluster.
            ignore_revs (bool | None): If set to `True`, the `_rev` attribute in the
                document is ignored. If this is set to `False`, then the `_rev`
                attribute given in the body document is taken as a precondition.
                The document is only replaced if the current revision is the one
                specified.

        Returns:
            list: List of documents. Missing ones are not included.

        Raises:
            DocumentGetError: If retrieval fails.

        References:
            - `get-multiple-documents <https://docs.arangodb.com/stable/develop/http-api/documents/#get-multiple-documents>`__
        """  # noqa: E501
        params: Params = {"onlyget": True}
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs

        headers: RequestHeaders = {}
        if allow_dirty_read is not None:
            if allow_dirty_read is True:
                headers["x-arango-allow-dirty-read"] = "true"
            else:
                headers["x-arango-allow-dirty-read"] = "false"

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/document/{self.name}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(documents),
        )

        def response_handler(resp: Response) -> V:
            if not resp.is_success:
                raise DocumentGetError(resp, request)
            return self._doc_deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def find(
        self,
        filters: Optional[Json] = None,
        skip: Optional[int] = None,
        limit: Optional[int | str] = None,
        allow_dirty_read: Optional[bool] = False,
        sort: Optional[Jsons] = None,
    ) -> Result[Cursor]:
        """Return all documents that match the given filters.

        Args:
            filters (dict | None): Query filters.
            skip (int | None): Number of documents to skip.
            limit (int | str | None): Maximum number of documents to return.
            allow_dirty_read (bool): Allow reads from followers in a cluster.
            sort (list | None): Document sort parameters.

        Returns:
            Cursor: Document cursor.

        Raises:
            DocumentGetError: If retrieval fails.
            SortValidationError: If sort parameters are invalid.
        """
        if not self._is_none_or_dict(filters):
            raise ValueError("filters parameter must be a dict")
        self._validate_sort_parameters(sort)
        if not self._is_none_or_int(skip):
            raise ValueError("skip parameter must be a non-negative int")
        if not (self._is_none_or_int(limit) or limit == "null"):
            raise ValueError("limit parameter must be a non-negative int")

        skip = skip if skip is not None else 0
        limit = limit if limit is not None else "null"
        query = f"""
            FOR doc IN @@collection
                {self._build_filter_conditions(filters)}
                LIMIT {skip}, {limit}
                {self._build_sort_expression(sort)}
                RETURN doc
        """
        bind_vars = {"@collection": self.name}
        data: Json = {"query": query, "bindVars": bind_vars, "count": True}
        headers: RequestHeaders = {}
        if allow_dirty_read is not None:
            if allow_dirty_read is True:
                headers["x-arango-allow-dirty-read"] = "true"
            else:
                headers["x-arango-allow-dirty-read"] = "false"

        request = Request(
            method=Method.POST,
            endpoint="/_api/cursor",
            data=self.serializer.dumps(data),
            headers=headers,
        )

        def response_handler(resp: Response) -> Cursor:
            if not resp.is_success:
                raise DocumentGetError(resp, request)
            if self._executor.context == "async":
                # We cannot have a cursor giving back async jobs
                executor: NonAsyncExecutor = DefaultApiExecutor(
                    self._executor.connection
                )
            else:
                executor = cast(NonAsyncExecutor, self._executor)
            return Cursor(executor, self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def update_match(
        self,
        filters: Json,
        body: T,
        limit: Optional[int | str] = None,
        keep_none: Optional[bool] = None,
        wait_for_sync: Optional[bool] = None,
        merge_objects: Optional[bool] = None,
    ) -> Result[int]:
        """Update matching documents.

        Args:
            filters (dict | None): Query filters.
            body (dict): Full or partial document body with the updates.
            limit (int | str | None): Maximum number of documents to update.
            keep_none (bool | None): If set to `True`, fields with value `None` are
                retained in the document. Otherwise, they are removed completely.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.
            merge_objects (bool | None): If set to `True`, sub-dictionaries are merged
                instead of the new one overwriting the old one.

        Returns:
            int: Number of documents that got updated.

        Raises:
           DocumentUpdateError: If update fails.
        """
        if not self._is_none_or_dict(filters):
            raise ValueError("filters parameter must be a dict")
        if not (self._is_none_or_int(limit) or limit == "null"):
            raise ValueError("limit parameter must be a non-negative int")

        sync = f", waitForSync: {wait_for_sync}" if wait_for_sync is not None else ""
        query = f"""
            FOR doc IN @@collection
                {self._build_filter_conditions(filters)}
                {f"LIMIT {limit}" if limit is not None else ""}
                UPDATE doc WITH @body IN @@collection
                OPTIONS {{ keepNull: @keep_none, mergeObjects: @merge {sync} }}
        """  # noqa: E201 E202
        bind_vars = {
            "@collection": self.name,
            "body": body,
            "keep_none": keep_none,
            "merge": merge_objects,
        }
        data = {"query": query, "bindVars": bind_vars}

        request = Request(
            method=Method.POST,
            endpoint="/_api/cursor",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> int:
            if resp.is_success:
                result = self.deserializer.loads(resp.raw_body)
                return cast(int, result["extra"]["stats"]["writesExecuted"])
            raise DocumentUpdateError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def replace_match(
        self,
        filters: Json,
        body: T,
        limit: Optional[int | str] = None,
        wait_for_sync: Optional[bool] = None,
    ) -> Result[int]:
        """Replace matching documents.

        Args:
            filters (dict | None): Query filters.
            body (dict): New document body.
            limit (int | str | None): Maximum number of documents to replace.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.

        Returns:
            int: Number of documents that got replaced.

        Raises:
           DocumentReplaceError: If replace fails.
        """
        if not self._is_none_or_dict(filters):
            raise ValueError("filters parameter must be a dict")
        if not (self._is_none_or_int(limit) or limit == "null"):
            raise ValueError("limit parameter must be a non-negative int")

        sync = f"waitForSync: {wait_for_sync}" if wait_for_sync is not None else ""
        query = f"""
            FOR doc IN @@collection
                {self._build_filter_conditions(filters)}
                {f"LIMIT {limit}" if limit is not None else ""}
                REPLACE doc WITH @body IN @@collection
                {f"OPTIONS {{ {sync} }}" if sync else ""}
        """  # noqa: E201 E202
        bind_vars = {
            "@collection": self.name,
            "body": body,
        }
        data = {"query": query, "bindVars": bind_vars}

        request = Request(
            method=Method.POST,
            endpoint="/_api/cursor",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> int:
            if resp.is_success:
                result = self.deserializer.loads(resp.raw_body)
                return cast(int, result["extra"]["stats"]["writesExecuted"])
            raise DocumentReplaceError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def delete_match(
        self,
        filters: Json,
        limit: Optional[int | str] = None,
        wait_for_sync: Optional[bool] = None,
    ) -> Result[int]:
        """Delete matching documents.

        Args:
            filters (dict | None): Query filters.
            limit (int | str | None): Maximum number of documents to delete.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.

        Returns:
            int: Number of documents that got deleted.

        Raises:
           DocumentDeleteError: If delete fails.
        """
        if not self._is_none_or_dict(filters):
            raise ValueError("filters parameter must be a dict")
        if not (self._is_none_or_int(limit) or limit == "null"):
            raise ValueError("limit parameter must be a non-negative int")

        sync = f"waitForSync: {wait_for_sync}" if wait_for_sync is not None else ""
        query = f"""
            FOR doc IN @@collection
                {self._build_filter_conditions(filters)}
                {f"LIMIT {limit}" if limit is not None else ""}
                REMOVE doc IN @@collection
                {f"OPTIONS {{ {sync} }}" if sync else ""}
        """  # noqa: E201 E202
        bind_vars = {"@collection": self.name}
        data = {"query": query, "bindVars": bind_vars}

        request = Request(
            method=Method.POST,
            endpoint="/_api/cursor",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> int:
            if resp.is_success:
                result = self.deserializer.loads(resp.raw_body)
                return cast(int, result["extra"]["stats"]["writesExecuted"])
            raise DocumentDeleteError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def insert_many(
        self,
        documents: Sequence[T],
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        overwrite: Optional[bool] = None,
        overwrite_mode: Optional[str] = None,
        keep_null: Optional[bool] = None,
        merge_objects: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
    ) -> Result[Jsons]:
        """Insert multiple documents.

        Note:
            If inserting a document fails, the exception is not raised but
            returned as an object in the "errors" list. It is up to you to
            inspect the list to determine which documents were inserted
            successfully (returns document metadata) and which were not
            (returns exception object).

        Args:
            documents (list): Documents to insert. If an item contains the "_key" or
                "_id" field, the value is used as the key of the new document
                (otherwise it is auto-generated). Any "_rev" field is ignored.
            wait_for_sync (bool | None): Wait until documents have been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result. Only available if the
                `overwrite` option is used.
            silent (bool | None): If set to `True`, an empty object is returned as
                response if all document operations succeed. No meta-data is returned
                for the created documents. If any of the operations raises an error,
                an array with the error object(s) is returned.
            overwrite (bool | None): If set to `True`, operation does not fail on
                duplicate key and existing document is overwritten (replace-insert).
            overwrite_mode (str | None): Overwrite mode. Supersedes **overwrite**
                option. May be one of "ignore", "replace", "update" or "conflict".
            keep_null (bool | None): If set to `True`, fields with value None are
                retained in the document. Otherwise, they are removed completely.
                Applies only when **overwrite_mode** is set to "update"
                (update-insert).
            merge_objects (bool | None): If set to `True`, sub-dictionaries are merged
                instead of the new one overwriting the old one. Applies only when
                **overwrite_mode** is set to "update" (update-insert).
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document operations affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations. Only applicable if **overwrite** is set to `True`
                or **overwrite_mode** is set to "update" or "replace".

        Returns:
            list: Documents metadata (e.g. document id, key, revision) and
                errors or just errors if **silent** is set to `True`.

        Raises:
            DocumentInsertError: If insertion fails.

        References:
            - `create-multiple-documents <https://docs.arangodb.com/stable/develop/http-api/documents/#create-multiple-documents>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if overwrite is not None:
            params["overwrite"] = overwrite
        if overwrite_mode is not None:
            params["overwriteMode"] = overwrite_mode
        if keep_null is not None:
            params["keepNull"] = keep_null
        if merge_objects is not None:
            params["mergeObjects"] = merge_objects
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/document/{self.name}",
            data=self._doc_serializer.dumps(documents),
            params=params,
        )

        def response_handler(
            resp: Response,
        ) -> Jsons:
            if not resp.is_success:
                raise DocumentInsertError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def replace_many(
        self,
        documents: Sequence[T],
        wait_for_sync: Optional[bool] = None,
        ignore_revs: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
    ) -> Result[Jsons]:
        """Insert multiple documents.

        Note:
            If replacing a document fails, the exception is not raised but
            returned as an object in the "errors" list. It is up to you to
            inspect the list to determine which documents were replaced
            successfully (returns document metadata) and which were not
            (returns exception object).

        Args:
            documents (list): New documents to replace the old ones. An item must
                contain the "_key" or "_id" field.
            wait_for_sync (bool | None): Wait until documents have been synced to disk.
            ignore_revs (bool | None): If this is set to `False`, then any `_rev`
                attribute given in a body document is taken as a precondition. The
                document is only replaced if the current revision is the one
                specified.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, an empty object is returned as
                response if all document operations succeed. No meta-data is returned
                for the created documents. If any of the operations raises an error,
                an array with the error object(s) is returned.
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document operations affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations.

        Returns:
            list: Documents metadata (e.g. document id, key, revision) and
                errors or just errors if **silent** is set to `True`.

        Raises:
            DocumentReplaceError: If replacing fails.

        References:
            - `replace-multiple-documents <https://docs.arangodb.com/stable/develop/http-api/documents/#replace-multiple-documents>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/document/{self.name}",
            data=self._doc_serializer.dumps(documents),
            params=params,
        )

        def response_handler(
            resp: Response,
        ) -> Jsons:
            if not resp.is_success:
                raise DocumentReplaceError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def update_many(
        self,
        documents: Sequence[T],
        wait_for_sync: Optional[bool] = None,
        ignore_revs: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        merge_objects: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
    ) -> Result[Jsons]:
        """Insert multiple documents.

        Note:
            If updating a document fails, the exception is not raised but
            returned as an object in the "errors" list. It is up to you to
            inspect the list to determine which documents were updated
            successfully (returned as document metadata) and which were not
            (returned as exception object).

        Args:
            documents (list): Documents to update. An item must contain the "_key" or
                "_id" field.
            wait_for_sync (bool | None): Wait until documents have been synced to disk.
            ignore_revs (bool | None): If this is set to `False`, then any `_rev`
                attribute given in a body document is taken as a precondition. The
                document is only updated if the current revision is the one
                specified.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, an empty object is returned as
                response if all document operations succeed. No meta-data is returned
                for the created documents. If any of the operations raises an error,
                an array with the error object(s) is returned.
            keep_null (bool | None): If set to `True`, fields with value None are
                retained in the document. Otherwise, they are removed completely.
                Applies only when **overwrite_mode** is set to "update"
                (update-insert).
            merge_objects (bool | None): If set to `True`, sub-dictionaries are merged
                instead of the new one overwriting the old one. Applies only when
                **overwrite_mode** is set to "update" (update-insert).
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document operations affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations.

        Returns:
            list: Documents metadata (e.g. document id, key, revision) and
                errors or just errors if **silent** is set to `True`.

        Raises:
            DocumentUpdateError: If update fails.

        References:
            - `update-multiple-documents <https://docs.arangodb.com/stable/develop/http-api/documents/#update-multiple-documents>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if keep_null is not None:
            params["keepNull"] = keep_null
        if merge_objects is not None:
            params["mergeObjects"] = merge_objects
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/document/{self.name}",
            data=self._doc_serializer.dumps(documents),
            params=params,
        )

        def response_handler(
            resp: Response,
        ) -> Jsons:
            if not resp.is_success:
                raise DocumentUpdateError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)

    async def delete_many(
        self,
        documents: Sequence[T],
        wait_for_sync: Optional[bool] = None,
        ignore_revs: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
    ) -> Result[Jsons]:
        """Delete multiple documents.

        Note:
            If deleting a document fails, the exception is not raised but
            returned as an object in the "errors" list. It is up to you to
            inspect the list to determine which documents were deleted
            successfully (returned as document metadata) and which were not
            (returned as exception object).

        Args:
            documents (list): Documents to delete. An item must contain the "_key" or
                "_id" field.
            wait_for_sync (bool | None): Wait until documents have been synced to disk.
            ignore_revs (bool | None): If this is set to `False`, then any `_rev`
                attribute given in a body document is taken as a precondition. The
                document is only updated if the current revision is the one
                specified.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, an empty object is returned as
                response if all document operations succeed. No meta-data is returned
                for the created documents. If any of the operations raises an error,
                an array with the error object(s) is returned.
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document operations affect the edge index
                or cache-enabled persistent indexes.

        Returns:
            list: Documents metadata (e.g. document id, key, revision) and
                errors or just errors if **silent** is set to `True`.

        Raises:
            DocumentRemoveError: If removal fails.

        References:
            - `remove-multiple-documents <https://docs.arangodb.com/stable/develop/http-api/documents/#remove-multiple-documents>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/document/{self.name}",
            data=self._doc_serializer.dumps(documents),
            params=params,
        )

        def response_handler(
            resp: Response,
        ) -> Jsons:
            if not resp.is_success:
                raise DocumentDeleteError(resp, request)
            return self.deserializer.loads_many(resp.raw_body)

        return await self._executor.execute(request, response_handler)


class StandardCollection(Collection[T, U, V]):
    """Standard collection API wrapper.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        super().__init__(executor, name, doc_serializer, doc_deserializer)

    def __repr__(self) -> str:
        return f"<StandardCollection {self.name}>"

    async def get(
        self,
        document: str | Json,
        allow_dirty_read: bool = False,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[Optional[U]]:
        """Return a document.

        Args:
            document (str | dict): Document ID, key or body.
                Document body must contain the "_id" or "_key" field.
            allow_dirty_read (bool):  Allow reads from followers in a cluster.
            if_match (str | None): The document is returned, if it has the same
                revision as the given ETag.
            if_none_match (str | None): The document is returned, if it has a
                different revision than the given ETag.

        Returns:
            Document or `None` if not found.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.
            DocumentParseError: If the document is malformed.

        References:
            - `get-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#get-a-document>`__
        """  # noqa: E501
        handle = self._get_doc_id(document)

        headers: RequestHeaders = {}
        if allow_dirty_read:
            headers["x-arango-allow-dirty-read"] = "true"
        if if_match is not None:
            headers["If-Match"] = if_match
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/document/{handle}",
            headers=headers,
        )

        def response_handler(resp: Response) -> Optional[U]:
            if resp.is_success:
                return self._doc_deserializer.loads(resp.raw_body)
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND:
                    return None
                else:
                    raise DocumentGetError(resp, request)
            elif resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            else:
                raise DocumentGetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def insert(
        self,
        document: T,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        overwrite: Optional[bool] = None,
        overwrite_mode: Optional[str] = None,
        keep_null: Optional[bool] = None,
        merge_objects: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Insert a new document.

        Args:
            document (dict): Document to insert. If it contains the "_key" or "_id"
                field, the value is used as the key of the new document (otherwise
                it is auto-generated). Any "_rev" field is ignored.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result. Only available if the
                `overwrite` option is used.
            silent (bool | None): If set to `True`, no document metadata is returned.
                This can be used to save resources.
            overwrite (bool | None): If set to `True`, operation does not fail on
                duplicate key and existing document is overwritten (replace-insert).
            overwrite_mode (str | None): Overwrite mode. Supersedes **overwrite**
                option. May be one of "ignore", "replace", "update" or "conflict".
            keep_null (bool | None): If set to `True`, fields with value None are
                retained in the document. Otherwise, they are removed completely.
                Applies only when **overwrite_mode** is set to "update"
                (update-insert).
            merge_objects (bool | None): If set to `True`, sub-dictionaries are merged
                instead of the new one overwriting the old one. Applies only when
                **overwrite_mode** is set to "update" (update-insert).
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document insertions affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations. Only applicable if **overwrite** is set to `True`
                or **overwrite_mode** is set to "update" or "replace".

        Returns:
            bool | dict: Document metadata (e.g. document id, key, revision) or `True`
                if **silent** is set to `True`.

        Raises:
            DocumentInsertError: If insertion fails.
            DocumentParseError: If the document is malformed.

        References:
            - `create-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#create-a-document>`__
        """  # noqa: E501
        if isinstance(document, dict):
            document = cast(T, self._ensure_key_from_id(document))

        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if overwrite is not None:
            params["overwrite"] = overwrite
        if overwrite_mode is not None:
            params["overwriteMode"] = overwrite_mode
        if keep_null is not None:
            params["keepNull"] = keep_null
        if merge_objects is not None:
            params["mergeObjects"] = merge_objects
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/document/{self._name}",
            params=params,
            data=self._doc_serializer.dumps(document),
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent:
                    return True
                return self.deserializer.loads(resp.raw_body)
            msg: Optional[str] = None
            if resp.status_code == HTTP_BAD_PARAMETER:
                msg = (
                    "Body does not contain a valid JSON representation of "
                    "one document."
                )
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = "Collection not found."
            raise DocumentInsertError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def update(
        self,
        document: T,
        ignore_revs: Optional[bool] = None,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        merge_objects: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Update a document.

        Args:
            document (dict): Partial or full document with the updated values.
                It must contain the "_key" or "_id" field.
            ignore_revs (bool | None): If set to `True`, the `_rev` attribute in the
                document is ignored. If this is set to `False`, then the `_rev`
                attribute given in the body document is taken as a precondition.
                The document is only updated if the current revision is the one
                specified.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, no document metadata is returned.
                This can be used to save resources.
            keep_null (bool | None): If the intention is to delete existing attributes
                with the patch command, set this parameter to `False`.
            merge_objects (bool | None): Controls whether objects (not arrays) are
                merged if present in both the existing and the patch document.
                If set to `False`, the value in the patch document overwrites the
                existing document’s value. If set to `True`, objects are merged.
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document updates affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations.
            if_match (str | None): You can conditionally update a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            bool | dict: Document metadata (e.g. document id, key, revision) or `True`
                if **silent** is set to `True`.

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentUpdateError: If update fails.

        References:
            - `update-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#update-a-document>`__
        """  # noqa: E501
        params: Params = {}
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if keep_null is not None:
            params["keepNull"] = keep_null
        if merge_objects is not None:
            params["mergeObjects"] = merge_objects
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/document/{self._extract_id(cast(Json, document))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(document),
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent is True:
                    return True
                return self.deserializer.loads(resp.raw_body)
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = "Document, collection or transaction not found."
            raise DocumentUpdateError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def replace(
        self,
        document: T,
        ignore_revs: Optional[bool] = None,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        version_attribute: Optional[str] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Replace a document.

        Args:
            document (dict): New document. It must contain the "_key" or "_id" field.
                Edge document must also have "_from" and "_to" fields.
            ignore_revs (bool | None): If set to `True`, the `_rev` attribute in the
                document is ignored. If this is set to `False`, then the `_rev`
                attribute given in the body document is taken as a precondition.
                The document is only replaced if the current revision is the one
                specified.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, no document metadata is returned.
                This can be used to save resources.
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document updates affect the edge index
                or cache-enabled persistent indexes.
            version_attribute (str | None): Support for simple external versioning to
                document operations.
            if_match (str | None): You can conditionally replace a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            bool | dict: Document metadata (e.g. document id, key, revision) or `True`
                if **silent** is set to `True`.

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentReplaceError: If replace fails.

        References:
            - `replace-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#replace-a-document>`__
        """  # noqa: E501
        params: Params = {}
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches
        if version_attribute is not None:
            params["versionAttribute"] = version_attribute

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/document/{self._extract_id(cast(Json, document))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(document),
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent is True:
                    return True
                return self.deserializer.loads(resp.raw_body)
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = "Document, collection or transaction not found."
            raise DocumentReplaceError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def delete(
        self,
        document: str | T,
        ignore_revs: Optional[bool] = None,
        ignore_missing: bool = False,
        wait_for_sync: Optional[bool] = None,
        return_old: Optional[bool] = None,
        silent: Optional[bool] = None,
        refill_index_caches: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Delete a document.

        Args:
            document (str | dict): Document ID, key or body. The body must contain the
                "_key" or "_id" field.
            ignore_revs (bool | None): If set to `True`, the `_rev` attribute in the
                document is ignored. If this is set to `False`, then the `_rev`
                attribute given in the body document is taken as a precondition.
                The document is only replaced if the current revision is the one
                specified.
            ignore_missing (bool): Do not raise an exception on missing document.
                This parameter has no effect in transactions where an exception is
                always raised on failures.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            silent (bool | None): If set to `True`, no document metadata is returned.
                This can be used to save resources.
            refill_index_caches (bool | None): Whether to add new entries to
                in-memory index caches if document updates affect the edge index
                or cache-enabled persistent indexes.
            if_match (bool | None): You can conditionally remove a document based
                on a target revision id by using the "if-match" HTTP header.

        Returns:
            bool | dict: Document metadata (e.g. document id, key, revision) or `True`
                if **silent** is set to `True` and the document was found.

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentDeleteError: If deletion fails.

        References:
            - `remove-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#remove-a-document>`__
        """  # noqa: E501
        handle = self._get_doc_id(cast(str | Json, document))

        params: Params = {}
        if ignore_revs is not None:
            params["ignoreRevs"] = ignore_revs
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_old is not None:
            params["returnOld"] = return_old
        if silent is not None:
            params["silent"] = silent
        if refill_index_caches is not None:
            params["refillIndexCaches"] = refill_index_caches

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/document/{handle}",
            params=params,
            headers=headers,
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent is True:
                    return True
                return self.deserializer.loads(resp.raw_body)
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND and ignore_missing:
                    return False
                msg = "Document, collection or transaction not found."
            raise DocumentDeleteError(resp, request, msg)

        return await self._executor.execute(request, response_handler)


class VertexCollection(Collection[T, U, V]):
    """Vertex collection API wrapper.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        graph (str): Graph name.
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        graph: str,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        super().__init__(executor, name, doc_serializer, doc_deserializer)
        self._graph = graph

    def __repr__(self) -> str:
        return f"<VertexCollection {self.name}>"

    @staticmethod
    def _parse_result(data: Json) -> Json:
        """Parse the result from the response.

        Args:
            data (dict): Response data.

        Returns:
            dict: Parsed result.
        """
        result: Json = {}
        if "new" in data or "old" in data:
            result["vertex"] = data["vertex"]
            if "new" in data:
                result["new"] = data["new"]
            if "old" in data:
                result["old"] = data["old"]
        else:
            result = data["vertex"]
        return result

    @property
    def graph(self) -> str:
        """Return the graph name.

        Returns:
            str: Graph name.
        """
        return self._graph

    async def get(
        self,
        vertex: str | Json,
        rev: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[Optional[Json]]:
        """Return a vertex from the graph.

        Args:
            vertex (str | dict): Document ID, key or body.
                Document body must contain the "_id" or "_key" field.
            rev (str | None): If this is set a document is only returned if it
                has exactly this revision.
            if_match (str | None): The document is returned, if it has the same
                revision as the given ETag.
            if_none_match (str | None): The document is returned, if it has a
                different revision than the given ETag.

        Returns:
            dict | None: Document or `None` if not found.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.
            DocumentParseError: If the document is malformed.

        References:
            - `get-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#get-a-vertex>`__
        """  # noqa: E501
        handle = self._get_doc_id(vertex)

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        params: Params = {}
        if rev is not None:
            params["rev"] = rev

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/gharial/{self._graph}/vertex/{handle}",
            headers=headers,
            params=params,
        )

        def response_handler(resp: Response) -> Optional[Json]:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND:
                    return None
                else:
                    raise DocumentGetError(resp, request)
            elif resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            else:
                raise DocumentGetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def insert(
        self,
        vertex: T,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
    ) -> Result[Json]:
        """Insert a new vertex document.

        Args:
            vertex (dict): Document to insert. If it contains the "_key" or "_id"
                field, the value is used as the key of the new document (otherwise
                it is auto-generated). Any "_rev" field is ignored.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` is specified, the result contains the document
                metadata in the "vertex" field and the new document in the "new" field.

        Raises:
            DocumentInsertError: If insertion fails.
            DocumentParseError: If the document is malformed.

        References:
            - `create-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#create-a-vertex>`__
        """  # noqa: E501
        if isinstance(vertex, dict):
            vertex = cast(T, self._ensure_key_from_id(vertex))

        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/gharial/{self._graph}/vertex/{self.name}",
            params=params,
            data=self._doc_serializer.dumps(vertex),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "The graph cannot be found or the collection is not "
                    "part of the graph."
                )
            raise DocumentInsertError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def update(
        self,
        vertex: T,
        wait_for_sync: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[Json]:
        """Update a vertex in the graph.

        Args:
            vertex (dict): Partial or full document with the updated values.
                It must contain the "_key" or "_id" field.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            keep_null (bool | None): If the intention is to delete existing attributes
                with the patch command, set this parameter to `False`.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally update a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` or "return_old" are specified, the result contains
                the document metadata in the "vertex" field and two additional fields
                ("new" and "old").

        Raises:
            DocumentUpdateError: If update fails.

        References:
            - `update-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#update-a-vertex>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if keep_null is not None:
            params["keepNull"] = keep_null
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/gharial/{self._graph}/vertex/"
            f"{self._get_doc_id(cast(Json, vertex))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(vertex),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "Vertex or graph not found, or the collection is not part of "
                    "this graph. Error may also occur if the transaction ID is "
                    "unknown."
                )
            raise DocumentUpdateError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def replace(
        self,
        vertex: T,
        wait_for_sync: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[Json]:
        """Replace a vertex in the graph.

        Args:
            vertex (dict): New document. It must contain the "_key" or "_id" field.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            keep_null (bool | None): If the intention is to delete existing attributes
                with the patch command, set this parameter to `False`.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally replace a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` or "return_old" are specified, the result contains
                the document metadata in the "vertex" field and two additional fields
                ("new" and "old").

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentReplaceError: If replace fails.

        References:
            - `replace-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#replace-a-vertex>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if keep_null is not None:
            params["keepNull"] = keep_null
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/gharial/{self._graph}/vertex/"
            f"{self._get_doc_id(cast(Json, vertex))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(vertex),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "Vertex or graph not found, or the collection is not part of "
                    "this graph. Error may also occur if the transaction ID is "
                    "unknown."
                )
            raise DocumentReplaceError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def delete(
        self,
        vertex: T,
        ignore_missing: bool = False,
        wait_for_sync: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Delete a vertex from the graph.

        Args:
            vertex (dict): Document ID, key or body. The body must contain the
                "_key" or "_id" field.
            ignore_missing (bool): Do not raise an exception on missing document.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally replace a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            bool | dict: `True` if vertex was deleted successfully, `False` if vertex
                was not found and **ignore_missing** was set to `True` (does not apply
                in transactions). Old document is returned if **return_old** is set
                to `True`.

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentDeleteError: If deletion fails.

        References:
            - `remove-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#remove-a-vertex>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/gharial/{self._graph}/vertex/"
            f"{self._get_doc_id(cast(Json, vertex))}",
            params=params,
            headers=headers,
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                data: Json = self.deserializer.loads(resp.raw_body)
                if "old" in data:
                    return cast(Json, data["old"])
                return True
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND and ignore_missing:
                    return False
                msg = (
                    "Vertex or graph not found, or the collection is not part of "
                    "this graph. Error may also occur if the transaction ID is "
                    "unknown."
                )
            raise DocumentDeleteError(resp, request, msg)

        return await self._executor.execute(request, response_handler)


class EdgeCollection(Collection[T, U, V]):
    """Edge collection API wrapper.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        graph (str): Graph name.
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        graph: str,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        super().__init__(executor, name, doc_serializer, doc_deserializer)
        self._graph = graph

    def __repr__(self) -> str:
        return f"<EdgeCollection {self.name}>"

    @staticmethod
    def _parse_result(data: Json) -> Json:
        """Parse the result from the response.

        Args:
            data (dict): Response data.

        Returns:
            dict: Parsed result.
        """
        result: Json = {}
        if "new" in data or "old" in data:
            result["edge"] = data["edge"]
            if "new" in data:
                result["new"] = data["new"]
            if "old" in data:
                result["old"] = data["old"]
        else:
            result = data["edge"]
        return result

    @property
    def graph(self) -> str:
        """Return the graph name.

        Returns:
            str: Graph name.
        """
        return self._graph

    async def get(
        self,
        edge: str | Json,
        rev: Optional[str] = None,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[Optional[Json]]:
        """Return an edge from the graph.

        Args:
            edge (str | dict): Document ID, key or body.
                Document body must contain the "_id" or "_key" field.
            rev (str | None): If this is set a document is only returned if it
                has exactly this revision.
            if_match (str | None): The document is returned, if it has the same
                revision as the given ETag.
            if_none_match (str | None): The document is returned, if it has a
                different revision than the given ETag.

        Returns:
            dict | None: Document or `None` if not found.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.
            DocumentParseError: If the document is malformed.

        References:
            - `get-an-edge <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#get-an-edge>`__
        """  # noqa: E501
        handle = self._get_doc_id(edge)

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        params: Params = {}
        if rev is not None:
            params["rev"] = rev

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/gharial/{self._graph}/edge/{handle}",
            headers=headers,
            params=params,
        )

        def response_handler(resp: Response) -> Optional[Json]:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND:
                    return None
                else:
                    raise DocumentGetError(resp, request)
            elif resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            else:
                raise DocumentGetError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def insert(
        self,
        edge: T,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
    ) -> Result[Json]:
        """Insert a new edge document.

        Args:
            edge (dict): Document to insert. It must contain "_from" and
                "_to" fields. If it contains the "_key" or "_id"
                field, the value is used as the key of the new document (otherwise
                it is auto-generated). Any "_rev" field is ignored.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` is specified, the result contains the document
                metadata in the "edge" field and the new document in the "new" field.

        Raises:
            DocumentInsertError: If insertion fails.
            DocumentParseError: If the document is malformed.

        References:
            - `create-an-edge <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#create-an-edge>`__
        """  # noqa: E501
        if isinstance(edge, dict):
            edge = cast(T, self._ensure_key_from_id(edge))

        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_new is not None:
            params["returnNew"] = return_new

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/gharial/{self._graph}/edge/{self.name}",
            params=params,
            data=self._doc_serializer.dumps(edge),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "The graph cannot be found or the edge collection is not "
                    "part of the graph. It is also possible that the vertex "
                    "collection referenced in the _from or _to attribute is not part "
                    "of the graph or the vertex collection is part of the graph, but "
                    "does not exist. Finally check that _from or _to vertex do exist."
                )
            raise DocumentInsertError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def update(
        self,
        edge: T,
        wait_for_sync: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[Json]:
        """Update an edge in the graph.

        Args:
            edge (dict): Partial or full document with the updated values.
                It must contain the "_key" or "_id" field, along with "_from" and
                "_to" fields.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            keep_null (bool | None): If the intention is to delete existing attributes
                with the patch command, set this parameter to `False`.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally update a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` or "return_old" are specified, the result contains
                the document metadata in the "edge" field and two additional fields
                ("new" and "old").

        Raises:
            DocumentUpdateError: If update fails.

        References:
            - `update-an-edge <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#update-an-edge>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if keep_null is not None:
            params["keepNull"] = keep_null
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/gharial/{self._graph}/edge/"
            f"{self._get_doc_id(cast(Json, edge))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(edge),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "The graph cannot be found or the edge collection is not "
                    "part of the graph. It is also possible that the vertex "
                    "collection referenced in the _from or _to attribute is not part "
                    "of the graph or the vertex collection is part of the graph, but "
                    "does not exist. Finally check that _from or _to vertex do exist."
                )
            raise DocumentUpdateError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def replace(
        self,
        edge: T,
        wait_for_sync: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[Json]:
        """Replace an edge in the graph.

        Args:
            edge (dict): Partial or full document with the updated values.
                It must contain the "_key" or "_id" field, along with "_from" and
                "_to" fields.
            wait_for_sync (bool | None): Wait until document has been synced to disk.
            keep_null (bool | None): If the intention is to delete existing attributes
                with the patch command, set this parameter to `False`.
            return_new (bool | None): Additionally return the complete new document
                under the attribute `new` in the result.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally replace a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` or "return_old" are specified, the result contains
                the document metadata in the "edge" field and two additional fields
                ("new" and "old").

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentReplaceError: If replace fails.

        References:
            - `replace-an-edge <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#replace-an-edge>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if keep_null is not None:
            params["keepNull"] = keep_null
        if return_new is not None:
            params["returnNew"] = return_new
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/gharial/{self._graph}/edge/"
            f"{self._get_doc_id(cast(Json, edge))}",
            params=params,
            headers=headers,
            data=self._doc_serializer.dumps(edge),
        )

        def response_handler(resp: Response) -> Json:
            if resp.is_success:
                return self._parse_result(self.deserializer.loads(resp.raw_body))
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = (
                    "The graph cannot be found or the edge collection is not "
                    "part of the graph. It is also possible that the vertex "
                    "collection referenced in the _from or _to attribute is not part "
                    "of the graph or the vertex collection is part of the graph, but "
                    "does not exist. Finally check that _from or _to vertex do exist."
                )
            raise DocumentReplaceError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def delete(
        self,
        edge: T,
        ignore_missing: bool = False,
        wait_for_sync: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Delete an edge from the graph.

        Args:
            edge (dict): Partial or full document with the updated values.
                It must contain the "_key" or "_id" field, along with "_from" and
                "_to" fields.
            ignore_missing (bool): Do not raise an exception on missing document.
            wait_for_sync (bool | None): Wait until operation has been synced to disk.
            return_old (bool | None): Additionally return the complete old document
                under the attribute `old` in the result.
            if_match (str | None): You can conditionally replace a document based on a
                target revision id by using the "if-match" HTTP header.

        Returns:
            bool | dict: `True` if vertex was deleted successfully, `False` if vertex
                was not found and **ignore_missing** was set to `True` (does not apply
                in transactions). Old document is returned if **return_old** is set
                to `True`.

        Raises:
            DocumentRevisionError: If precondition was violated.
            DocumentDeleteError: If deletion fails.

        References:
            - `remove-an-edge <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#remove-an-edge>`__
        """  # noqa: E501
        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if return_old is not None:
            params["returnOld"] = return_old

        headers: RequestHeaders = {}
        if if_match is not None:
            headers["If-Match"] = if_match

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/gharial/{self._graph}/edge/"
            f"{self._get_doc_id(cast(Json, edge))}",
            params=params,
            headers=headers,
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                data: Json = self.deserializer.loads(resp.raw_body)
                if "old" in data:
                    return cast(Json, data["old"])
                return True
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                if resp.error_code == DOCUMENT_NOT_FOUND and ignore_missing:
                    return False
                msg = (
                    "Either the graph cannot be found, the edge collection is not "
                    "part of the graph, or the edge does not exist"
                )
            raise DocumentDeleteError(resp, request, msg)

        return await self._executor.execute(request, response_handler)

    async def edges(
        self,
        vertex: str | Json,
        direction: Optional[Literal["in", "out"]] = None,
        allow_dirty_read: Optional[bool] = None,
    ) -> Result[Json]:
        """Return the edges starting or ending at the specified vertex.

        Args:
            vertex (str | dict): Document ID, key or body.
            direction (str | None): Direction of the edges to return. Selects `in`
                or `out` direction for edges. If not set, any edges are returned.
            allow_dirty_read (bool | None): Allow reads from followers in a cluster.

        Returns:
            dict: List of edges and statistics.

        Raises:
            EdgeListError: If retrieval fails.

        References:
            - `get-inbound-and-outbound-edges <https://docs.arangodb.com/stable/develop/http-api/graphs/edges/#get-inbound-and-outbound-edges>`__
        """  # noqa: E501
        params: Params = {
            "vertex": self._get_doc_id(vertex, validate=False),
        }
        if direction is not None:
            params["direction"] = direction

        headers: RequestHeaders = {}
        if allow_dirty_read is not None:
            headers["x-arango-allow-dirty-read"] = (
                "true" if allow_dirty_read else "false"
            )

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/edges/{self._name}",
            params=params,
            headers=headers,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise EdgeListError(resp, request)
            return Response.format_body(self.deserializer.loads(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def link(
        self,
        from_vertex: str | Json,
        to_vertex: str | Json,
        data: Optional[Json] = None,
        wait_for_sync: Optional[bool] = None,
        return_new: bool = False,
    ) -> Result[Json]:
        """Insert a new edge document linking the given vertices.

        Args:
            from_vertex (str | dict): "_from" vertex document ID or body with "_id"
                field.
            to_vertex (str | dict): "_to" vertex document ID or body with "_id" field.
            data (dict | None): Any extra data for the new edge document. If it has
                "_key" or "_id" field, its value is used as key of the new edge document
                (otherwise it is auto-generated).
            wait_for_sync (bool | None): Wait until operation has been synced to disk.
            return_new: Optional[bool]: Additionally return the complete new document
                under the attribute `new` in the result.

        Returns:
            dict: Document metadata (e.g. document id, key, revision).
                If `return_new` is specified, the result contains the document
                metadata in the "edge" field and the new document in the "new" field.

        Raises:
            DocumentInsertError: If insertion fails.
            DocumentParseError: If the document is malformed.
        """
        edge: Json = {
            "_from": self._get_doc_id(from_vertex, validate=False),
            "_to": self._get_doc_id(to_vertex, validate=False),
        }
        if data is not None:
            edge.update(self._ensure_key_from_id(data))
        return await self.insert(
            cast(T, edge), wait_for_sync=wait_for_sync, return_new=return_new
        )
