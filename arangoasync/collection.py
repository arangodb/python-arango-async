__all__ = ["Collection", "StandardCollection"]


from typing import Generic, List, Optional, Tuple, TypeVar, cast

from arangoasync.errno import (
    HTTP_BAD_PARAMETER,
    HTTP_NOT_FOUND,
    HTTP_PRECONDITION_FAILED,
)
from arangoasync.exceptions import (
    CollectionPropertiesError,
    DocumentGetError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    IndexCreateError,
    IndexDeleteError,
    IndexGetError,
    IndexListError,
    IndexLoadError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    CollectionProperties,
    IndexProperties,
    Json,
    Jsons,
    Params,
)

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


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

    def _extract_id(self, body: Json) -> str:
        """Extract the document ID from document body.

        Args:
            body (dict): Document body.

        Returns:
            str: Document ID.

        Raises:
            DocumentParseError: On missing ID and key.
        """
        try:
            if "_id" in body:
                return self._validate_id(body["_id"])
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
        """
        if "_id" in body and "_key" not in body:
            doc_id = self._validate_id(body["_id"])
            body = body.copy()
            body["_key"] = doc_id[len(self._id_prefix) :]
        return body

    def _prep_from_doc(
        self,
        document: str | Json,
        rev: Optional[str] = None,
        check_rev: bool = False,
    ) -> Tuple[str, Json]:
        """Prepare document ID, body and request headers before a query.

        Args:
            document (str | dict): Document ID, key or body.
            rev (str | None): Document revision.
            check_rev (bool): Whether to check the revision.

        Returns:
            Document ID and request headers.

        Raises:
            DocumentParseError: On missing ID and key.
            TypeError: On bad document type.
        """
        if isinstance(document, dict):
            doc_id = self._extract_id(document)
            rev = rev or document.get("_rev")
        elif isinstance(document, str):
            if "/" in document:
                doc_id = self._validate_id(document)
            else:
                doc_id = self._id_prefix + document
        else:
            raise TypeError("Document must be str or a dict")

        if not check_rev or rev is None:
            return doc_id, {}
        else:
            return doc_id, {"If-Match": rev}

    @property
    def name(self) -> str:
        """Return the name of the collection.

        Returns:
            str: Collection name.
        """
        return self._name

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

    async def indexes(self) -> Result[List[IndexProperties]]:
        """Fetch all index descriptions for the given collection.

        Returns:
            list: List of index properties.

        Raises:
            IndexListError: If retrieval fails.

        References:
            - `list-all-indexes-of-a-collection <https://docs.arangodb.com/stable/develop/http-api/indexes/#list-all-indexes-of-a-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/index",
            params=dict(collection=self._name),
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
            return CollectionProperties(self._executor.deserialize(resp.raw_body))

        return await self._executor.execute(request, response_handler)

    async def get(
        self,
        document: str | Json,
        rev: Optional[str] = None,
        check_rev: bool = True,
        allow_dirty_read: bool = False,
    ) -> Result[Optional[U]]:
        """Return a document.

        Args:
            document (str | dict): Document ID, key or body.
            Document body must contain the "_id" or "_key" field.
            rev (str | None): Expected document revision. Overrides the
            value of "_rev" field in **document** if present.
            check_rev (bool): If set to True, revision of **document** (if given)
            is compared against the revision of target document.
            allow_dirty_read (bool):  Allow reads from followers in a cluster.

        Returns:
            Document or None if not found.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.

        References:
            - `get-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#get-a-document>`__
        """  # noqa: E501
        handle, headers = self._prep_from_doc(document, rev, check_rev)

        if allow_dirty_read:
            headers["x-arango-allow-dirty-read"] = "true"

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/document/{handle}",
            headers=headers,
        )

        def response_handler(resp: Response) -> Optional[U]:
            if resp.is_success:
                return self._doc_deserializer.loads(resp.raw_body)
            elif resp.status_code == HTTP_NOT_FOUND:
                return None
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

        References:
            - `create-a-document <https://docs.arangodb.com/stable/develop/http-api/documents/#create-a-document>`__
        """  # noqa: E501
        if isinstance(document, dict):
            # We assume that the document deserializer works with dictionaries.
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
                if silent is True:
                    return True
                return self._executor.deserialize(resp.raw_body)
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
    ) -> Result[bool | Json]:
        """Insert a new document.

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

        request = Request(
            method=Method.PATCH,
            endpoint=f"/_api/document/{self._extract_id(cast(Json, document))}",
            params=params,
            data=self._doc_serializer.dumps(document),
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent is True:
                    return True
                return self._executor.deserialize(resp.raw_body)
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

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/document/{self._extract_id(cast(Json, document))}",
            params=params,
            data=self._doc_serializer.dumps(document),
        )

        def response_handler(resp: Response) -> bool | Json:
            if resp.is_success:
                if silent is True:
                    return True
                return self._executor.deserialize(resp.raw_body)
            msg: Optional[str] = None
            if resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            elif resp.status_code == HTTP_NOT_FOUND:
                msg = "Document, collection or transaction not found."
            raise DocumentReplaceError(resp, request, msg)

        return await self._executor.execute(request, response_handler)
