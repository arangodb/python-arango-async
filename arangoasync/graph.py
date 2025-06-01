__all__ = ["Graph"]


from typing import Generic, List, Literal, Optional, Sequence, TypeVar, cast

from arangoasync.collection import Collection, EdgeCollection, VertexCollection
from arangoasync.exceptions import (
    EdgeCollectionListError,
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
    GraphPropertiesError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import (
    EdgeDefinitionOptions,
    GraphProperties,
    Json,
    Jsons,
    Params,
    VertexCollectionOptions,
)

T = TypeVar("T")  # Serializer type
U = TypeVar("U")  # Deserializer loads
V = TypeVar("V")  # Deserializer loads_many


class Graph(Generic[T, U, V]):
    """Graph API wrapper, representing a graph in ArangoDB.

    Args:
        executor (APIExecutor): Required to execute the API requests.
        name (str): Graph name.
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

    def __repr__(self) -> str:
        return f"<Graph {self._name}>"

    @property
    def name(self) -> str:
        """Name of the graph."""
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

    async def properties(self) -> Result[GraphProperties]:
        """Get the properties of the graph.

        Returns:
            GraphProperties: Properties of the graph.

        Raises:
            GraphProperties: If the operation fails.

        References:
            - `get-a-graph <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#get-a-graph>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> GraphProperties:
            if not resp.is_success:
                raise GraphPropertiesError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return GraphProperties(body["graph"])

        return await self._executor.execute(request, response_handler)

    def vertex_collection(self, name: str) -> VertexCollection[T, U, V]:
        """Returns the vertex collection API wrapper.

        Args:
            name (str): Vertex collection name.

        Returns:
            VertexCollection: Vertex collection API wrapper.
        """
        return VertexCollection[T, U, V](
            executor=self._executor,
            graph=self._name,
            name=name,
            doc_serializer=self._doc_serializer,
            doc_deserializer=self._doc_deserializer,
        )

    async def vertex_collections(self) -> Result[List[str]]:
        """Get the names of all vertex collections in the graph.

        Returns:
            list: List of vertex collection names.

        Raises:
            VertexCollectionListError: If the operation fails.

        References:
            - `list-vertex-collections <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#list-vertex-collections>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/gharial/{self._name}/vertex",
        )

        def response_handler(resp: Response) -> List[str]:
            if not resp.is_success:
                raise VertexCollectionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return list(sorted(body["collections"]))

        return await self._executor.execute(request, response_handler)

    async def has_vertex_collection(self, name: str) -> Result[bool]:
        """Check if the graph has the given vertex collection.

        Args:
            name (str): Vertex collection mame.

        Returns:
            bool: `True` if the graph has the vertex collection, `False` otherwise.

        Raises:
            VertexCollectionListError: If the operation fails.
        """
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/gharial/{self._name}/vertex",
        )

        def response_handler(resp: Response) -> bool:
            if not resp.is_success:
                raise VertexCollectionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return name in body["collections"]

        return await self._executor.execute(request, response_handler)

    async def create_vertex_collection(
        self,
        name: str,
        options: Optional[VertexCollectionOptions | Json] = None,
    ) -> Result[VertexCollection[T, U, V]]:
        """Create a vertex collection in the graph.

        Args:
            name (str): Vertex collection name.
            options (dict | VertexCollectionOptions | None): Extra options for
                creating vertex collections.

        Returns:
            VertexCollection: Vertex collection API wrapper.

        Raises:
            VertexCollectionCreateError: If the operation fails.

        References:
           - `add-a-vertex-collection <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#add-a-vertex-collection>`__
        """  # noqa: E501
        data: Json = {"collection": name}

        if options is not None:
            if isinstance(options, VertexCollectionOptions):
                data["options"] = options.to_dict()
            else:
                data["options"] = options

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/gharial/{self._name}/vertex",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> VertexCollection[T, U, V]:
            if not resp.is_success:
                raise VertexCollectionCreateError(resp, request)
            return self.vertex_collection(name)

        return await self._executor.execute(request, response_handler)

    async def delete_vertex_collection(self, name: str, purge: bool = False) -> None:
        """Remove a vertex collection from the graph.

        Args:
            name (str): Vertex collection name.
            purge (bool): If set to `True`, the vertex collection is not just deleted
                from the graph but also from the database completely. Note that you
                cannot remove vertex collections that are used in one of the edge
                definitions of the graph.

        Raises:
            VertexCollectionDeleteError: If the operation fails.

        References:
           - `remove-a-vertex-collection <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#remove-a-vertex-collection>`__
        """  # noqa: E501
        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/gharial/{self._name}/vertex/{name}",
            params={"dropCollection": purge},
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise VertexCollectionDeleteError(resp, request)

        await self._executor.execute(request, response_handler)

    async def has_vertex(
        self,
        vertex: str | Json,
        allow_dirty_read: bool = False,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[bool]:
        """Check if the vertex exists in the graph.

        Args:
            vertex (str | dict): Document ID, key or body.
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
        """  # noqa: E501
        col = Collection.get_col_name(vertex)
        return await self.vertex_collection(col).has(
            vertex,
            allow_dirty_read=allow_dirty_read,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def vertex(
        self,
        vertex: str | Json,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[Optional[Json]]:
        """Return a vertex document.

        Args:
            vertex (str | dict): Document ID, key or body.
                Document body must contain the "_id" or "_key" field.
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
            - `get-a-vertex <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#get-a-vertex>`__
        """  # noqa: E501
        col = Collection.get_col_name(vertex)
        return await self.vertex_collection(col).get(
            vertex,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def insert_vertex(
        self,
        collection: str,
        vertex: T,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
    ) -> Result[Json]:
        """Insert a new vertex document.

        Args:
            collection (str): Name of the vertex collection to insert the document into.
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
        return await self.vertex_collection(collection).insert(
            vertex,
            wait_for_sync=wait_for_sync,
            return_new=return_new,
        )

    async def update_vertex(
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
        col = Collection.get_col_name(cast(Json | str, vertex))
        return await self.vertex_collection(col).update(
            vertex,
            wait_for_sync=wait_for_sync,
            keep_null=keep_null,
            return_new=return_new,
            return_old=return_old,
            if_match=if_match,
        )

    async def replace_vertex(
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
        col = Collection.get_col_name(cast(Json | str, vertex))
        return await self.vertex_collection(col).replace(
            vertex,
            wait_for_sync=wait_for_sync,
            keep_null=keep_null,
            return_new=return_new,
            return_old=return_old,
            if_match=if_match,
        )

    async def delete_vertex(
        self,
        vertex: T,
        ignore_missing: bool = False,
        wait_for_sync: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[bool | Json]:
        """Delete a vertex in the graph.

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
        col = Collection.get_col_name(cast(Json | str, vertex))
        return await self.vertex_collection(col).delete(
            vertex,
            ignore_missing=ignore_missing,
            wait_for_sync=wait_for_sync,
            return_old=return_old,
            if_match=if_match,
        )

    def edge_collection(self, name: str) -> EdgeCollection[T, U, V]:
        """Returns the edge collection API wrapper.

        Args:
            name (str): Edge collection name.

        Returns:
            EdgeCollection: Edge collection API wrapper.
        """
        return EdgeCollection[T, U, V](
            executor=self._executor,
            graph=self._name,
            name=name,
            doc_serializer=self._doc_serializer,
            doc_deserializer=self._doc_deserializer,
        )

    async def edge_definitions(self) -> Result[Jsons]:
        """Return the edge definitions from the graph.

        Returns:
            list: List of edge definitions.

        Raises:
            EdgeDefinitionListError: If the operation fails.
        """
        request = Request(method=Method.GET, endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise EdgeDefinitionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            properties = GraphProperties(body["graph"])
            edge_definitions = properties.format(
                GraphProperties.compatibility_formatter
            )["edge_definitions"]
            return cast(Jsons, edge_definitions)

        return await self._executor.execute(request, response_handler)

    async def has_edge_definition(self, name: str) -> Result[bool]:
        """Check if the graph has the given edge definition.

        Returns:
            bool: `True` if the graph has the edge definitions, `False` otherwise.

        Raises:
            EdgeDefinitionListError: If the operation fails.
        """
        request = Request(method=Method.GET, endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> bool:
            if not resp.is_success:
                raise EdgeDefinitionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return any(
                edge_definition["collection"] == name
                for edge_definition in body["graph"]["edgeDefinitions"]
            )

        return await self._executor.execute(request, response_handler)

    async def edge_collections(self) -> Result[List[str]]:
        """Get the names of all edge collections in the graph.

        Returns:
            list: List of edge collection names.

        Raises:
            EdgeCollectionListError: If the operation fails.

        References:
            - `list-edge-collections <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#list-edge-collections>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint=f"/_api/gharial/{self._name}/edge",
        )

        def response_handler(resp: Response) -> List[str]:
            if not resp.is_success:
                raise EdgeCollectionListError(resp, request)
            body = self.deserializer.loads(resp.raw_body)
            return list(sorted(body["collections"]))

        return await self._executor.execute(request, response_handler)

    async def create_edge_definition(
        self,
        edge_collection: str,
        from_vertex_collections: Sequence[str],
        to_vertex_collections: Sequence[str],
        options: Optional[EdgeDefinitionOptions | Json] = None,
    ) -> Result[EdgeCollection[T, U, V]]:
        """Create an edge definition in the graph.

        This edge definition has to contain a collection and an array of each from
        and to vertex collections.

        .. code-block:: python

            {
                "edge_collection": "edge_collection_name",
                "from_vertex_collections": ["from_vertex_collection_name"],
                "to_vertex_collections": ["to_vertex_collection_name"]
            }

        Args:
            edge_collection (str): Edge collection name.
            from_vertex_collections (list): List of vertex collections
                that can be used as the "from" vertex in edges.
            to_vertex_collections (list): List of vertex collections
                that can be used as the "to" vertex in edges.
            options (dict | EdgeDefinitionOptions | None): Extra options for
                creating edge definitions.

        Returns:
            EdgeCollection: Edge collection API wrapper.

        Raises:
            EdgeDefinitionCreateError: If the operation fails.

        References:
            - `add-an-edge-definition <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#add-an-edge-definition>`__
        """  # noqa: E501
        data: Json = {
            "collection": edge_collection,
            "from": from_vertex_collections,
            "to": to_vertex_collections,
        }

        if options is not None:
            if isinstance(options, VertexCollectionOptions):
                data["options"] = options.to_dict()
            else:
                data["options"] = options

        request = Request(
            method=Method.POST,
            endpoint=f"/_api/gharial/{self._name}/edge",
            data=self.serializer.dumps(data),
        )

        def response_handler(resp: Response) -> EdgeCollection[T, U, V]:
            if not resp.is_success:
                raise EdgeDefinitionCreateError(resp, request)
            return self.edge_collection(edge_collection)

        return await self._executor.execute(request, response_handler)

    async def replace_edge_definition(
        self,
        edge_collection: str,
        from_vertex_collections: Sequence[str],
        to_vertex_collections: Sequence[str],
        options: Optional[EdgeDefinitionOptions | Json] = None,
        wait_for_sync: Optional[bool] = None,
        drop_collections: Optional[bool] = None,
    ) -> Result[EdgeCollection[T, U, V]]:
        """Replace an edge definition.

        Args:
            edge_collection (str): Edge collection name.
            from_vertex_collections (list): Names of "from" vertex collections.
            to_vertex_collections (list): Names of "to" vertex collections.
            options (dict | EdgeDefinitionOptions | None): Extra options for
                modifying collections withing this edge definition.
            wait_for_sync (bool | None): If set to `True`, the operation waits for
                data to be synced to disk before returning.
            drop_collections (bool | None): Drop the edge collection in addition to
                removing it from the graph. The collection is only dropped if it is
                not used in other graphs.

        Returns:
            EdgeCollection: API wrapper.

        Raises:
            EdgeDefinitionReplaceError: If the operation fails.

        References:
            - `replace-an-edge-definition <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#replace-an-edge-definition>`__
        """  # noqa: E501
        data: Json = {
            "collection": edge_collection,
            "from": from_vertex_collections,
            "to": to_vertex_collections,
        }
        if options is not None:
            if isinstance(options, VertexCollectionOptions):
                data["options"] = options.to_dict()
            else:
                data["options"] = options

        params: Params = {}
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync
        if drop_collections is not None:
            params["dropCollections"] = drop_collections

        request = Request(
            method=Method.PUT,
            endpoint=f"/_api/gharial/{self._name}/edge/{edge_collection}",
            data=self.serializer.dumps(data),
            params=params,
        )

        def response_handler(resp: Response) -> EdgeCollection[T, U, V]:
            if resp.is_success:
                return self.edge_collection(edge_collection)
            raise EdgeDefinitionReplaceError(resp, request)

        return await self._executor.execute(request, response_handler)

    async def delete_edge_definition(
        self,
        name: str,
        drop_collections: Optional[bool] = None,
        wait_for_sync: Optional[bool] = None,
    ) -> None:
        """Delete an edge definition from the graph.

        Args:
            name (str): Edge collection name.
            drop_collections (bool | None): If set to `True`, the edge definition is not
                just removed from the graph but the edge collection is also deleted
                completely from the database.
            wait_for_sync (bool | None): If set to `True`, the operation waits for
                changes to be synced to disk before returning.

        Raises:
            EdgeDefinitionDeleteError: If the operation fails.

        References:
            - `remove-an-edge-definition <https://docs.arangodb.com/stable/develop/http-api/graphs/named-graphs/#remove-an-edge-definition>`__
        """  # noqa: E501
        params: Params = {}
        if drop_collections is not None:
            params["dropCollections"] = drop_collections
        if wait_for_sync is not None:
            params["waitForSync"] = wait_for_sync

        request = Request(
            method=Method.DELETE,
            endpoint=f"/_api/gharial/{self._name}/edge/{name}",
            params=params,
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise EdgeDefinitionDeleteError(resp, request)

        await self._executor.execute(request, response_handler)

    async def has_edge(
        self,
        edge: str | Json,
        allow_dirty_read: bool = False,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> Result[bool]:
        """Check if the edge exists in the graph.

        Args:
            edge (str | dict): Document ID, key or body.
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
        """  # noqa: E501
        col = Collection.get_col_name(edge)
        return await self.edge_collection(col).has(
            edge,
            allow_dirty_read=allow_dirty_read,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def edge(
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
        col = Collection.get_col_name(edge)
        return await self.edge_collection(col).get(
            edge,
            rev=rev,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def insert_edge(
        self,
        collection: str,
        edge: T,
        wait_for_sync: Optional[bool] = None,
        return_new: Optional[bool] = None,
    ) -> Result[Json]:
        """Insert a new edge document.

        Args:
            collection (str): Name of the vertex collection to insert the document into.
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
        return await self.edge_collection(collection).insert(
            edge,
            wait_for_sync=wait_for_sync,
            return_new=return_new,
        )

    async def update_edge(
        self,
        edge: T,
        wait_for_sync: Optional[bool] = None,
        keep_null: Optional[bool] = None,
        return_new: Optional[bool] = None,
        return_old: Optional[bool] = None,
        if_match: Optional[str] = None,
    ) -> Result[Json]:
        """Update a vertex in the graph.

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
        col = Collection.get_col_name(cast(Json | str, edge))
        return await self.edge_collection(col).update(
            edge,
            wait_for_sync=wait_for_sync,
            keep_null=keep_null,
            return_new=return_new,
            return_old=return_old,
            if_match=if_match,
        )

    async def replace_edge(
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
        col = Collection.get_col_name(cast(Json | str, edge))
        return await self.edge_collection(col).replace(
            edge,
            wait_for_sync=wait_for_sync,
            keep_null=keep_null,
            return_new=return_new,
            return_old=return_old,
            if_match=if_match,
        )

    async def delete_edge(
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
        col = Collection.get_col_name(cast(Json | str, edge))
        return await self.edge_collection(col).delete(
            edge,
            ignore_missing=ignore_missing,
            wait_for_sync=wait_for_sync,
            return_old=return_old,
            if_match=if_match,
        )

    async def edges(
        self,
        collection: str,
        vertex: str | Json,
        direction: Optional[Literal["in", "out"]] = None,
        allow_dirty_read: Optional[bool] = None,
    ) -> Result[Json]:
        """Return the edges starting or ending at the specified vertex.

        Args:
            collection (str): Name of the edge collection to return edges from.
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
        return await self.edge_collection(collection).edges(
            vertex,
            direction=direction,
            allow_dirty_read=allow_dirty_read,
        )

    async def link(
        self,
        collection: str,
        from_vertex: str | Json,
        to_vertex: str | Json,
        data: Optional[Json] = None,
        wait_for_sync: Optional[bool] = None,
        return_new: bool = False,
    ) -> Result[Json]:
        """Insert a new edge document linking the given vertices.

        Args:
            collection (str): Name of the collection to insert the edge into.
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
        return await self.edge_collection(collection).link(
            from_vertex,
            to_vertex,
            data=data,
            wait_for_sync=wait_for_sync,
            return_new=return_new,
        )
