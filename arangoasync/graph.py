__all__ = ["Graph"]


from typing import Generic, List, Optional, Sequence, TypeVar

from arangoasync.collection import EdgeCollection, VertexCollection
from arangoasync.exceptions import (
    EdgeDefinitionCreateError,
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
            return list(sorted(set(body["collections"])))

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
