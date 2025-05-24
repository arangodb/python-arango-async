__all__ = ["Graph"]


from typing import Generic, TypeVar

from arangoasync.collection import EdgeCollection, VertexCollection
from arangoasync.exceptions import GraphListError
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import GraphProperties, Json, Jsons

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
            GraphListError: If the operation fails.

        References:
            - `get-a-graph <https://docs.arangodb.com/3.12/develop/http-api/graphs/named-graphs/#get-a-graph>`__
        """  # noqa: E501
        request = Request(method=Method.GET, endpoint=f"/_api/gharial/{self._name}")

        def response_handler(resp: Response) -> GraphProperties:
            if not resp.is_success:
                raise GraphListError(resp, request)
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
