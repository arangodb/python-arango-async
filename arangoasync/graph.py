from arangoasync.executor import ApiExecutor


class Graph:
    """Graph API wrapper, representing a graph in ArangoDB.

    Args:
        executor: API executor. Required to execute the API requests.
    """

    def __init__(self, executor: ApiExecutor, name: str) -> None:
        self._executor = executor
        self._name = name

    def __repr__(self) -> str:
        return f"<Graph {self._name}>"

    @property
    def name(self) -> str:
        """Name of the graph."""
        return self._name
