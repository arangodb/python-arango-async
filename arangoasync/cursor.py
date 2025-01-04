__all__ = ["Cursor"]


from arangoasync.executor import ApiExecutor
from arangoasync.typings import Json


class Cursor:
    """Cursor API wrapper.

    Cursors fetch query results from ArangoDB server in batches. Cursor objects
    are *stateful* as they store the fetched items in-memory. They must not be
    shared across threads without proper locking mechanism.

    Args:
        executor: Required to execute the API requests.
        data: Cursor initialization data.
    """

    def __init__(self, executor: ApiExecutor, data: Json) -> None:
        self._executor = executor
        print(data)
        # TODO complete this
