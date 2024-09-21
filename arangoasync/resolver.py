__all__ = [
    "HostResolver",
    "SingleHostResolver",
    "RoundRobinHostResolver",
    "DefaultHostResolver",
    "get_resolver",
]

from abc import ABC, abstractmethod
from typing import List, Optional


class HostResolver(ABC):
    """Abstract base class for host resolvers.

    Args:
        host_count (int): Number of hosts.
        max_tries (int | None): Maximum number of attempts to try a host.
            Will default to 3 times the number of hosts if not provided.

    Raises:
        ValueError: If max_tries is less than host_count.
    """

    def __init__(self, host_count: int = 1, max_tries: Optional[int] = None) -> None:
        max_tries = max_tries or host_count * 3
        if max_tries < host_count:
            raise ValueError(
                "The maximum number of attempts cannot be "
                "lower than the number of hosts."
            )
        self._host_count = host_count
        self._max_tries = max_tries
        self._index = 0

    @abstractmethod
    def get_host_index(self) -> int:  # pragma: no cover
        """Return the index of the host to use.

        Returns:
            int: Index of the host.
        """
        raise NotImplementedError

    def change_host(self) -> None:
        """If there are multiple hosts available, switch to the next one."""
        self._index = (self._index + 1) % self.host_count

    @property
    def host_count(self) -> int:
        """Return the number of hosts."""
        return self._host_count

    @property
    def max_tries(self) -> int:
        """Return the maximum number of attempts."""
        return self._max_tries


class SingleHostResolver(HostResolver):
    """Single host resolver.

    Always returns the same host index, unless prompted to change.
    """

    def __init__(self, host_count: int, max_tries: Optional[int] = None) -> None:
        super().__init__(host_count, max_tries)

    def get_host_index(self) -> int:
        return self._index


class RoundRobinHostResolver(HostResolver):
    """Round-robin host resolver. Changes host every time.

    Useful for bulk inserts or updates.

    Note:
        Do not use this resolver for stream transactions.
        Transaction IDs cannot be shared across different coordinators.
    """

    def __init__(self, host_count: int, max_tries: Optional[int] = None) -> None:
        super().__init__(host_count, max_tries)
        self._index = -1

    def get_host_index(self, indexes_to_filter: Optional[List[int]] = None) -> int:
        self.change_host()
        return self._index


DefaultHostResolver = SingleHostResolver


def get_resolver(
    strategy: str,
    host_count: int,
    max_tries: Optional[int] = None,
) -> HostResolver:
    """Return a host resolver based on the strategy.

    Args:
        strategy (str): Resolver strategy.
        host_count (int): Number of hosts.
        max_tries (int): Maximum number of attempts to try a host.

    Returns:
        HostResolver: Host resolver.

    Raises:
        ValueError: If the strategy is not supported.
    """
    if strategy == "roundrobin":
        return RoundRobinHostResolver(host_count, max_tries)
    if strategy == "single":
        return SingleHostResolver(host_count, max_tries)
    if strategy == "default":
        return DefaultHostResolver(host_count, max_tries)
    raise ValueError(f"Unsupported host resolver strategy: {strategy}")
