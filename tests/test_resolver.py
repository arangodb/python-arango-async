import pytest

from arangoasync.resolver import (
    DefaultHostResolver,
    RoundRobinHostResolver,
    SingleHostResolver,
    get_resolver,
)


def test_get_resolver():
    resolver = get_resolver("default", 1, 2)
    assert isinstance(resolver, DefaultHostResolver)

    resolver = get_resolver("single", 2)
    assert isinstance(resolver, SingleHostResolver)

    resolver = get_resolver("roundrobin", 3)
    assert isinstance(resolver, RoundRobinHostResolver)

    with pytest.raises(ValueError):
        get_resolver("invalid", 1)

    with pytest.raises(ValueError):
        # max_tries cannot be less than host_count
        get_resolver("roundrobin", 3, 1)


def test_SingleHostResolver():
    resolver = SingleHostResolver(1, 2)
    assert resolver.host_count == 1
    assert resolver.max_tries == 2
    assert resolver.get_host_index() == 0
    assert resolver.get_host_index() == 0

    resolver = SingleHostResolver(3)
    assert resolver.host_count == 3
    assert resolver.max_tries == 9
    assert resolver.get_host_index() == 0
    resolver.change_host()
    assert resolver.get_host_index() == 1
    resolver.change_host()
    assert resolver.get_host_index() == 2
    resolver.change_host()
    assert resolver.get_host_index() == 0


def test_RoundRobinHostResolver():
    resolver = RoundRobinHostResolver(3)
    assert resolver.host_count == 3
    assert resolver.get_host_index() == 0
    assert resolver.get_host_index() == 1
    assert resolver.get_host_index() == 2
    assert resolver.get_host_index() == 0
    resolver.change_host()
    assert resolver.get_host_index() == 2
