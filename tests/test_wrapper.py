from arangoasync.wrapper import Wrapper


def test_basic_wrapper():
    wrapper = Wrapper({"a": 1, "b": 2})
    assert wrapper["a"] == 1
    assert wrapper["b"] == 2

    wrapper["c"] = 3
    assert wrapper["c"] == 3

    del wrapper["a"]
    assert "a" not in wrapper

    wrapper = Wrapper({"a": 1, "b": 2})
    keys = list(iter(wrapper))
    assert keys == ["a", "b"]
    assert len(wrapper) == 2

    assert "a" in wrapper
    assert "c" not in wrapper

    assert repr(wrapper) == "Wrapper({'a': 1, 'b': 2})"
    wrapper = Wrapper({"a": 1, "b": 2})
    assert str(wrapper) == "{'a': 1, 'b': 2}"
    assert wrapper == {"a": 1, "b": 2}

    assert wrapper.get("a") == 1
    assert wrapper.get("c", 3) == 3

    items = list(wrapper.items())
    assert items == [("a", 1), ("b", 2)]
