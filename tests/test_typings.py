import pytest

from arangoasync.typings import JsonWrapper, KeyOptions


def test_basic_wrapper():
    wrapper = JsonWrapper({"a": 1, "b": 2})
    assert wrapper["a"] == 1
    assert wrapper["b"] == 2

    wrapper["c"] = 3
    assert wrapper["c"] == 3

    del wrapper["a"]
    assert "a" not in wrapper

    wrapper = JsonWrapper({"a": 1, "b": 2})
    keys = list(iter(wrapper))
    assert keys == ["a", "b"]
    assert len(wrapper) == 2

    assert "a" in wrapper
    assert "c" not in wrapper

    assert repr(wrapper) == "JsonWrapper({'a': 1, 'b': 2})"
    wrapper = JsonWrapper({"a": 1, "b": 2})
    assert str(wrapper) == "{'a': 1, 'b': 2}"
    assert wrapper == {"a": 1, "b": 2}

    assert wrapper.get("a") == 1
    assert wrapper.get("c", 3) == 3

    items = list(wrapper.items())
    assert items == [("a", 1), ("b", 2)]
    assert wrapper.to_dict() == {"a": 1, "b": 2}


def test_KeyOptions():
    options = KeyOptions(generator_type="autoincrement")
    options.validate()
    with pytest.raises(ValueError, match="Invalid key generator type 'invalid_type'"):
        KeyOptions(generator_type="invalid_type").validate()
    with pytest.raises(ValueError, match='"increment" value'):
        KeyOptions(generator_type="uuid", increment=5).validate()
    with pytest.raises(ValueError, match='"offset" value'):
        KeyOptions(generator_type="uuid", offset=5).validate()
    with pytest.raises(ValueError, match='"type" value'):
        KeyOptions(data={"allowUserKeys": True}).validate()
    with pytest.raises(ValueError, match='"allowUserKeys" value'):
        KeyOptions(data={"type": "autoincrement"}).validate()
