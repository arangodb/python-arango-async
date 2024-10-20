import pytest

from arangoasync.typings import (
    CollectionInfo,
    CollectionStatus,
    CollectionType,
    JsonWrapper,
    KeyOptions,
    UserInfo,
)


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


def test_CollectionType():
    assert CollectionType.from_int(2) == CollectionType.DOCUMENT
    assert CollectionType.from_int(3) == CollectionType.EDGE
    with pytest.raises(ValueError, match="Invalid collection type value: 1"):
        CollectionType.from_int(1)
    assert CollectionType.from_str("document") == CollectionType.DOCUMENT
    assert CollectionType.from_str("edge") == CollectionType.EDGE
    with pytest.raises(ValueError, match="Invalid collection type value: invalid"):
        CollectionType.from_str("invalid")


def test_CollectionStatus():
    assert CollectionStatus.from_int(1) == CollectionStatus.NEW
    assert CollectionStatus.from_int(2) == CollectionStatus.UNLOADED
    assert CollectionStatus.from_int(3) == CollectionStatus.LOADED
    assert CollectionStatus.from_int(4) == CollectionStatus.UNLOADING
    assert CollectionStatus.from_int(5) == CollectionStatus.DELETED
    assert CollectionStatus.from_int(6) == CollectionStatus.LOADING
    with pytest.raises(ValueError, match="Invalid collection status value: 0"):
        CollectionStatus.from_int(0)


def test_CollectionInfo():
    data = {
        "id": "151",
        "name": "animals",
        "status": 3,
        "type": 2,
        "isSystem": False,
        "globallyUniqueId": "hDA74058C1843/151",
    }
    info = CollectionInfo(data)
    assert info.globally_unique_id == "hDA74058C1843/151"
    assert info.is_system is False
    assert info.name == "animals"
    assert info.status == CollectionStatus.LOADED
    assert info.col_type == CollectionType.DOCUMENT

    # Custom formatter
    formatted_data = info.format(
        lambda x: {k: v.upper() if isinstance(v, str) else v for k, v in x.items()}
    )
    assert formatted_data["name"] == "ANIMALS"

    # Default formatter
    formatted_data = info.format()
    assert formatted_data == {
        "id": "151",
        "name": "animals",
        "system": False,
        "type": "document",
        "status": "loaded",
    }


def test_UserInfo():
    data = {
        "user": "john",
        "password": "secret",
        "active": True,
        "extra": {"role": "admin"},
    }
    user_info = UserInfo(**data)
    assert user_info.user == "john"
    assert user_info.password == "secret"
    assert user_info.active is True
    assert user_info.extra == {"role": "admin"}
    assert user_info.to_dict() == data
