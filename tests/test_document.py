import pytest

from arangoasync.exceptions import (
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentUpdateError,
)
from tests.helpers import generate_col_name


@pytest.mark.asyncio
async def test_document_insert(doc_col, bad_col, docs):
    # Test insert document with no key
    result = await doc_col.insert({})
    assert await doc_col.get(result["_key"]) is not None

    # Test insert document with ID
    doc_id = f"{doc_col.name}/foo"
    result = await doc_col.insert({"_id": doc_id})
    assert doc_id == result["_id"]
    assert result["_key"] == "foo"
    assert await doc_col.get("foo") is not None
    assert await doc_col.get(doc_id) is not None

    with pytest.raises(DocumentParseError) as err:
        await doc_col.insert({"_id": f"{generate_col_name()}/foo"})
    assert "Bad collection name" in err.value.message

    with pytest.raises(DocumentInsertError):
        await bad_col.insert({})

    # Test insert with default options
    for doc in docs:
        result = await doc_col.insert(doc)
        assert result["_id"] == f"{doc_col.name}/{doc['_key']}"
        assert result["_key"] == doc["_key"]
        assert isinstance(result["_rev"], str)
        assert (await doc_col.get(doc["_key"]))["val"] == doc["val"]


@pytest.mark.asyncio
async def test_document_update(doc_col, bad_col, docs):
    # Test updating a non-existent document
    with pytest.raises(DocumentUpdateError):
        await bad_col.update({"_key": "non-existent", "val": 42})

    # Verbose update
    doc = docs[0]
    assert doc["val"] != 42
    await doc_col.insert(doc)
    doc["val"] = 42
    updated = await doc_col.update(doc, return_old=True, return_new=True)
    assert updated["_key"] == doc["_key"]
    assert "old" in updated
    assert "new" in updated
    new_value = await doc_col.get(doc)
    assert new_value["val"] == doc["val"]

    # Silent update
    doc["val"] = None
    updated = await doc_col.update(doc, silent=True, keep_null=False)
    assert updated is True
    new_value = await doc_col.get(doc)
    assert "val" not in new_value


@pytest.mark.asyncio
async def test_document_replace(doc_col, bad_col, docs):
    # Test updating a non-existent document
    with pytest.raises(DocumentReplaceError):
        await bad_col.replace({"_key": "non-existent", "val": 42})

    # Verbose replace
    doc = docs[0]
    assert doc["val"] != 42
    await doc_col.insert(doc)
    doc["val"] = 42
    doc.pop("loc")
    doc.pop("text")
    replaced = await doc_col.replace(doc, return_old=True, return_new=True)
    assert replaced["_key"] == doc["_key"]
    new_value = await doc_col.get(doc)
    assert new_value["val"] == doc["val"]
    assert "text" not in new_value
    assert "loc" not in new_value
    assert "new" in replaced
    assert "old" in replaced

    # Silent replace
    doc["text"] = "abcd"
    doc["new_entry"] = 3.14
    replaced = await doc_col.replace(doc, silent=True)
    assert replaced is True
    new_value = await doc_col.get(doc)
    assert new_value["text"] == doc["text"]
    assert new_value["new_entry"] == doc["new_entry"]
