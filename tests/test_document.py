import asyncio

import pytest

from arangoasync.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
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

    # Test wrong revision
    with pytest.raises(DocumentRevisionError):
        await doc_col.update(new_value, if_match="foobar")

    # Update using correct revision
    doc["val"] = 24
    result = await doc_col.update(new_value, silent=True, if_match=new_value["_rev"])
    assert result is True


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

    # Test wrong revision
    with pytest.raises(DocumentRevisionError):
        await doc_col.replace(new_value, if_match="foobar")

    # Replace using correct revision
    doc["foo"] = "foobar"
    result = await doc_col.replace(new_value, silent=True, if_match=new_value["_rev"])
    assert result is True


@pytest.mark.asyncio
async def test_document_delete(doc_col, bad_col, docs):
    # Test deleting a non-existent document
    with pytest.raises(DocumentDeleteError):
        await bad_col.delete({"_key": "non-existent"})
    deleted = await doc_col.delete({"_key": "non-existent"}, ignore_missing=True)
    assert deleted is False

    # Verbose delete
    doc = docs[0]
    inserted = await doc_col.insert(doc)
    deleted = await doc_col.delete(doc, return_old=True)
    assert deleted["_key"] == inserted["_key"]

    # Silent delete
    await doc_col.insert(doc)
    deleted = await doc_col.delete(doc, silent=True, ignore_missing=True)
    assert deleted is True

    # Test wrong revision
    inserted = await doc_col.insert(doc)
    with pytest.raises(DocumentRevisionError):
        await doc_col.delete(inserted, if_match="foobar")

    # Delete using correct revision
    deleted = await doc_col.delete(doc, silent=True, if_match=inserted["_rev"])
    assert deleted is True


@pytest.mark.asyncio
async def test_document_get(doc_col, bad_col, docs):
    # Test getting a non-existent document
    with pytest.raises(DocumentGetError):
        await bad_col.get({"_key": "non-existent"})
    result = await doc_col.get({"_key": "non-existent"})
    assert result is None

    doc = docs[0]
    inserted = await doc_col.insert(doc)
    result = await doc_col.get(doc)
    assert result["_key"] == inserted["_key"]

    # Test with good revision
    result = await doc_col.get(inserted["_key"], if_match=inserted["_rev"])
    assert result["_key"] == inserted["_key"]

    # Test with non-matching revision
    result = await doc_col.get(inserted["_id"], if_none_match="foobar")
    assert result["_key"] == inserted["_key"]

    # Test with incorrect revision
    with pytest.raises(DocumentGetError):
        await doc_col.get(inserted["_id"], if_none_match=inserted["_rev"])
    with pytest.raises(DocumentRevisionError):
        await doc_col.get(inserted["_id"], if_match="foobar")


@pytest.mark.asyncio
async def test_document_has(doc_col, bad_col, docs):
    # Test getting a non-existent document
    result = await bad_col.has({"_key": "non-existent"})
    assert result is False
    result = await doc_col.has({"_key": "non-existent"})
    assert result is False

    doc = docs[0]
    inserted = await doc_col.insert(doc)
    result = await doc_col.has(doc)
    assert result is True

    # Test with good revision
    result = await doc_col.has(inserted["_key"], if_match=inserted["_rev"])
    assert result is True

    # Test with non-matching revision
    result = await doc_col.has(inserted["_id"], if_none_match="foobar")
    assert result is True

    # Test with incorrect revision
    with pytest.raises(DocumentGetError):
        await doc_col.has(inserted["_id"], if_none_match=inserted["_rev"])
    with pytest.raises(DocumentRevisionError):
        await doc_col.has(inserted["_id"], if_match="foobar")


@pytest.mark.asyncio
async def test_document_get_many(doc_col, bad_col, docs):
    # Test with invalid collection
    with pytest.raises(DocumentGetError):
        await bad_col.get_many(["non-existent"])

    # Insert all documents first
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    # Test with good keys
    many = await doc_col.get_many([doc["_key"] for doc in docs])
    assert len(many) == len(docs)

    # Test with full documents
    many = await doc_col.get_many(docs)
    assert len(many) == len(docs)

    # Revs
    bad_rev = many
    bad_rev[0]["_rev"] = "foobar"
    many = await doc_col.get_many([bad_rev[0]], ignore_revs=True)
    assert len(many) == 1
    assert "error" not in many[0]
    many = await doc_col.get_many([bad_rev[0]], ignore_revs=False)
    assert len(many) == 1
    assert "error" in many[0]

    # Empty list
    many = await doc_col.get_many([])
    assert len(many) == 0
