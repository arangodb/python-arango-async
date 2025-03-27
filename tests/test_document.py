import asyncio

import pytest
from packaging import version

from arangoasync.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    SortValidationError,
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

    # Test with partially good keys
    keys = [doc["_key"] for doc in docs]
    keys.append("invalid_key")
    many = await doc_col.get_many(keys)
    assert len(many) == len(keys)
    assert "error" in many[-1]

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


@pytest.mark.asyncio
async def test_document_find(doc_col, bad_col, docs):
    # Check errors first
    with pytest.raises(DocumentGetError):
        await bad_col.find()
    with pytest.raises(ValueError):
        await doc_col.find(limit=-1)
    with pytest.raises(ValueError):
        await doc_col.find(skip="abcd")
    with pytest.raises(ValueError):
        await doc_col.find(filters="abcd")
    with pytest.raises(SortValidationError):
        await doc_col.find(sort="abcd")
    with pytest.raises(SortValidationError):
        await doc_col.find(sort=[{"x": "text", "sort_order": "ASC"}])

    # Insert all documents
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])

    # Empty find
    filter_docs = []
    async for doc in await doc_col.find():
        filter_docs.append(doc)
    assert len(filter_docs) == len(docs)

    # Test with filter
    filter_docs = []
    async for doc in await doc_col.find(filters={"val": 42}):
        filter_docs.append(doc)
    assert len(filter_docs) == 0
    async for doc in await doc_col.find(filters={"text": "foo"}):
        filter_docs.append(doc)
    assert len(filter_docs) == 3
    filter_docs = []
    async for doc in await doc_col.find(filters={"text": "foo", "val": 1}):
        filter_docs.append(doc)
    assert len(filter_docs) == 1

    # Test with limit
    filter_docs = []
    async for doc in await doc_col.find(limit=2):
        filter_docs.append(doc)
    assert len(filter_docs) == 2

    # Test with skip
    filter_docs = []
    async for doc in await doc_col.find(skip=2, allow_dirty_read=True):
        filter_docs.append(doc)
    assert len(filter_docs) == len(docs) - 2

    # Test with sort
    filter_docs = []
    async for doc in await doc_col.find(
        {}, sort=[{"sort_by": "text", "sort_order": "ASC"}]
    ):
        filter_docs.append(doc)

    for idx in range(len(filter_docs) - 1):
        assert filter_docs[idx]["text"] <= filter_docs[idx + 1]["text"]


@pytest.mark.asyncio
async def test_document_insert_many(cluster, db_version, doc_col, bad_col, docs):
    # Check errors
    with pytest.raises(DocumentInsertError):
        await bad_col.insert_many(docs)

    # Insert all documents
    result = await doc_col.insert_many(docs, return_new=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" not in res

    # Empty list
    result = await doc_col.insert_many([])
    assert len(result) == 0

    # Insert again (should not work due to unique constraint)
    result = await doc_col.insert_many(docs)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res

    # Silent mode
    if cluster and db_version < version.parse("3.12.0"):
        pytest.skip("Skipping silent option")

    result = await doc_col.insert_many(docs, silent=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res
    await doc_col.truncate()
    result = await doc_col.insert_many(docs, silent=True)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_document_replace_many(cluster, db_version, doc_col, bad_col, docs):
    # Check errors
    with pytest.raises(DocumentReplaceError):
        await bad_col.replace_many(docs)

    # Empty list
    result = await doc_col.replace_many([])
    assert len(result) == 0

    # Replace "not found" documents
    result = await doc_col.replace_many(docs, return_new=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res

    # Replace successfully
    result = await doc_col.insert_many(docs, return_new=True)
    replacements = []
    for doc in result:
        replacements.append({"_key": doc["_key"], "val": 42})
    result = await doc_col.replace_many(replacements, return_new=True)
    assert len(result) == len(docs)
    for doc in result:
        assert doc["new"]["val"] == 42
        assert "text" not in doc["new"]

    # Silent mode
    if cluster and db_version < version.parse("3.12.0"):
        pytest.skip("Skipping silent option")

    result = await doc_col.replace_many(docs, silent=True)
    assert len(result) == 0
    await doc_col.truncate()
    result = await doc_col.replace_many(docs, silent=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res


@pytest.mark.asyncio
async def test_document_update_many(db_version, cluster, doc_col, bad_col, docs):
    # Check errors
    with pytest.raises(DocumentUpdateError):
        await bad_col.update_many(docs)

    # Empty list
    result = await doc_col.update_many([])
    assert len(result) == 0

    # Update "not found" documents
    result = await doc_col.update_many(docs, return_new=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res

    # Update successfully
    result = await doc_col.insert_many(docs, return_new=True)
    updates = []
    for doc in result:
        updates.append({"_key": doc["_key"], "val": 42})
    result = await doc_col.update_many(updates, return_new=True)
    assert len(result) == len(docs)
    for doc in result:
        assert doc["new"]["val"] == 42
        assert "text" in doc["new"]

    # Silent mode
    if cluster and db_version < version.parse("3.12.0"):
        pytest.skip("Skipping silent option")

    result = await doc_col.update_many(docs, silent=True)
    assert len(result) == 0
    await doc_col.truncate()
    result = await doc_col.update_many(docs, silent=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res


@pytest.mark.asyncio
async def test_document_delete_many(db_version, cluster, doc_col, bad_col, docs):
    # Check errors
    with pytest.raises(DocumentDeleteError):
        await bad_col.delete_many(docs)

    # Empty list
    result = await doc_col.delete_many([])
    assert len(result) == 0

    # Delete "not found" documents
    result = await doc_col.delete_many(docs, return_old=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res

    # Delete successfully
    result = await doc_col.insert_many(docs, return_new=True)
    deleted = []
    for doc in result:
        deleted.append({"_key": doc["_key"], "val": 42})
    result = await doc_col.delete_many(deleted, return_old=True)
    assert len(result) == len(docs)

    # Wrong and right rev
    result = await doc_col.insert_many(docs, return_new=True)
    deleted = [result[0]["new"], result[1]["new"]]
    deleted[1]["_rev"] = "foobar"
    result = await doc_col.delete_many(deleted, ignore_revs=False)
    assert "_key" in result[0]
    assert "error" in result[1]

    # Silent mode
    if cluster and db_version < version.parse("3.12.0"):
        pytest.skip("Skipping silent option")

    await doc_col.truncate()
    _ = await doc_col.insert_many(docs)
    result = await doc_col.delete_many(docs, silent=True)
    assert len(result) == 0
    result = await doc_col.delete_many(docs, silent=True)
    assert len(result) == len(docs)
    for res in result:
        assert "error" in res


@pytest.mark.asyncio
async def test_document_update_match(doc_col, bad_col, docs):
    # Check errors first
    with pytest.raises(DocumentUpdateError):
        await bad_col.update_match({}, {})
    with pytest.raises(ValueError):
        await doc_col.update_match({}, {}, limit=-1)
    with pytest.raises(ValueError):
        await doc_col.update_match("abcd", {})

    # Insert all documents
    await doc_col.insert_many(docs)

    # Update value for all documents
    count = await doc_col.update_match({}, {"val": 42})
    async for doc in await doc_col.find():
        assert doc["val"] == 42
    assert count == len(docs)

    # Update documents partially
    count = await doc_col.update_match({"text": "foo"}, {"val": 24})
    async for doc in await doc_col.find():
        if doc["text"] == "foo":
            assert doc["val"] == 24
    assert count == sum([1 for doc in docs if doc["text"] == "foo"])

    # No matching documents
    count = await doc_col.update_match({"text": "no_matching"}, {"val": -1})
    async for doc in await doc_col.find():
        assert doc["val"] != -1
    assert count == 0


@pytest.mark.asyncio
async def test_document_replace_match(doc_col, bad_col, docs):
    # Check errors first
    with pytest.raises(DocumentReplaceError):
        await bad_col.replace_match({}, {})
    with pytest.raises(ValueError):
        await doc_col.replace_match({}, {}, limit=-1)
    with pytest.raises(ValueError):
        await doc_col.replace_match("abcd", {})

    # Replace all documents
    await doc_col.insert_many(docs)
    count = await doc_col.replace_match({}, {"replacement": 42})
    async for doc in await doc_col.find():
        assert "replacement" in doc
        assert "val" not in doc
    assert count == len(docs)
    await doc_col.truncate()

    # Replace documents partially
    await doc_col.insert_many(docs)
    count = await doc_col.replace_match({"text": "foo"}, {"replacement": 24})
    async for doc in await doc_col.find():
        if doc.get("text") == "bar":
            assert "replacement" not in doc
        else:
            assert "replacement" in doc
    assert count == sum([1 for doc in docs if doc["text"] == "foo"])
    await doc_col.truncate()

    # No matching documents
    await doc_col.insert_many(docs)
    count = await doc_col.replace_match({"text": "no_matching"}, {"val": -1})
    async for doc in await doc_col.find():
        assert doc["val"] != -1
    assert count == 0


@pytest.mark.asyncio
async def test_document_delete_match(doc_col, bad_col, docs):
    # Check errors first
    with pytest.raises(DocumentDeleteError):
        await bad_col.delete_match({})
    with pytest.raises(ValueError):
        await doc_col.delete_match({}, limit=-1)
    with pytest.raises(ValueError):
        await doc_col.delete_match("abcd")

    # Delete all documents
    await doc_col.insert_many(docs)
    count = await doc_col.delete_match({})
    assert count == len(docs)
    assert await doc_col.count() == 0

    # Delete documents partially
    await doc_col.insert_many(docs)
    count = await doc_col.delete_match({"text": "foo"})
    async for doc in await doc_col.find():
        assert doc["text"] != "foo"
    assert count == sum([1 for doc in docs if doc["text"] == "foo"])
    await doc_col.truncate()

    # No matching documents
    await doc_col.insert_many(docs)
    count = await doc_col.delete_match({"text": "no_matching"})
    assert count == 0
