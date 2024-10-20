import pytest

from arangoasync.exceptions import DocumentParseError
from tests.helpers import generate_col_name


@pytest.mark.asyncio
async def test_document_insert(doc_col, docs):
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

    # Test insert with default options
    for doc in docs:
        result = await doc_col.insert(doc)
        assert result["_id"] == f"{doc_col.name}/{doc['_key']}"
        assert result["_key"] == doc["_key"]
        assert isinstance(result["_rev"], str)
        assert (await doc_col.get(doc["_key"]))["val"] == doc["val"]
