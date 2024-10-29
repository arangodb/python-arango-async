import pytest

from arangoasync.exceptions import CollectionPropertiesError


def test_collection_attributes(db, doc_col):
    assert doc_col.db_name == db.name
    assert doc_col.name.startswith("test_collection")
    assert repr(doc_col) == f"<StandardCollection {doc_col.name}>"


@pytest.mark.asyncio
async def test_collection_misc_methods(doc_col, bad_col):
    # Properties
    properties = await doc_col.properties()
    assert properties.name == doc_col.name
    assert properties.is_system is False
    assert len(properties.format()) > 1
    with pytest.raises(CollectionPropertiesError):
        await bad_col.properties()
