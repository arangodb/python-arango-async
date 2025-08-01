import asyncio

import pytest

from arangoasync.errno import DATA_SOURCE_NOT_FOUND, INDEX_NOT_FOUND
from arangoasync.exceptions import (
    CollectionChecksumError,
    CollectionCompactError,
    CollectionConfigureError,
    CollectionPropertiesError,
    CollectionRecalculateCountError,
    CollectionRenameError,
    CollectionResponsibleShardError,
    CollectionRevisionError,
    CollectionShardsError,
    CollectionStatisticsError,
    CollectionTruncateError,
    DocumentCountError,
    IndexCreateError,
    IndexDeleteError,
    IndexGetError,
    IndexListError,
    IndexLoadError,
)
from tests.helpers import generate_col_name


def test_collection_attributes(db, doc_col):
    assert doc_col.db_name == db.name
    assert doc_col.name.startswith("test_collection")
    assert repr(doc_col) == f"<StandardCollection {doc_col.name}>"


@pytest.mark.asyncio
async def test_collection_misc_methods(doc_col, bad_col, docs, cluster):
    doc = await doc_col.insert(docs[0])

    # Properties
    properties = await doc_col.properties()
    assert properties.name == doc_col.name
    assert properties.is_system is False
    assert len(properties.format()) > 1
    with pytest.raises(CollectionPropertiesError):
        await bad_col.properties()

    # Configure
    wfs = not properties.wait_for_sync
    new_properties = await doc_col.configure(wait_for_sync=wfs)
    assert new_properties.wait_for_sync == wfs
    with pytest.raises(CollectionConfigureError):
        await bad_col.configure(wait_for_sync=wfs)

    # Statistics
    statistics = await doc_col.statistics()
    assert statistics.name == doc_col.name
    assert "figures" in statistics
    with pytest.raises(CollectionStatisticsError):
        await bad_col.statistics()

    # Shards
    if cluster:
        shard = await doc_col.responsible_shard(doc)
        assert isinstance(shard, str)
        with pytest.raises(CollectionResponsibleShardError):
            await bad_col.responsible_shard(doc)
        shards = await doc_col.shards(details=True)
        assert isinstance(shards, dict)
        with pytest.raises(CollectionShardsError):
            await bad_col.shards()

    # Revision
    revision = await doc_col.revision()
    assert isinstance(revision, str)
    with pytest.raises(CollectionRevisionError):
        await bad_col.revision()

    # Checksum
    checksum = await doc_col.checksum(with_rev=True, with_data=True)
    assert isinstance(checksum, str)
    with pytest.raises(CollectionChecksumError):
        await bad_col.checksum()

    # Recalculate count
    with pytest.raises(CollectionRecalculateCountError):
        await bad_col.recalculate_count()
    await doc_col.recalculate_count()

    # Compact
    with pytest.raises(CollectionCompactError):
        await bad_col.compact()
    res = await doc_col.compact()
    assert res.name == doc_col.name


@pytest.mark.asyncio
async def test_collection_rename(cluster, db, bad_col, docs):
    if cluster:
        pytest.skip("Renaming collections is not supported in cluster deployments.")

    with pytest.raises(CollectionRenameError):
        await bad_col.rename("new_name")

    col_name = generate_col_name()
    new_name = generate_col_name()
    try:
        await db.create_collection(col_name)
        col = db.collection(col_name)
        await col.rename(new_name)
        assert col.name == new_name
        doc = await col.insert(docs[0])
        assert col.get_col_name(doc) == new_name
    finally:
        db.delete_collection(new_name, ignore_missing=True)


@pytest.mark.asyncio
async def test_collection_index(doc_col, bad_col, cluster):
    # Create indexes
    idx1 = await doc_col.add_index(
        type="persistent",
        fields=["_key"],
        options={
            "unique": True,
            "name": "idx1",
        },
    )
    assert idx1.id is not None
    assert idx1.id == f"{doc_col.name}/{idx1.numeric_id}"
    assert idx1.type == "persistent"
    assert idx1["type"] == "persistent"
    assert idx1.fields == ["_key"]
    assert idx1.name == "idx1"
    assert idx1["unique"] is True
    assert idx1.unique is True
    assert idx1.format()["id"] == str(idx1.numeric_id)

    idx2 = await doc_col.add_index(
        type="inverted",
        fields=[{"name": "attr1", "cache": True}],
        options={
            "unique": False,
            "sparse": True,
            "name": "idx2",
            "storedValues": [{"fields": ["a"], "compression": "lz4", "cache": True}],
            "includeAllFields": True,
            "analyzer": "identity",
            "primarySort": {
                "cache": True,
                "fields": [{"field": "a", "direction": "asc"}],
            },
        },
    )
    assert idx2.id is not None
    assert idx2.id == f"{doc_col.name}/{idx2.numeric_id}"
    assert idx2.type == "inverted"
    assert idx2["fields"][0]["name"] == "attr1"
    assert idx2.name == "idx2"
    assert idx2.include_all_fields is True
    assert idx2.analyzer == "identity"
    assert idx2.sparse is True
    assert idx2.unique is False

    idx3 = await doc_col.add_index(
        type="geo",
        fields=["location"],
        options={
            "geoJson": True,
            "name": "idx3",
            "inBackground": True,
        },
    )
    assert idx3.id is not None
    assert idx3.type == "geo"
    assert idx3.fields == ["location"]
    assert idx3.name == "idx3"
    assert idx3.geo_json is True
    if cluster:
        assert idx3.in_background is True

    with pytest.raises(IndexCreateError):
        await bad_col.add_index(type="persistent", fields=["_key"])

    # List all indexes
    indexes = await doc_col.indexes()
    assert len(indexes) > 3, indexes
    found_idx1 = found_idx2 = found_idx3 = False
    for idx in indexes:
        if idx.id == idx1.id:
            found_idx1 = True
        elif idx.id == idx2.id:
            found_idx2 = True
        elif idx.id == idx3.id:
            found_idx3 = True
    assert found_idx1 is True, indexes
    assert found_idx2 is True, indexes
    assert found_idx3 is True, indexes

    with pytest.raises(IndexListError) as err:
        await bad_col.indexes()
    assert err.value.error_code == DATA_SOURCE_NOT_FOUND

    # Get an index
    get1, get2, get3 = await asyncio.gather(
        doc_col.get_index(idx1.id),
        doc_col.get_index(idx2.numeric_id),
        doc_col.get_index(str(idx3.numeric_id)),
    )
    assert get1.id == idx1.id
    assert get1.type == idx1.type
    assert get1.name == idx1.name
    assert get2.id == idx2.id
    assert get2.type == idx2.type
    assert get2.name == idx2.name
    assert get3.id == idx3.id
    assert get3.type == idx3.type
    assert get3.name == idx3.name

    with pytest.raises(IndexGetError) as err:
        await doc_col.get_index("non-existent")
    assert err.value.error_code == INDEX_NOT_FOUND

    # Load indexes into main memory
    assert await doc_col.load_indexes() is True
    with pytest.raises(IndexLoadError) as err:
        await bad_col.load_indexes()
    assert err.value.error_code == DATA_SOURCE_NOT_FOUND

    # Delete indexes
    del1, del2, del3 = await asyncio.gather(
        doc_col.delete_index(idx1.id),
        doc_col.delete_index(idx2.numeric_id),
        doc_col.delete_index(str(idx3.numeric_id)),
    )
    assert del1 is True
    assert del2 is True
    assert del3 is True

    # Now, the indexes should be gone
    with pytest.raises(IndexDeleteError) as err:
        await doc_col.delete_index(idx1.id)
    assert err.value.error_code == INDEX_NOT_FOUND
    assert await doc_col.delete_index(idx2.id, ignore_missing=True) is False


@pytest.mark.asyncio
async def test_collection_truncate_count(docs, doc_col, bad_col):
    # Test errors
    with pytest.raises(CollectionTruncateError):
        await bad_col.truncate()
    with pytest.raises(DocumentCountError):
        await bad_col.count()

    # Test regular operations
    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])
    cnt = await doc_col.count()
    assert cnt == len(docs)

    await doc_col.truncate()
    cnt = await doc_col.count()
    assert cnt == 0

    await asyncio.gather(*[doc_col.insert(doc) for doc in docs])
    await doc_col.truncate(wait_for_sync=True, compact=True)
    cnt = await doc_col.count()
    assert cnt == 0
