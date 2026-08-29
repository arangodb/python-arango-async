import asyncio

import pytest
from packaging import version

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
    DocumentInsertError,
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
    with pytest.raises(ValueError):
        await doc_col.configure(schema={})

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
        await db.delete_collection(new_name, ignore_missing=True)


@pytest.mark.asyncio
async def test_collection_index(doc_col, bad_col, cluster, db_version):
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

    # Create vector indexes using the fixed nLists format supported by older servers.
    docs = []
    for key in range(100):
        docs.append(
            {
                "_key": f"key_{key}",
                "embedding1": [1] * 128,
                "embedding2": [1] * 128,
            }
        )
    await doc_col.insert_many(docs)
    idx4 = await doc_col.add_index(
        "vector",
        ["embedding1"],
        {
            "name": "vector_index_1",
            "params": {
                "metric": "cosine",
                "dimension": 128,
                "nLists": 2,
            },
        },
    )
    idx5 = await doc_col.add_index(
        "vector",
        ["embedding2"],
        {
            "name": "vector_index_2",
            "params": {
                "metric": "cosine",
                "dimension": 128,
                "nLists": 3,
            },
        },
    )
    assert idx4.name == "vector_index_1"
    assert idx5.name == "vector_index_2"

    if db_version >= version.parse("3.12.10"):
        # Hidden listing details expose resolved vector-index settings per shard.
        indexes = {idx.id: idx for idx in await doc_col.indexes(with_hidden=True)}
        for index in (idx4, idx5):
            shards = indexes[index.id].shards
            assert shards is not None
            for status in shards.values():
                assert {
                    "trainingState",
                    "error",
                    "resolvedNLists",
                } <= status.keys()
                assert isinstance(status["resolvedNLists"], int)

    # Delete indexes
    del1, del2, del3, del4, del5 = await asyncio.gather(
        doc_col.delete_index(idx1.id),
        doc_col.delete_index(idx2.numeric_id),
        doc_col.delete_index(str(idx3.numeric_id)),
        doc_col.delete_index(idx4.id),
        doc_col.delete_index(idx5.id),
    )
    assert del1 is True
    assert del2 is True
    assert del3 is True
    assert del4 is True
    assert del5 is True

    if db_version >= version.parse("3.12.10"):
        # Let the server choose nLists, then supply an explicit scaling object.
        scaling_n_lists = {
            "strategy": "autoSqrt",
            "multiplier": 1,
            "minNLists": 2,
            "tiers": [],
        }
        default_index = await doc_col.add_index(
            "vector",
            ["embedding1"],
            {
                "name": "vector_index_default",
                "params": {"metric": "cosine", "dimension": 128},
            },
        )
        scaling_index = await doc_col.add_index(
            "vector",
            ["embedding2"],
            {
                "name": "vector_index_scaling",
                "params": {
                    "metric": "cosine",
                    "dimension": 128,
                    "nLists": scaling_n_lists,
                    "numberOfDocsPerCentroid": 10,
                    "factory": "IVF{},Flat",
                },
            },
        )

        default_n_lists = default_index["params"]["nLists"]
        assert default_n_lists["strategy"] == "autoSqrt"
        assert default_n_lists["multiplier"] == 4
        assert default_n_lists["minNLists"] == 2
        assert scaling_index["params"]["nLists"] == scaling_n_lists
        assert scaling_index["params"]["numberOfDocsPerCentroid"] == 10
        assert scaling_index["params"]["factory"] == "IVF{},Flat"

        await doc_col.delete_index(default_index.id)
        await doc_col.delete_index(scaling_index.id)

        # A permanent training failure still creates an unusable index.
        unusable_index = await doc_col.add_index(
            "vector",
            ["embedding1"],
            {
                "name": "vector_index_unusable",
                "params": {
                    "metric": "cosine",
                    "dimension": 128,
                    "nLists": 2,
                    "factory": "IVF3,Flat",
                },
            },
        )
        assert unusable_index.training_state == "unusable"
        assert unusable_index.error_message
        await doc_col.delete_index(unusable_index.id)

        # Invalid requests continue to fail at the HTTP layer.
        with pytest.raises(IndexCreateError) as err:
            await doc_col.add_index(
                "vector",
                ["embedding1"],
                {
                    "name": "vector_index_invalid",
                    "params": {
                        "metric": "cosine",
                        "dimension": 128,
                        "nLists": 0,
                    },
                },
            )
        assert err.value.http_code == 400

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


@pytest.mark.asyncio
async def test_collection_import_bulk(doc_col, bad_col, docs):
    documents = "\n".join(doc_col.serializer.dumps(doc) for doc in docs)

    # Test errors
    with pytest.raises(DocumentInsertError):
        await bad_col.import_bulk(documents, doc_type="documents")

    # Insert documents in bulk
    result = await doc_col.import_bulk(documents, doc_type="documents")

    # Verify the documents were inserted
    count = await doc_col.count()
    assert count == len(docs)
    assert result["created"] == count
