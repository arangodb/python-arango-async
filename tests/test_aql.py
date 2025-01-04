import pytest


@pytest.mark.asyncio
async def test_simple_query(db, doc_col, docs):
    await doc_col.insert(docs[0])
    aql = db.aql
    _ = await aql.execute(
        query="FOR doc IN @@collection RETURN doc",
        bind_vars={"@collection": doc_col.name},
    )
