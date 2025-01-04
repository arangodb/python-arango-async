import asyncio

import pytest

from arangoasync.database import TransactionDatabase
from arangoasync.errno import BAD_PARAMETER, FORBIDDEN, TRANSACTION_NOT_FOUND
from arangoasync.exceptions import (
    TransactionAbortError,
    TransactionCommitError,
    TransactionExecuteError,
    TransactionInitError,
    TransactionStatusError,
)


@pytest.mark.asyncio
async def test_transaction_execute_raw(db, doc_col, docs):
    # Test a valid JS transaction
    doc = docs[0]
    key = doc["_key"]
    command = f"""
        function (params) {{
            var db = require('internal').db;
            db.{doc_col.name}.save({{'_key': params.key, 'val': 1}});
            return true;
        }}
    """  # noqa: E702 E231 E272 E202
    result = await db.execute_transaction(
        command=command,
        params={"key": key},
        write=[doc_col.name],
        read=[doc_col.name],
        exclusive=[doc_col.name],
        wait_for_sync=False,
        lock_timeout=1000,
        max_transaction_size=100000,
        allow_implicit=True,
    )
    assert result is True
    doc = await doc_col.get(key)
    assert doc is not None and doc["val"] == 1

    # Test an invalid transaction
    with pytest.raises(TransactionExecuteError) as err:
        await db.execute_transaction(command="INVALID COMMAND")
    assert err.value.error_code == BAD_PARAMETER


@pytest.mark.asyncio
async def test_transaction_document_insert(db, bad_db, doc_col, docs):
    # Start a basic transaction
    txn_db = await db.begin_transaction(
        read=doc_col.name,
        write=doc_col.name,
        exclusive=[],
        wait_for_sync=True,
        allow_implicit=False,
        lock_timeout=1000,
        max_transaction_size=1024 * 1024,
        skip_fast_lock_round=True,
        allow_dirty_read=False,
    )

    # Make sure the object properties are set correctly
    assert isinstance(txn_db, TransactionDatabase)
    assert txn_db.name == db.name
    assert txn_db.context == "transaction"
    assert txn_db.transaction_id is not None
    assert repr(txn_db) == f"<TransactionDatabase {db.name}>"
    txn_col = txn_db.collection(doc_col.name)
    assert txn_col.db_name == db.name

    with pytest.raises(TransactionInitError) as err:
        await bad_db.begin_transaction()
    assert err.value.error_code == FORBIDDEN

    # Insert a document in the transaction
    for doc in docs:
        result = await txn_col.insert(doc)
        assert result["_id"] == f"{doc_col.name}/{doc['_key']}"
        assert result["_key"] == doc["_key"]
        assert isinstance(result["_rev"], str)
        assert (await txn_col.get(doc["_key"]))["val"] == doc["val"]

    # Abort the transaction
    await txn_db.abort_transaction()


@pytest.mark.asyncio
async def test_transaction_status(db, doc_col):
    # Begin a transaction
    txn_db = await db.begin_transaction(read=doc_col.name)
    assert await txn_db.transaction_status() == "running"

    # Commit the transaction
    await txn_db.commit_transaction()
    assert await txn_db.transaction_status() == "committed"

    # Begin another transaction
    txn_db = await db.begin_transaction(read=doc_col.name)
    assert await txn_db.transaction_status() == "running"

    # Abort the transaction
    await txn_db.abort_transaction()
    assert await txn_db.transaction_status() == "aborted"

    # Test with an illegal transaction ID
    txn_db = db.fetch_transaction("illegal")
    with pytest.raises(TransactionStatusError) as err:
        await txn_db.transaction_status()
    # Error code differs between single server and cluster mode
    assert err.value.error_code in {BAD_PARAMETER, TRANSACTION_NOT_FOUND}


@pytest.mark.asyncio
async def test_transaction_commit(db, doc_col, docs):
    # Begin a transaction
    txn_db = await db.begin_transaction(
        read=doc_col.name,
        write=doc_col.name,
    )
    txn_col = txn_db.collection(doc_col.name)

    # Insert documents in the transaction
    assert "_rev" in await txn_col.insert(docs[0])
    assert "_rev" in await txn_col.insert(docs[2])
    await txn_db.commit_transaction()
    assert await txn_db.transaction_status() == "committed"

    # Check the documents, after transaction has been committed
    doc = await doc_col.get(docs[2]["_key"])
    assert doc["_key"] == docs[2]["_key"]
    assert doc["val"] == docs[2]["val"]

    # Test with an illegal transaction ID
    txn_db = db.fetch_transaction("illegal")
    with pytest.raises(TransactionCommitError) as err:
        await txn_db.commit_transaction()
    # Error code differs between single server and cluster mode
    assert err.value.error_code in {BAD_PARAMETER, TRANSACTION_NOT_FOUND}


@pytest.mark.asyncio
async def test_transaction_abort(db, doc_col, docs):
    # Begin a transaction
    txn_db = await db.begin_transaction(
        read=doc_col.name,
        write=doc_col.name,
    )
    txn_col = txn_db.collection(doc_col.name)

    # Insert documents in the transaction
    assert "_rev" in await txn_col.insert(docs[0])
    assert "_rev" in await txn_col.insert(docs[2])
    await txn_db.abort_transaction()
    assert await txn_db.transaction_status() == "aborted"

    # Check the documents, after transaction has been aborted
    assert await doc_col.get(docs[2]["_key"]) is None

    # Test with an illegal transaction ID
    txn_db = db.fetch_transaction("illegal")
    with pytest.raises(TransactionAbortError) as err:
        await txn_db.abort_transaction()
    # Error code differs between single server and cluster mode
    assert err.value.error_code in {BAD_PARAMETER, TRANSACTION_NOT_FOUND}


@pytest.mark.asyncio
async def test_transaction_fetch_existing(db, doc_col, docs):
    # Begin a transaction
    txn_db = await db.begin_transaction(
        read=doc_col.name,
        write=doc_col.name,
    )
    txn_col = txn_db.collection(doc_col.name)

    # Insert documents in the transaction
    assert "_rev" in await txn_col.insert(docs[0])
    assert "_rev" in await txn_col.insert(docs[1])

    txn_db2 = db.fetch_transaction(txn_db.transaction_id)
    assert txn_db2.transaction_id == txn_db.transaction_id
    txn_col2 = txn_db2.collection(doc_col.name)
    assert "_rev" in await txn_col2.insert(docs[2])

    await txn_db2.commit_transaction()
    assert await txn_db.transaction_status() == "committed"
    assert await txn_db2.transaction_status() == "committed"

    # Check the documents, after transaction has been aborted
    assert all(
        await asyncio.gather(*(doc_col.get(docs[idx]["_key"]) for idx in range(3)))
    )


@pytest.mark.asyncio
async def test_transaction_list(db):
    # There should be no transactions initially
    assert await db.list_transactions() == []

    # Begin a transaction
    txn_db1 = await db.begin_transaction()
    tx_ls = await db.list_transactions()
    assert len(tx_ls) == 1
    assert any(txn_db1.transaction_id == tx["id"] for tx in tx_ls)

    # Begin another transaction
    txn_db2 = await db.begin_transaction()
    tx_ls = await db.list_transactions()
    assert len(tx_ls) == 2
    assert any(txn_db1.transaction_id == tx["id"] for tx in tx_ls)
    assert any(txn_db2.transaction_id == tx["id"] for tx in tx_ls)

    # Only the first transaction should be running after aborting the second
    await txn_db2.abort_transaction()
    tx_ls = await db.list_transactions()
    assert len(tx_ls) == 1
    assert any(txn_db1.transaction_id == tx["id"] for tx in tx_ls)

    # Commit the first transaction, no transactions should be left
    await txn_db1.commit_transaction()
    tx_ls = await db.list_transactions()
    assert len(tx_ls) == 0
