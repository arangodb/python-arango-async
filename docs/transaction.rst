Transactions
------------

In **transactions**, requests to ArangoDB server are committed as a single,
logical unit of work (ACID compliant).

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for "students" collection.
        students = db.collection("students")

        # Begin a transaction. Read and write collections must be declared ahead of
        # time. This returns an instance of TransactionDatabase, database-level
        # API wrapper tailored specifically for executing transactions.
        txn_db = await db.begin_transaction(read=students.name, write=students.name)

        # The API wrapper is specific to a single transaction with a unique ID.
        trx_id = txn_db.transaction_id

        # Child wrappers are also tailored only for the specific transaction.
        txn_aql = txn_db.aql
        txn_col = txn_db.collection("students")

        # API execution context is always set to "transaction".
        assert txn_db.context == "transaction"
        assert txn_aql.context == "transaction"
        assert txn_col.context == "transaction"

        assert "_rev" in await txn_col.insert({"_key": "Abby"})
        assert "_rev" in await txn_col.insert({"_key": "John"})
        assert "_rev" in await txn_col.insert({"_key": "Mary"})

        # Check the transaction status.
        status = await txn_db.transaction_status()

        # Commit the transaction.
        await txn_db.commit_transaction()
        assert await students.has("Abby")
        assert await students.has("John")
        assert await students.has("Mary")
        assert await students.count() == 3

        # Begin another transaction. Note that the wrappers above are specific to
        # the last transaction and cannot be reused. New ones must be created.
        txn_db = await db.begin_transaction(read=students.name, write=students.name)
        txn_col = txn_db.collection("students")
        assert "_rev" in await txn_col.insert({"_key": "Kate"})
        assert "_rev" in await txn_col.insert({"_key": "Mike"})
        assert "_rev" in await txn_col.insert({"_key": "Lily"})
        assert await txn_col.count() == 6

        # Abort the transaction
        await txn_db.abort_transaction()
        assert not await students.has("Kate")
        assert not await students.has("Mike")
        assert not await students.has("Lily")
        assert await students.count() == 3 # transaction is aborted so txn_col cannot be used

        # Fetch an existing transaction. Useful if you have received a Transaction ID
        # from an external system.
        original_txn = await db.begin_transaction(write='students')
        txn_col = original_txn.collection('students')
        assert '_rev' in await txn_col.insert({'_key': 'Chip'})
        txn_db = db.fetch_transaction(original_txn.transaction_id)
        txn_col = txn_db.collection('students')
        assert '_rev' in await txn_col.insert({'_key': 'Alya'})
        await  txn_db.abort_transaction()

See :class:`arangoasync.database.TransactionDatabase` for API specification.
