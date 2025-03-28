Cursors
-------

Many operations provided by python-arango-async (e.g. executing :doc:`aql` queries)
return result **cursors** to batch the network communication between ArangoDB
server and python-arango-async client. Each HTTP request from a cursor fetches the
next batch of results (usually documents). Depending on the query, the total
number of items in the result set may or may not be known in advance.
