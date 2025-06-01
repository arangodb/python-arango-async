Graphs
------

A **graph** consists of vertices and edges. Vertices are stored as documents in
:ref:`vertex collections <vertex-collections>` and edges stored as documents in
:ref:`edge collections <edge-collections>`. The collections used in a graph and
their relations are specified with :ref:`edge definitions <edge-definitions>`.
For more information, refer to `ArangoDB Manual`_.

.. _ArangoDB Manual: https://docs.arangodb.com

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # List existing graphs in the database.
        await db.graphs()

        # Create a new graph named "school" if it does not already exist.
        # This returns an API wrapper for "school" graph.
        if await db.has_graph("school"):
            school = db.graph("school")
        else:
            school = await db.create_graph("school")

        # Retrieve various graph properties.
        graph_name = school.name
        db_name = school.db_name
        vcols = await school.vertex_collections()
        ecols = await school.edge_definitions()

        # Delete the graph.
        await db.delete_graph("school")

.. _edge-definitions:

Edge Definitions
================

An **edge definition** specifies a directed relation in a graph. A graph can
have arbitrary number of edge definitions. Each edge definition consists of the
following components:

* **From Vertex Collections:** contain "_from" vertices referencing "_to" vertices.
* **To Vertex Collections:** contain "_to" vertices referenced by "_from" vertices.
* **Edge Collection:** contains edges that link "_from" and "_to" vertices.

Here is an example body of an edge definition:

.. code-block:: python

    {
        "edge_collection": "teach",
        "from_vertex_collections": ["teachers"],
        "to_vertex_collections": ["lectures"]
    }

Here is an example showing how edge definitions are managed:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        if await db.has_graph("school"):
            school = db.graph("school")
        else:
            school = await db.create_graph("school")

        # Create an edge definition named "teach". This creates any missing
        # collections and returns an API wrapper for "teach" edge collection.
        # At first, create a wrong teachers->teachers mapping intentionally.
        if not await school.has_edge_definition("teach"):
            await school.create_edge_definition(
                edge_collection="teach",
                from_vertex_collections=["teachers"],
                to_vertex_collections=["teachers"]
            )

        # List edge definitions.
        edge_defs = await school.edge_definitions()

        # Replace with the correct edge definition.
        await school.replace_edge_definition(
            edge_collection="teach",
            from_vertex_collections=["teachers"],
            to_vertex_collections=["lectures"]
        )

        # Delete the edge definition (and its collections).
        await school.delete_edge_definition("teach", drop_collections=True)

.. _vertex-collections:

Vertex Collections
==================

A **vertex collection** contains vertex documents, and shares its namespace
with all other types of collections. Each graph can have an arbitrary number of
vertex collections. Vertex collections that are not part of any edge definition
are called **orphan collections**. You can manage vertex documents via standard
collection API wrappers, but using vertex collection API wrappers provides
additional safeguards:

* All modifications are executed in transactions.
* If a vertex is deleted, all connected edges are also automatically deleted.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        school = db.graph("school")

        # Create a new vertex collection named "teachers" if it does not exist.
        # This returns an API wrapper for "teachers" vertex collection.
        if await school.has_vertex_collection("teachers"):
            teachers = school.vertex_collection("teachers")
        else:
            teachers = await school.create_vertex_collection("teachers")

        # List vertex collections in the graph.
        cols = await school.vertex_collections()

        # Vertex collections have similar interface as standard collections.
        props = await teachers.properties()
        await teachers.insert({"_key": "jon", "name": "Jon"})
        await teachers.update({"_key": "jon", "age": 35})
        await teachers.replace({"_key": "jon", "name": "Jon", "age": 36})
        await teachers.get("jon")
        await teachers.has("jon")
        await teachers.delete("jon")

You can manage vertices via graph API wrappers also, but you must use document
IDs instead of keys where applicable.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        school = db.graph("school")

        # Create a new vertex collection named "lectures" if it does not exist.
        # This returns an API wrapper for "lectures" vertex collection.
        if await school.has_vertex_collection("lectures"):
            school.vertex_collection("lectures")
        else:
            await school.create_vertex_collection("lectures")

        # The "_id" field is required instead of "_key" field (except for insert).
        await school.insert_vertex("lectures", {"_key": "CSC101"})
        await school.update_vertex({"_id": "lectures/CSC101", "difficulty": "easy"})
        await school.replace_vertex({"_id": "lectures/CSC101", "difficulty": "hard"})
        await school.has_vertex("lectures/CSC101")
        await school.vertex("lectures/CSC101")
        await school.delete_vertex("lectures/CSC101")

See :class:`arangoasync.graph.Graph` and :class:`arangoasync.collection.VertexCollection` for API specification.

.. _edge-collections:

Edge Collections
================

An **edge collection** contains :ref:`edge documents <edge-documents>`, and
shares its namespace with all other types of collections. You can manage edge
documents via standard collection API wrappers, but using edge collection API
wrappers provides additional safeguards:

* All modifications are executed in transactions.
* Edge documents are checked against the edge definitions on insert.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        if await db.has_graph("school"):
            school = db.graph("school")
        else:
            school = await db.create_graph("school")

        if not await school.has_vertex_collection("lectures"):
            await school.create_vertex_collection("lectures")
        await school.insert_vertex("lectures", {"_key": "CSC101"})

        if not await school.has_vertex_collection("teachers"):
            await school.create_vertex_collection("teachers")
        await school.insert_vertex("teachers", {"_key": "jon"})

        # Get the API wrapper for edge collection "teach".
        if await school.has_edge_definition("teach"):
            teach = school.edge_collection("teach")
        else:
            teach = await school.create_edge_definition(
                edge_collection="teach",
                from_vertex_collections=["teachers"],
                to_vertex_collections=["lectures"]
            )

        # Edge collections have a similar interface as standard collections.
        await teach.insert({
            "_key": "jon-CSC101",
            "_from": "teachers/jon",
            "_to": "lectures/CSC101"
        })
        await teach.replace({
            "_key": "jon-CSC101",
            "_from": "teachers/jon",
            "_to": "lectures/CSC101",
            "online": False
        })
        await teach.update({
            "_key": "jon-CSC101",
            "online": True
        })
        await teach.has("jon-CSC101")
        await teach.get("jon-CSC101")
        await teach.delete("jon-CSC101")

        # Create an edge between two vertices (essentially the same as insert).
        await teach.link("teachers/jon", "lectures/CSC101", data={"online": False})

        # List edges going in/out of a vertex.
        inbound = await teach.edges("teachers/jon", direction="in")
        outbound = await teach.edges("teachers/jon", direction="out")

You can manage edges via graph API wrappers also, but you must use document
IDs instead of keys where applicable.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        if await db.has_graph("school"):
            school = db.graph("school")
        else:
            school = await db.create_graph("school")

        if not await school.has_vertex_collection("lectures"):
            await school.create_vertex_collection("lectures")
        await school.insert_vertex("lectures", {"_key": "CSC101"})

        if not await school.has_vertex_collection("teachers"):
            await school.create_vertex_collection("teachers")
        await school.insert_vertex("teachers", {"_key": "jon"})

        # Create the edge collection "teach".
        if not await school.has_edge_definition("teach"):
            await school.create_edge_definition(
                edge_collection="teach",
                from_vertex_collections=["teachers"],
                to_vertex_collections=["lectures"]
            )

        # The "_id" field is required instead of "_key" field.
        await school.insert_edge(
            collection="teach",
            edge={
                "_id": "teach/jon-CSC101",
                "_from": "teachers/jon",
                "_to": "lectures/CSC101"
            }
        )
        await school.replace_edge({
            "_id": "teach/jon-CSC101",
            "_from": "teachers/jon",
            "_to": "lectures/CSC101",
            "online": False,
        })
        await school.update_edge({
            "_id": "teach/jon-CSC101",
            "online": True
        })
        await school.has_edge("teach/jon-CSC101")
        await school.edge("teach/jon-CSC101")
        await school.delete_edge("teach/jon-CSC101")
        await school.link("teach", "teachers/jon", "lectures/CSC101")
        await school.edges("teach", "teachers/jon", direction="out")

See :class:`arangoasync.graph.Graph` and :class:`arangoasync.graph.EdgeCollection` for API specification.

.. _graph-traversals:

Graph Traversals
================

**Graph traversals** are executed via AQL.
Each traversal can span across multiple vertex collections, and walk
over edges and vertices using various algorithms.

**Example:**

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(hosts="http://localhost:8529") as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        db = await client.db("test", auth=auth)

        # Get the API wrapper for graph "school".
        if await db.has_graph("school"):
            school = db.graph("school")
        else:
            school = await db.create_graph("school")

        # Create vertex collections "lectures" and "teachers" if they do not exist.
        if not await school.has_vertex_collection("lectures"):
            await school.create_vertex_collection("lectures")
        if not await school.has_vertex_collection("teachers"):
            await school.create_vertex_collection("teachers")

        # Create the edge collection "teach".
        if not await school.has_edge_definition("teach"):
            await school.create_edge_definition(
                edge_collection="teach",
                from_vertex_collections=["teachers"],
                to_vertex_collections=["lectures"]
            )

        # Get API wrappers for "from" and "to" vertex collections.
        teachers = school.vertex_collection("teachers")
        lectures = school.vertex_collection("lectures")

        # Get the API wrapper for the edge collection.
        teach = school.edge_collection("teach")

        # Insert vertices into the graph.
        await teachers.insert({"_key": "jon", "name": "Professor jon"})
        await lectures.insert({"_key": "CSC101", "name": "Introduction to CS"})
        await lectures.insert({"_key": "MAT223", "name": "Linear Algebra"})
        await lectures.insert({"_key": "STA201", "name": "Statistics"})

        # Insert edges into the graph.
        await teach.insert({"_from": "teachers/jon", "_to": "lectures/CSC101"})
        await teach.insert({"_from": "teachers/jon", "_to": "lectures/STA201"})
        await teach.insert({"_from": "teachers/jon", "_to": "lectures/MAT223"})

        # AQL to perform a graph traversal.
        # Traverse 1 to 3 hops from the vertex "teachers/jon",
        query = """
        FOR v, e, p IN 1..3 OUTBOUND 'teachers/jon' GRAPH 'school'
        OPTIONS { bfs: true, uniqueVertices: 'global' }
        RETURN {vertex: v, edge: e, path: p}
        """

        # Traverse the graph in outbound direction, breath-first.
        async with await db.aql.execute(query) as cursor:
            async for lecture in cursor:
                print(lecture)
