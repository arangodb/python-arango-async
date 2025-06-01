import pytest

from arangoasync.exceptions import (
    DocumentDeleteError,
    EdgeCollectionListError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
    EdgeListError,
    GraphCreateError,
    GraphDeleteError,
    GraphListError,
    GraphPropertiesError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
)
from arangoasync.typings import GraphOptions
from tests.helpers import generate_col_name, generate_graph_name


@pytest.mark.asyncio
async def test_graph_basic(db, bad_db):
    graph1_name = generate_graph_name()
    # Test the graph representation
    graph = db.graph(graph1_name)
    assert graph.name == graph1_name
    assert graph1_name in repr(graph)

    # Cannot find any graph
    graph2_name = generate_graph_name()
    assert await db.graphs() == []
    assert await db.has_graph(graph2_name) is False
    with pytest.raises(GraphListError):
        await bad_db.has_graph(graph2_name)
    with pytest.raises(GraphListError):
        await bad_db.graphs()

    # Create a graph
    graph = await db.create_graph(graph1_name, wait_for_sync=True)
    assert graph.name == graph1_name
    with pytest.raises(GraphCreateError):
        await bad_db.create_graph(graph1_name)

    # Check if the graph exists
    assert await db.has_graph(graph1_name) is True
    graphs = await db.graphs()
    assert len(graphs) == 1
    assert graphs[0].name == graph1_name

    # Delete the graph
    await db.delete_graph(graph1_name)
    assert await db.has_graph(graph1_name) is False
    with pytest.raises(GraphDeleteError):
        await bad_db.delete_graph(graph1_name)


@pytest.mark.asyncio
async def test_graph_properties(db, bad_graph, cluster, enterprise):
    # Create a graph
    name = generate_graph_name()
    is_smart = cluster and enterprise
    options = GraphOptions(number_of_shards=3)
    graph = await db.create_graph(name, is_smart=is_smart, options=options)

    with pytest.raises(GraphPropertiesError):
        await bad_graph.properties()

    # Create first vertex collection
    vcol_name = generate_col_name()
    vcol = await graph.create_vertex_collection(vcol_name)
    assert vcol.name == vcol_name

    # Get the properties of the graph
    properties = await graph.properties()
    assert properties.name == name
    assert properties.is_smart == is_smart
    if cluster:
        assert properties.number_of_shards == options.number_of_shards
    assert properties.orphan_collections == [vcol_name]

    # Create second vertex collection
    vcol2_name = generate_col_name()
    vcol2 = await graph.create_vertex_collection(vcol2_name)
    assert vcol2.name == vcol2_name
    properties = await graph.properties()
    assert len(properties.orphan_collections) == 2

    # Create an edge definition
    edge_name = generate_col_name()
    edge_col = await graph.create_edge_definition(
        edge_name,
        from_vertex_collections=[vcol_name],
        to_vertex_collections=[vcol2_name],
    )
    assert edge_col.name == edge_name

    # There should be no more orphan collections
    properties = await graph.properties()
    assert len(properties.orphan_collections) == 0
    assert len(properties.edge_definitions) == 1
    assert properties.edge_definitions[0]["collection"] == edge_name
    assert len(properties.edge_definitions[0]["from"]) == 1
    assert properties.edge_definitions[0]["from"][0] == vcol_name
    assert len(properties.edge_definitions[0]["to"]) == 1
    assert properties.edge_definitions[0]["to"][0] == vcol2_name


@pytest.mark.asyncio
async def test_vertex_collections(db, docs, bad_graph):
    # Test errors
    with pytest.raises(VertexCollectionCreateError):
        await bad_graph.create_vertex_collection("bad_col")
    with pytest.raises(VertexCollectionListError):
        await bad_graph.vertex_collections()
    with pytest.raises(VertexCollectionListError):
        await bad_graph.has_vertex_collection("bad_col")
    with pytest.raises(VertexCollectionDeleteError):
        await bad_graph.delete_vertex_collection("bad_col")

    # Create graph
    graph = await db.create_graph(generate_graph_name())

    # Create vertex collections
    names = [generate_col_name() for _ in range(3)]
    cols = [await graph.create_vertex_collection(name) for name in names]

    # List vertex collection
    col_list = await graph.vertex_collections()
    assert len(col_list) == 3
    for c in cols:
        assert c.name in col_list
        assert await graph.has_vertex_collection(c.name)

    # Delete collections
    await graph.delete_vertex_collection(names[0])
    assert await graph.has_vertex_collection(names[0]) is False

    # Insert in both collections
    v1_meta = await graph.insert_vertex(names[1], docs[0])
    v2_meta = await graph.insert_vertex(names[2], docs[1], return_new=True)
    assert "new" in v2_meta
    v2_meta = v2_meta["vertex"]

    # Get the vertex
    v1 = await graph.vertex(v1_meta)
    assert v1 is not None
    assert v1["text"] == docs[0]["text"]
    v2 = await graph.vertex(v2_meta["_id"])
    assert v2 is not None
    v3 = await graph.vertex(f"{names[2]}/bad_id")
    assert v3 is None

    # Update one vertex
    v1["text"] = "updated_text"
    v1_meta = await graph.update_vertex(v1, return_new=True)
    assert "new" in v1_meta
    assert "vertex" in v1_meta
    v1 = await graph.vertex(v1_meta["vertex"])
    assert v1["text"] == "updated_text"

    # Replace the other vertex
    v1["text"] = "replaced_text"
    v1["additional"] = "data"
    v1.pop("loc")
    v1_meta = await graph.replace_vertex(v1, return_old=True, return_new=True)
    assert "old" in v1_meta
    assert "new" in v1_meta
    assert "vertex" in v1_meta
    v1 = await graph.vertex(v1_meta["vertex"])
    assert v1["text"] == "replaced_text"
    assert "additional" in v1
    assert "loc" not in v1

    # Delete a vertex
    v1 = await graph.delete_vertex(v1["_id"], return_old=True)
    assert "_id" in v1
    assert await graph.delete_vertex(v1["_id"], ignore_missing=True) is False
    with pytest.raises(DocumentDeleteError):
        assert await graph.delete_vertex(v1["_id"])

    # Check has method
    assert await graph.has_vertex(v1) is False
    assert await graph.has_vertex(v2["_id"]) is True


@pytest.mark.asyncio
async def test_edge_collections(db, bad_graph):
    # Test errors
    with pytest.raises(EdgeDefinitionListError):
        await bad_graph.edge_definitions()
    with pytest.raises(EdgeDefinitionListError):
        await bad_graph.has_edge_definition("bad_col")
    with pytest.raises(EdgeCollectionListError):
        await bad_graph.edge_collections()
    with pytest.raises(EdgeDefinitionReplaceError):
        await bad_graph.replace_edge_definition("foo", ["bar1"], ["bar2"])
    with pytest.raises(EdgeDefinitionDeleteError):
        await bad_graph.delete_edge_definition("foo")
    with pytest.raises(EdgeListError):
        await bad_graph.edges("col", "foo")

    # Create full graph
    name = generate_graph_name()
    graph = await db.create_graph(name)
    teachers_col_name = generate_col_name()
    await db.create_collection(teachers_col_name)
    await graph.create_vertex_collection(teachers_col_name)
    students_col_name = generate_col_name()
    await db.create_collection(students_col_name)
    await graph.create_vertex_collection(students_col_name)
    edge_col_name = generate_col_name()
    edge_col = await graph.create_edge_definition(
        edge_col_name,
        from_vertex_collections=[teachers_col_name],
        to_vertex_collections=[students_col_name],
    )
    assert edge_col.name == edge_col_name

    # List edge definitions
    edge_definitions = await graph.edge_definitions()
    assert len(edge_definitions) == 1
    assert "edge_collection" in edge_definitions[0]
    assert "from_vertex_collections" in edge_definitions[0]
    assert "to_vertex_collections" in edge_definitions[0]
    assert await graph.has_edge_definition(edge_col_name) is True
    assert await graph.has_edge_definition("bad_edge") is False

    edge_cols = await graph.edge_collections()
    assert len(edge_cols) == 1
    assert edge_col_name in edge_cols

    # Design the graph
    teachers = [
        {"_key": "101", "name": "Mr. Smith"},
        {"_key": "102", "name": "Ms. Johnson"},
        {"_key": "103", "name": "Dr. Brown"},
    ]
    students = [
        {"_key": "123", "name": "Alice"},
        {"_key": "456", "name": "Bob"},
        {"_key": "789", "name": "Charlie"},
    ]
    edges = [
        {
            "_from": f"{teachers_col_name}/101",
            "_to": f"{students_col_name}/123",
            "subject": "Math",
        },
        {
            "_from": f"{teachers_col_name}/102",
            "_to": f"{students_col_name}/456",
            "subject": "Science",
        },
        {
            "_from": f"{teachers_col_name}/103",
            "_to": f"{students_col_name}/789",
            "subject": "History",
        },
    ]

    # Create an edge
    edge_metas = []
    for idx in range(len(edges)):
        await graph.insert_vertex(teachers_col_name, teachers[idx])
        await graph.insert_vertex(students_col_name, students[idx])
        edge_meta = await graph.insert_edge(
            edge_col_name,
            edges[0],
            return_new=True,
        )
        assert "new" in edge_meta
        edge_metas.append(edge_meta)

    # Check for edge existence
    edge_meta = edge_metas[0]
    edge_id = edge_meta["new"]["_id"]
    assert await graph.has_edge(edge_id) is True
    assert await graph.has_edge(f"{edge_col_name}/bad_id") is False
    edge = await graph.edge(edge_id)
    assert edge is not None

    # Update an edge
    edge["subject"] = "Advanced Math"
    updated_edge_meta = await graph.update_edge(edge, return_new=True, return_old=True)
    assert "new" in updated_edge_meta
    assert "old" in updated_edge_meta
    assert "edge" in updated_edge_meta
    edge = await graph.edge(edge_id)
    assert edge["subject"] == "Advanced Math"

    # Replace an edge
    edge["subject"] = "Replaced Subject"
    edge["extra_info"] = "Some additional data"
    replaced_edge_meta = await graph.replace_edge(
        edge, return_old=True, return_new=True
    )
    assert "old" in replaced_edge_meta
    assert "new" in replaced_edge_meta
    assert "edge" in replaced_edge_meta
    edge = await graph.edge(edge_id)
    assert edge["subject"] == "Replaced Subject"

    # Delete the edge
    deleted_edge = await graph.delete_edge(edge_id, return_old=True)
    assert "_id" in deleted_edge
    assert await graph.has_edge(edge_id) is False

    # Replace the edge definition
    new_from_collections = [students_col_name]
    new_to_collections = [teachers_col_name]
    replaced_edge_col = await graph.replace_edge_definition(
        edge_col_name,
        from_vertex_collections=new_from_collections,
        to_vertex_collections=new_to_collections,
    )
    assert replaced_edge_col.name == edge_col_name

    # Verify the updated edge definition
    edge_definitions = await graph.edge_definitions()
    assert len(edge_definitions) == 1
    assert edge_definitions[0]["edge_collection"] == edge_col_name
    assert edge_definitions[0]["from_vertex_collections"] == new_from_collections
    assert edge_definitions[0]["to_vertex_collections"] == new_to_collections

    # Delete the edge definition
    await graph.delete_edge_definition(edge_col_name)
    assert await graph.has_edge_definition(edge_col_name) is False


@pytest.mark.asyncio
async def test_edge_links(db):
    # Create full graph
    name = generate_graph_name()
    graph = await db.create_graph(name)

    # Teachers collection
    teachers_col_name = generate_col_name()
    await db.create_collection(teachers_col_name)
    await graph.create_vertex_collection(teachers_col_name)

    # Students collection
    students_col_name = generate_col_name()
    await db.create_collection(students_col_name)
    await graph.create_vertex_collection(students_col_name)

    # Edges
    teachers_to_students = generate_col_name()
    await graph.create_edge_definition(
        teachers_to_students,
        from_vertex_collections=[teachers_col_name],
        to_vertex_collections=[students_col_name],
    )
    students_to_students = generate_col_name()
    await graph.create_edge_definition(
        students_to_students,
        from_vertex_collections=[teachers_col_name, students_col_name],
        to_vertex_collections=[students_col_name],
    )

    # Populate the graph
    teachers = [
        {"_key": "101", "name": "Mr. Smith"},
        {"_key": "102", "name": "Ms. Johnson"},
        {"_key": "103", "name": "Dr. Brown"},
    ]
    students = [
        {"_key": "123", "name": "Alice"},
        {"_key": "456", "name": "Bob"},
        {"_key": "789", "name": "Charlie"},
    ]

    docs = []
    t = await graph.insert_vertex(teachers_col_name, teachers[0])
    s = await graph.insert_vertex(students_col_name, students[0])
    await graph.link(teachers_to_students, t, s, {"subject": "Math"})
    docs.append(s)

    t = await graph.insert_vertex(teachers_col_name, teachers[1])
    s = await graph.insert_vertex(students_col_name, students[1])
    await graph.link(teachers_to_students, t["_id"], s["_id"], {"subject": "Science"})
    docs.append(s)

    t = await graph.insert_vertex(teachers_col_name, teachers[2])
    s = await graph.insert_vertex(students_col_name, students[2])
    await graph.link(teachers_to_students, t, s, {"subject": "History"})
    docs.append(s)

    await graph.link(students_to_students, docs[0], docs[1], {"friendship": "close"})
    await graph.link(students_to_students, docs[1], docs[0], {"friendship": "close"})

    edges = await graph.edges(students_to_students, docs[0])
    assert len(edges["edges"]) == 2
    assert "stats" in edges

    await graph.link(students_to_students, docs[2], docs[0], {"friendship": "close"})
    edges = await graph.edges(students_to_students, docs[0], direction="in")
    assert len(edges["edges"]) == 2

    edges = await graph.edges(students_to_students, docs[0], direction="out")
    assert len(edges["edges"]) == 1

    edges = await graph.edges(students_to_students, docs[0])
    assert len(edges["edges"]) == 3
