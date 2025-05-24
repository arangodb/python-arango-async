import pytest

from arangoasync.exceptions import (
    EdgeCollectionListError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
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


async def test_vertex_collections(db, bad_graph):
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

    # Create full graph
    name = generate_graph_name()
    graph = await db.create_graph(name)
    vcol_name = generate_col_name()
    await graph.create_vertex_collection(vcol_name)
    vcol2_name = generate_col_name()
    await graph.create_vertex_collection(vcol2_name)
    edge_name = generate_col_name()
    edge_col = await graph.create_edge_definition(
        edge_name,
        from_vertex_collections=[vcol_name],
        to_vertex_collections=[vcol2_name],
    )
    assert edge_col.name == edge_name

    # List edge definitions
    edge_definitions = await graph.edge_definitions()
    assert len(edge_definitions) == 1
    assert "edge_collection" in edge_definitions[0]
    assert "from_vertex_collections" in edge_definitions[0]
    assert "to_vertex_collections" in edge_definitions[0]
    assert await graph.has_edge_definition(edge_name) is True
    assert await graph.has_edge_definition("bad_edge") is False

    edge_cols = await graph.edge_collections()
    assert len(edge_cols) == 1
    assert edge_name in edge_cols

    # Replace the edge definition
    new_from_collections = [vcol2_name]
    new_to_collections = [vcol_name]
    replaced_edge_col = await graph.replace_edge_definition(
        edge_name,
        from_vertex_collections=new_from_collections,
        to_vertex_collections=new_to_collections,
    )
    assert replaced_edge_col.name == edge_name

    # Verify the updated edge definition
    edge_definitions = await graph.edge_definitions()
    assert len(edge_definitions) == 1
    assert edge_definitions[0]["edge_collection"] == edge_name
    assert edge_definitions[0]["from_vertex_collections"] == new_from_collections
    assert edge_definitions[0]["to_vertex_collections"] == new_to_collections

    # Delete the edge definition
    await graph.delete_edge_definition(edge_name)
    assert await graph.has_edge_definition(edge_name) is False
