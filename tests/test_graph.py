import pytest

from arangoasync.exceptions import GraphCreateError, GraphDeleteError, GraphListError
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


async def test_graph_properties(db, cluster, enterprise):
    # Create a graph
    name = generate_graph_name()
    is_smart = cluster and enterprise
    options = GraphOptions(number_of_shards=3)
    graph = await db.create_graph(name, is_smart=is_smart, options=options)

    # Create first vertex collection
    vcol_name = generate_col_name()
    vcol = await graph.create_vertex_collection(vcol_name)
    assert vcol.name == vcol_name

    # Get the properties of the graph
    properties = await graph.properties()
    assert properties.name == name
    assert properties.is_smart == is_smart
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
