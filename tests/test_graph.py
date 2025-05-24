import pytest

from arangoasync.exceptions import GraphCreateError, GraphDeleteError, GraphListError
from tests.helpers import generate_graph_name


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


async def test_graph_properties(db):
    # Create a graph
    name = generate_graph_name()
    graph = await db.create_graph(name)

    # Get the properties of the graph
    properties = await graph.properties()
    assert properties.name == name
