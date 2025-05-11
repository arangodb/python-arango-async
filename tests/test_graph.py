import pytest

from arangoasync.exceptions import GraphCreateError, GraphDeleteError, GraphListError


@pytest.mark.asyncio
async def test_graph_basic(db, bad_db):
    # Test the graph representation
    graph = db.graph("test_graph")
    assert graph.name == "test_graph"
    assert "test_graph" in repr(graph)

    # Cannot find any graph
    assert await db.graphs() == []
    assert await db.has_graph("fake_graph") is False
    with pytest.raises(GraphListError):
        await bad_db.has_graph("fake_graph")
    with pytest.raises(GraphListError):
        await bad_db.graphs()

    # Create a graph
    graph = await db.create_graph("test_graph", wait_for_sync=True)
    assert graph.name == "test_graph"
    with pytest.raises(GraphCreateError):
        await bad_db.create_graph("test_graph")

    # Check if the graph exists
    assert await db.has_graph("test_graph") is True
    graphs = await db.graphs()
    assert len(graphs) == 1
    assert graphs[0].name == "test_graph"

    # Delete the graph
    await db.delete_graph("test_graph")
    assert await db.has_graph("test_graph") is False
    with pytest.raises(GraphDeleteError):
        await bad_db.delete_graph("test_graph")
