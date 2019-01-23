import pytest
import igraph

from graphistry.ext.igraph import to_arrow
from graphistry.plotter import AssignableSettings, Plotter, _plotter_default_bindings

def test_to_arrow():
    bindings = AssignableSettings(_plotter_default_bindings)
    graph = igraph.Graph.Tree(2, 10)
    (edges, nodes) = to_arrow(graph, bindings)
    assert len(graph.vs) == len(nodes)
    assert len(graph.es) == len(edges)
    pass

if __name__ == '__main__':
    pytest.main()
