import pytest
import networkx

from graphistry.ext.networkx import to_arrow
from graphistry.plotter import AssignableSettings, Plotter, _plotter_default_bindings

def test_to_arrow():
    bindings = AssignableSettings(_plotter_default_bindings)
    graph = networkx.random_lobster(100, 0.9, 0.9)
    (edges, nodes) = to_arrow(graph, bindings)
    assert len(graph.nodes()) == len(nodes)
    assert len(graph.edges()) == len(edges)
    pass

if __name__ == '__main__':
    pytest.main()
