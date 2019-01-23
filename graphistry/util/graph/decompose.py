import pyarrow
import os


def decompose(graph, bindings):
    for decompose in [_decompose_igraph, _decompose_networkx, _decompose_neo4j]:
        try:
            decomposition = decompose(graph, bindings)
            if decomposition is not None:
                return decomposition
        except ImportError:
            continue

    raise TypeError("Unsupported Graph: %s" % (type(graph)))


def _decompose_igraph(graph, bindings):
    from graphistry.ext.igraph import to_arrow
    return to_arrow(graph, bindings)


def _decompose_networkx(graph, bindings):
    from graphistry.ext.networkx import to_arrow
    return to_arrow(graph, bindings)

def _decompose_neo4j(graph, bindings):
    from graphistry.ext.neo4j import to_arrow
    return to_arrow(graph, bindings)
