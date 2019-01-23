import igraph
import pyarrow as arrow
import itertools

def to_arrow(
    graph,
    bindings
):
    if not isinstance(graph, igraph.Graph):
        return None

    nodes: arrow.Table = arrow.Table.from_arrays(
        [column for column in itertools.chain(
            _id_columns(graph.vs, bindings),
            _attribute_columns(graph.vs)
        )]
    )

    edges = arrow.Table.from_arrays(
        [column for column in itertools.chain(
            _id_columns(graph.es, bindings),
            _src_dst_columns(graph.es, bindings),
            _attribute_columns(graph.es)
        )]
    )

    return (edges, nodes)


def _attribute_columns(sequence):
    for attribute_name in sequence.attributes():
        yield arrow.column(attribute_name, [
            [item[attribute_name] for item in sequence]
        ])


def _id_columns(sequence, bindings):
    yield arrow.column(bindings.get('node_id'), [
        [id for id, _ in enumerate(sequence)]
    ])


def _src_dst_columns(edgeSequence, bindings):
    yield arrow.column(bindings.get('edge_src'), [
        [edge.tuple[0] for edge in edgeSequence]
    ])

    yield arrow.column(bindings.get('edge_dst'), [
        [edge.tuple[1] for edge in edgeSequence]
    ])
