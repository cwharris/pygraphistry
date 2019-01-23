import neo4j
import pyarrow as arrow
import itertools

from graphistry import constants


def to_arrow( # TODO(cwharris): move these consts out of here
    graph,
    bindings,
    neo4j_type_column_name=constants.DEFAULT_NEO4J_TYPE_BINDING,
    neo4j_label_column_name=constants.DEFAULT_NEO4J_LABEL_BINDING
):
    edge_table = _edge_table(
        graph.relationships,
        bindings,
        neo4j_type_column_name
    )

    node_table = _node_table(
        graph.nodes,
        bindings,
        neo4j_label_column_name
    )

    return (edge_table, node_table)


def _edge_table(
    relationships,
    bindings,
    neo4j_type_column_name
):
    attribute_names = _attributes_for_entities(relationships)
    return arrow.Table.from_arrays(
        [column for column in itertools.chain(
            _intrinsic_edge_columns(
                relationships=relationships,
                bindings=bindings,
                neo4j_type_column_name=neo4j_type_column_name
            ),
            _columns_for_entity(
                entities=relationships,
                entity_attributes=attribute_names
            )
        )]
    )


def _node_table(
    nodes,
    bindings,
    neo4j_label_column_name
):
    attribute_names = _attributes_for_entities(nodes)
    return arrow.Table.from_arrays(
        [column for column in itertools.chain(
            _intrinsic_node_columns(
                nodes=nodes,
                bindings=bindings,
                neo4j_label_column_name=neo4j_label_column_name
            ),
            _columns_for_entity(
                entities=nodes,
                entity_attributes=attribute_names
            )
        )]
    )


def _attributes_for_entities(entities):
    return set(
        key for entity in entities for key in entity.keys()
    )


def _columns_for_entity(
    entities,
    entity_attributes
):
    for attribute in entity_attributes:
        yield arrow.column(attribute, [
            [entity[attribute] if attribute in entity else None for entity in entities]
        ])


def _intrinsic_edge_columns(
    relationships,
    bindings,
    neo4j_type_column_name
):
    # TODO(cwharris): remove the string conversion once server can haandle non-ascending integers.
    # currently, ids will be remapped as part of pre-plot rectification.
    yield arrow.column(bindings.get('edge_id'), [
        [str(relationship.id) for relationship in relationships]
    ])

    yield arrow.column(bindings.get('edge_src'), [
        [str(relationship.start_node.id) for relationship in relationships]
    ])

    yield arrow.column(bindings.get('edge_dst'), [
        [str(relationship.end_node.id) for relationship in relationships]
    ])

    yield arrow.column(neo4j_type_column_name, [
        [relationship.type for relationship in relationships]
    ])


def _intrinsic_node_columns(
    nodes,
    bindings,
    neo4j_label_column_name
):
    # TODO(cwharris): remove the string conversion once server can haandle non-ascending integers.
    # currently, ids will be remapped as part of pre-plot rectification.
    yield arrow.column(bindings.get('node_id'), [
        [str(node.id) for node in nodes]
    ])

    yield arrow.column(neo4j_label_column_name, [
        [list(node.labels) for node in nodes]
    ])
