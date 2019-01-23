import pytest
import graphistry
import networkx
import pyarrow

from graphistry import constants

graphistry.register(
    protocol='http',
    server='nginx'
)

def test_plot():
    graph = networkx.random_lobster(100, 0.9, 0.9)
    uri = graphistry \
        .data(graph=graph) \
        .plot()
    print(uri)
