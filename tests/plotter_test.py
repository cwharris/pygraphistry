import pytest
import igraph

from graphistry import Plotter


def test_plot():
    pass

def test_igraph2pandas():
    sourceGraph = igraph.Graph.Tree(2, 10)
    plotter = Plotter().data(graph=sourceGraph)
    pandasGraph = plotter.igraph2pandas(sourceGraph)
    originGraph = plotter.pandas2igraph(pandasGraph)
    pass