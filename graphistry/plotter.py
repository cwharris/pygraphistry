import requests
import os
import warnings

from graphistry import constants
from graphistry.util import arrow_util, dict_util, graph as graph_util

class AssignableSettings(object):

    _settings: dict

    def __init__(self, defaults, settings = {}):
        self._defaults = defaults
        self._settings = settings


    def get(self, key):
        return self._settings[key] if key in self._settings else self._defaults[key]

    def set_unsafely(self, key, value):
        self._settings[key] = value

    def with_assignments(self, settings):
        return AssignableSettings(
            self._defaults,
            self._updates(settings)
        )


    def _updates(self, settings):
        return {
            key: settings[key] for key in set(self._defaults.keys()).intersection(set(settings.keys()))
        }


class Plotter(object):

    @staticmethod
    def update_default_settings(settings):
        Plotter.__default_settings.update(
            (key, value) for key, value in settings.items() if key in Plotter.__default_settings
        )


    __default_settings = {
        'protocol': 'https',
        'server': 'labs.graphistry.com',
        'key': None,
        'certificate_validation': None,
        'bolt': None,
        'height': 600
    }


    __default_bindings = {
        # node : data
        'node_id': constants.DEFAULT_NODE_ID,

        # edge : data
        'edge_id': constants.DEFAULT_EDGE_ID,
        'edge_src': constants.DEFAULT_EDGE_SRC,
        'edge_dst': constants.DEFAULT_EDGE_DST,

        # node : visualization
        'node_title': None,
        'node_label': None,
        'node_color': None,
        'node_size': None,

        # edge : visualization
        'edge_title': None,
        'edge_label': None,
        'edge_color': None,
        'edge_weight': None,
    }


    _data = {
        'nodes': None,
        'edges': None
    }


    _bindings_map = {
        'node_id':     'nodeId',
        'edge_id':     'edgeId',
        'edge_src':    'source',
        'edge_dst':    'destination',
        'edge_color':  'edgeColor',
        'edge_label':  'edgeLabel',
        'edge_title':  'edgeTitle',
        'edge_weight': 'edgeWeight',
        'node_color':  'pointColor',
        'node_label':  'pointLabel',
        'node_title':  'pointTitle',
        'node_size':   'pointSize'
    }


    _bindings = AssignableSettings(__default_bindings)
    _settings = AssignableSettings(__default_settings)


    def __init__(
        self,
        base=None,
        data=None,
        bindings=None,
        settings=None
    ):
        if base is None:
            base = self

        self._data     = data     or base._data
        self._bindings = bindings or base._bindings
        self._settings = settings or base._settings


    def data(self,  **data):
        if 'graph' in data:
            (edges, nodes) = graph_util.decompose(data['graph'])
            return self.data(
                edges=edges,
                nodes=nodes
            )

        if 'edges' in data:
            data['edges'] = arrow_util.to_arrow(data['edges'])

        if 'nodes' in data:
            data['nodes'] = arrow_util.to_arrow(data['nodes'])

        return Plotter(
            self,
            data=dict_util.assign(self._data, data)
        )


    def bind(self,  **bindings):
        return Plotter(
            self,
            bindings=self._bindings.with_assignments(bindings)
        )


    def settings(self, **settings):
        return Plotter(
            self,
            settings=self._settings.with_assignments(settings)
        )


    def nodes(self, nodes):
        return self.data(nodes=nodes)


    def edges(self, edges):
        return self.data(edges=edges)


    def graph(self, graph):
        return self.data(graph=graph)


    def plot(self):
        # TODO(cwharris): verify required bindings

        (edges, nodes) = graph_util.rectify(
            edges=self._data['edges'],
            nodes=self._data['nodes'],
            edge=self._bindings.get('edge_id'),
            node=self._bindings.get('node_id'),
            edge_src=self._bindings.get('edge_src'),
            edge_dst=self._bindings.get('edge_dst'),
            safe=True
        )

        nodeBuffer = arrow_util.table_to_buffer(nodes)
        edgeBuffer = arrow_util.table_to_buffer(edges)

        import pyarrow as arrow

        a = arrow.open_stream(nodeBuffer)
        b = arrow.open_stream(edgeBuffer)

        files = {
            'nodes': ('nodes', nodeBuffer, 'application/octet-stream'),
            'edges': ('edges', edgeBuffer, 'application/octet-stream')
        }

        data = {
            self._bindings_map[key]: self._bindings.get(key)
            for key in Plotter.__default_bindings.keys()
            if self._bindings.get(key) is not None
        }

        graphistry_uri = f"{self._settings.get('protocol')}://{self._settings.get('server')}"
        
        response = requests.post(
            f'{graphistry_uri}/datasets' ,
            files=files,
            data=data,
            timeout=(10, None) # TODO(cwharris): make 'connection' timeout configurable... maybe the 'read' timeout, too.
        )

        # TODO(cwharris): Try to present a friendly error message.

        response.raise_for_status()

        # TODO(cwharris): Transform in to appropriate return value (HTML, etc).

        jres = response.json()

        from IPython.core.display import HTML

        return HTML(
            _make_iframe(f"{graphistry_uri}/graph/graph.html?dataset={jres['revisionId']}", self._settings.get('height'))
        )

    def cypher(self, query, params={}):
        import neo4j
        driver = neo4j.GraphDatabase.driver(**self._settings.get('bolt'))
        with driver.session() as session:
            bolt_statement = session.run(query, **params)
            graph = bolt_statement.graph()
            return self.data(graph=graph)


    # mutative utility functions which should really be located outside of this class elsewhere

    def pandas2igraph(self, edges, directed=True):
        """Convert a pandas edge dataframe to an IGraph graph.
        Uses current bindings. Defaults to treating edges as directed.
        **Example**
            ::
                import graphistry
                g = graphistry.bind()
                es = pandas.DataFrame({'src': [0,1,2], 'dst': [1,2,0]})
                g = g.bind(source='src', destination='dst')
                ig = g.pandas2igraph(es)
                ig.vs['community'] = ig.community_infomap().membership
                g.bind(point_color='community').plot(ig)
        """

        import igraph
        
        self._check_mandatory_bindings(False)
        self._check_bound_attribs(edges, ['source', 'destination'], 'Edge')
        
        eattribs = edges.columns.values.tolist()
        eattribs.remove(self._bindings.get('edge_src'))
        eattribs.remove(self._bindings.get('edge_dst'))
        cols = [self._bindings.get('edge_src'), self._bindings.get('edge_dst')] + eattribs
        etuples = [tuple(x) for x in edges[cols].values]
        return igraph.Graph.TupleList(etuples, directed=directed, edge_attrs=eattribs,
                                      vertex_name_attr=self._bindings.get('node_id'))

    def igraph2pandas(self, ig):
        """Under current bindings, transform an IGraph into a pandas edges dataframe and a nodes dataframe.
        **Example**
            ::
                import graphistry
                g = graphistry.bind()
                es = pandas.DataFrame({'src': [0,1,2], 'dst': [1,2,0]})
                g = g.bind(source='src', destination='dst').edges(es)
                ig = g.pandas2igraph(es)
                ig.vs['community'] = ig.community_infomap().membership
                (es2, vs2) = g.igraph2pandas(ig)
                g.nodes(vs2).bind(point_color='community').plot()
        """

        _warn_plotter_mutation()

        def get_edgelist(ig):
            idmap = dict(enumerate(ig.vs[self._bindings.get('node_id')]))
            for e in ig.es:
                t = e.tuple
                yield dict(
                    {
                        self._bindings.get('edge_src'): idmap[t[0]],
                        self._bindings.get('edge_dst'): idmap[t[1]]
                    },
                    **e.attributes()
                )

        self._check_mandatory_bindings(False)

        if self._bindings.get('node_id') is None:
            self._bindings.set_unsafely('node_id', constants.DEFAULT_NODE_ID)
            ig.vs[constants.DEFAULT_NODE_ID] = [v.index for v in ig.vs]

        if self._bindings.get('node_id') not in ig.vs.attributes():
            util.error('Vertex attribute "%s" bound to "node" does not exist.' % self._bindings.get('node_id'))

        edata = get_edgelist(ig)
        ndata = [v.attributes() for v in ig.vs]
        nodes = pandas.DataFrame(ndata, columns=ig.vs.attributes())

        cols = [self._bindings.get('edge_src'), self._bindings.get('edge_dst')] + ig.es.attributes()

        edges = pandas.DataFrame(edata, columns=cols)

        return (edges, nodes)


    def networkx_checkoverlap(self, g):
        _warn_plotter_mutation()

        import networkx as nx
        [x, y] = [int(x) for x in nx.__version__.split('.')]

        vattribs = None
        if x == 1:
            vattribs = g.nodes(data=True)[0][1] if g.number_of_nodes() > 0 else []
        else:
            vattribs = g.nodes(data=True) if g.number_of_nodes() > 0 else []
        if not (self._bindings.get('node_id') is None) and self._bindings.get('node_id') in vattribs:
            util.error('Vertex attribute "%s" already exists.' % self._bindings.get('node_id'))

    def networkx2pandas(self, g):
        _warn_plotter_mutation()

        def get_nodelist(g):
            for n in g.nodes(data=True):
                yield dict(
                    {
                        self._bindings.get('node_id'): n[0]
                    },
                    **n[1]
                )
        def get_edgelist(g):
            for e in g.edges(data=True):
                yield dict(
                    {
                        self._bindings.get('edge_src'): e[0],
                        self._bindings.get('edge_dst'): e[1]
                    },
                    **e[2]
                )

        self._check_mandatory_bindings(False)
        self.networkx_checkoverlap(g)
        
        if self._bindings.get('node_id') is None:
            self._bindings.set_unsafely('node_id', constants.DEFAULT_NODE_ID)

        nodes = pandas.DataFrame(get_nodelist(g))
        edges = pandas.DataFrame(get_edgelist(g))

        return (edges, nodes)


def _make_iframe(raw_url, height):
    import uuid
    id = uuid.uuid4()

    scrollbug_workaround='''
            <script>
                $("#%s").bind('mousewheel', function(e) {
                e.preventDefault();
                });
            </script>
        ''' % id

    iframe = '''
            <iframe id="%s" src="%s"
                    allowfullscreen="true" webkitallowfullscreen="true" mozallowfullscreen="true"
                    oallowfullscreen="true" msallowfullscreen="true"
                    style="width:100%%; height:%dpx; border: 1px solid #DDD">
            </iframe>
        ''' % (id, raw_url, height)

    return iframe + scrollbug_workaround


def _warn_plotter_mutation():
    warnings.warn("This method may mutate the plotter instance.", RuntimeWarning)
