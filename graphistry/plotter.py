import requests
import os

from graphistry.util import arrow_util, dict_util, graph as graph_util

NODE_ID  = '__node_id__'
EDGE_ID  = '__edge_id__'
EDGE_SRC = '__edge_src__'
EDGE_DST = '__edge_dst__'

class AssignableSettings(object):

    _settings: dict

    def __init__(self, defaults, settings = {}):
        self._defaults = defaults
        self._settings = settings


    def get(self, key):
        return self._settings[key] if key in self._settings else self._defaults[key]


    def with_assignments(self, settings):
        return AssignableSettings(
            self._defaults,
            self._updates(settings)
        )


    def _updates(self, settings):
        return {
            k: settings[k] for k in set(self._defaults.keys()).intersection(set(settings.keys()))
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
        'node_id': '__node_id__',

        # edge : data
        'edge_id': '__edge_id__',
        'edge_src': '__edge_src__',
        'edge_dst': '__edge_dst__',

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
