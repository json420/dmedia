from wsgiref.simple_server import make_server
import math
import json
from copy import deepcopy

from dmedia.ui import load_datafile

LEAF_SIZE = 8 * 2**20  # 8 MiB

def read_input(environ):
    try:
        length = int(environ.get('CONTENT_LENGTH'))
    except ValueError:
        return ''
    return environ['wsgi.input'].read(length)


class App(object):
    def __init__(self, data):
        self.data = data
        self.sessions = {}

    def init(self, obj):
        key = (obj['quick_id'], obj['bytes'])
        try:
            return deepcopy(self.sessions[key])
        except KeyError:
            pass
        d = dict(
            (k, obj[k]) for k in ['bytes', 'quick_id']
        )
        self.sessions[key] = d
        size = d['bytes']
        d['leaves'] = [
            None for i in xrange(int(math.ceil(size / float(LEAF_SIZE))))
        ]
        d['leaf_size'] = LEAF_SIZE
        return d

    def get(self, obj):
        key = (obj['quick_id'], obj['bytes'])
        return deepcopy(self.sessions[key])

    def __call__(self, environ, start_response):
        method = environ['REQUEST_METHOD']
        content_type = environ.get('CONTENT_TYPE')
        if method not in ('GET', 'POST'):
            start_response('405 Method Not Allowed', [])
            return ''
        path_info = environ['PATH_INFO']

        if method == 'GET':
            try:
                (body, mime) = self.data[path_info]
            except KeyError:
                start_response('404 Not Found', [])
                return ''
            headers = [
                ('Content-Type', mime),
                ('Content-Length', str(len(body)).encode('utf-8'))
            ]
            start_response('200 OK', headers)
            return body

        if method == 'POST' and content_type.startswith('application/json'):
            obj = json.loads(read_input(environ))
            print obj
            if path_info == '/':
                d = self.init(obj)
                return self.json_response(d, environ, start_response)

        start_response('400 Bad Request', [])
        return ''

    def json_response(self, d, environ, start_response, status='201 Created'):
        body = json.dumps(d, sort_keys=True, indent=4)
        headers = [
            ('Content-Type', 'application/json'),
            ('Content-Length', str(len(body)).encode('utf-8'))
        ]
        start_response(status, headers)
        print body
        return body


scripts = dict(
    (n, load_datafile(n)) for n in ['mootools-core.js', 'uploader.js']
)

data = {
    '/': (
        load_datafile('uploader.html'),
        'text/html; charset=UTF-8'
    ),
    '/mootools-core.js': (
        load_datafile('mootools-core.js'),
        'application/javascript; charset=UTF-8'
    ),
    '/uploader.js': (
        load_datafile('uploader.js'),
        'application/javascript; charset=UTF-8'
    ),
}



app = App(data)
httpd = make_server('', 8000, app)
httpd.serve_forever()
