from wsgiref.simple_server import make_server
import math
import json
from copy import deepcopy
import re
from hashlib import sha1
from base64 import b32encode

from dmedia.ui import load_datafile

LEAF_SIZE = 8 * 2**20  # 8 MiB

def read_input(environ):
    try:
        length = int(environ.get('CONTENT_LENGTH'))
    except ValueError:
        return ''
    return environ['wsgi.input'].read(length)


def b32_sha1(chunk):
    return b32encode(sha1(chunk).digest())


class App(object):
    def __init__(self, data):
        self.data = data
        self.sessions = {}

    def key(self, obj):
        return obj['quick_id']

    def init(self, obj):
        key = self.key(obj)
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
        return deepcopy(self.sessions[self.key(obj)])

    def __call__(self, environ, start_response):
        method = environ['REQUEST_METHOD']
        content_type = environ.get('CONTENT_TYPE')
        if method not in ('GET', 'POST', 'PUT'):
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

        elif method == 'POST' and content_type.startswith('application/json'):
            obj = json.loads(read_input(environ))
            print obj
            if path_info == '/':
                d = self.init(obj)
                return self.json_response(d, environ, start_response)

        elif method == 'PUT' and content_type == 'application/octet-stream':
            m = re.match('/([A-Z0-9]{32})/(\d+)$', path_info)
            if m:
                quick_id = m.group(1)
                i = int(m.group(2))
                obj = self.sessions[quick_id]
                chash = environ.get('HTTP_X_DMEDIA_CHASH')
                leaf = read_input(environ)
                if chash == b32_sha1(leaf):
                    obj['leaves'][i] = chash
                    d = {
                        'success': True,
                        'received': {
                            'index': i,
                            'chash': chash,
                            'size': len(leaf),
                        },
                        'leaves': obj['leaves'],
                        'quick_id': quick_id,
                    }
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
