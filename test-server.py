#!/usr/bin/python

from wsgiref.simple_server import make_server
import math
import json
from copy import deepcopy
import re
from hashlib import sha1
from base64 import b32encode
import random
import optparse

from dmedia.ui import load_datafile
from dmedia.wsgi import *

LEAF_SIZE = 8 * 2**20  # 8 MiB



def b32_sha1(chunk):
    return b32encode(sha1(chunk).digest())


class App(BaseWSGI):
    def __init__(self, data, fail):
        super(App, self).__init__()
        self.data = data
        self.fail = fail
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

    def get_session(self, quick_id):
        if self.fail and random.randint(0, 3) == 0:
            self.sessions.pop(quick_id, None)
        try:
            return self.sessions[quick_id]
        except KeyError:
            raise Conflict()

    @http_method
    def GET(self, environ, start_response):
        path_info = environ['PATH_INFO']
        try:
            (body, mime) = self.data[path_info]
        except KeyError:
            raise NotFound()
        headers = [
            ('Content-Type', mime),
            ('Content-Length', str(len(body)).encode('utf-8'))
        ]
        start_response('200 OK', headers)
        return body

    @http_method
    def POST(self, environ, start_response):
        content_type = environ.get('CONTENT_TYPE', '')
        if not content_type.startswith('application/json'):
            raise UnsupportedMediaType()
        path_info = environ['PATH_INFO']
        obj = json.loads(read_input(environ))
        print obj
        if path_info == '/':
            d = self.init(obj)
            return self.json(d, environ, start_response)
        m = re.match('/([A-Z0-9]{32})$', path_info)
        if m:
            quick_id = m.group(1)
            session = self.get_session(quick_id)
            return self.json(session, environ, start_response)
        raise BadRequest()

    @http_method
    def PUT(self, environ, start_response):
        content_type = environ.get('CONTENT_TYPE', '')
        if not content_type.startswith('application/octet-stream'):
            raise UnsupportedMediaType()
        path_info = environ['PATH_INFO']
        m = re.match('/([A-Z0-9]{32})/(\d+)$', path_info)
        if not m:
            raise BadRequest()
        quick_id = m.group(1)
        session = self.get_session(quick_id)
        i = int(m.group(2))
        chash = environ.get('HTTP_X_DMEDIA_CHASH')
        leaf = read_input(environ)
        if self.fail and random.randint(0, 1) == 1:
            leaf += b'corruption'
        got = b32_sha1(leaf)
        d = {
            'quick_id': quick_id,
            'index': i,
            'received': got,
        }
        if got == chash:
            session['leaves'][i] = chash
            return self.json(d, environ, start_response)
        else:
            d['expected'] = chash
            return self.json(d, environ, start_response,
                '412 Precondition Failed'
            )

    def json(self, d, environ, start_response, status='201 Created'):
        body = json.dumps(d, sort_keys=True, indent=4)
        headers = [
            ('Content-Type', 'application/json'),
            ('Content-Length', str(len(body)).encode('utf-8'))
        ]
        start_response(status, headers)
        print body
        return body


index = """<!DOCTYPE html>
<html>
<head>
<title>HTML5 File upload test</title>
<script type="text/javascript" src="sha1.js"></script>
<script type="text/javascript" src="base32.js"></script>
<script type="text/javascript" src="uploader.js"></script>
<script type="text/javascript">

function log() {
    var parent = document.getElementById('log');
    if (! parent) {
        return;
    }
    var args = Array.prototype.slice.call(arguments);
    var msg = args.join(' ');
    var pre = document.createElement('pre');
    pre.textContent = msg;
    parent.appendChild(pre);
}

function on_request(request) {
    log('RESPONSE:', request.status, request.statusText);
    log(request.responseText);
}

function handle(files) {
    var u = new Uploader('/');
    u.onrequest = on_request;
    var file = files[0];
    u.upload(file);
}

</script>
</head>
<body>
<form>
<input type="file" id="file" onchange="handle(this.files)" />
</form>
<div id="log"></div>
</body>
</html>"""


data = {
    '/': (
        index,
        'text/html; charset=UTF-8'
    ),
    '/sha1.js': (
        load_datafile('sha1.js'),
        'application/javascript; charset=UTF-8'
    ),
    '/base32.js': (
        load_datafile('base32.js'),
        'application/javascript; charset=UTF-8'
    ),
    '/uploader.js': (
        load_datafile('uploader.js'),
        'application/javascript; charset=UTF-8'
    ),
}


parser = optparse.OptionParser()
parser.add_option('--fail',
    help='add random data corruption and server failures',
    action='store_true',
    default=False,
)

(options, args) = parser.parse_args()
app = App(data, options.fail)
httpd = make_server('', 8000, app)
httpd.serve_forever()
