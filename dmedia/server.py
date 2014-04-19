# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Dmedia WSGI applications.
"""

import json
import os
import socket
from base64 import b64encode, b64decode
import re
from wsgiref.util import shift_path_info
import logging

from filestore import DIGEST_B32LEN
from microfiber import dumps, basic_auth_header, CouchBase
from dbase32 import isdb32

import dmedia
from dmedia.httpd import WSGIError, make_server 
from dmedia import local, identity


USER = os.environ.get('USER')
HOST = socket.gethostname()
log = logging.getLogger()


def iter_headers(environ):
    for (key, value) in environ.items():
        if key in ('CONTENT_LENGTH', 'CONTENT_TYPE'):
            yield (key.replace('_', '-').lower(), value)
        elif key.startswith('HTTP_'):
            yield (key[5:].replace('_', '-').lower(), value)


def request_args(environ):
    headers = dict(iter_headers(environ))
    if environ['wsgi.input']._avail:
        body = environ['wsgi.input'].read()
    else:
        body = b''
    path = environ['PATH_INFO']
    if path == '':
        path = '/'
    query = environ['QUERY_STRING']
    if query:
        path = '?'.join([path, query])
    return (environ['REQUEST_METHOD'], path, body, headers)


def get_slice(environ):
    parts = environ['PATH_INFO'].lstrip('/').split('/')
    if len(parts) > 3:
        raise BadRequest('too many slashes in request path')
    _id = parts[0]
    if not (len(_id) == DIGEST_B32LEN and isdb32(_id)):
        raise BadRequest('badly formed dmedia ID')
    try:
        start = (int(parts[1]) if len(parts) > 1 else 0)
    except ValueError:
        raise BadRequest('start is not a valid integer')
    try:
        stop = (int(parts[2]) if len(parts) > 2 else None)
    except ValueError:
        raise BadRequest('stop is not a valid integer')
    if start < 0:
        raise BadRequest('start cannot be less than zero')
    if not (stop is None or start < stop):
        raise BadRequest('start must be less than stop')
    return (_id, start, stop)


RE_RANGE = re.compile('^bytes=(\d+)-(\d+)$')

def range_to_slice(value, file_size):
    """
    No bullshit HTTP Range parser from the wrong side of the tracks.

    Converts a byte-wise HTTP Range into a sane Python-esque byte-wise slice,
    and then checks that the following condition is met::

        0 <= start < stop <= file_size

    If not, a `WSGIError` is raised, aborting the request handling.  This is a
    strict parser only designed to handle the boring, predictable Range requests
    that the Dmedia HTTP client will make.  The Range request must have this
    form::

        bytes=START-END

    Where `START` and `END` are integers.  Some exciting variations that this
    parser does not support::

        bytes=-START
        bytes=START-

    For example, a request for the first 500 bytes in a 1000 byte file:

    >>> range_to_slice('bytes=0-499', 1000)
    (0, 500)

    Or a request for the final 500 bytes in the same:

    >>> range_to_slice('bytes=500-999', 1000)
    (500, 1000)

    But if you slip up and start thinking like a coder or someone who knows
    math, this tough kid has your back:

    >>> range_to_slice('bytes=500-1000', 1000)
    Traceback (most recent call last):
      ...
    dmedia.httpd.WSGIError: 416 Requested Range Not Satisfiable

    For details on the HTTP Range header, see:

        http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    assert isinstance(file_size, int)
    assert file_size > 0
    match = RE_RANGE.match(value)
    if match is None:
        raise WSGIError('400 Bad Range Request')
    start = int(match.group(1))
    end = int(match.group(2))
    stop = end + 1
    if not (0 <= start < stop <= file_size):
        raise WSGIError('416 Requested Range Not Satisfiable')
    return (start, stop)


def slice_to_content_range(start, stop, file_size):
    """
    Convert Python slice to HTTP Content-Range.

    For example, a slice containing the first 500 bytes of a 1234 byte file:

    >>> slice_to_content_range(0, 500, 1234)
    ('Content-Range', 'bytes 0-499/1234')

    Or the 2nd 500 bytes:

    >>> slice_to_content_range(500, 1000, 1234)
    ('Content-Range', 'bytes 500-999/1234')

    """
    assert 0 <= start < stop <= file_size
    end = stop - 1
    return ('Content-Range', 'bytes {}-{}/{}'.format(start, end, file_size))


MiB = 1024 * 1024

class FileSlice:
    __slots__ = ('fp', 'start', 'stop', 'content_length')

    def __init__(self, fp, start, stop):
        assert isinstance(start, int)
        assert isinstance(stop, int)
        assert 0 <= start < stop
        self.fp = fp
        self.start = start
        self.stop = stop
        self.content_length = stop - start

    def __iter__(self):
        self.fp.seek(self.start)
        os.posix_fadvise(
            self.fp.fileno(),
            self.start,
            self.content_length,
            os.POSIX_FADV_SEQUENTIAL
        )
        remaining = self.content_length
        while remaining:
            read = min(remaining, MiB)
            remaining -= read
            data = self.fp.read1(read)
            assert len(data) == read
            yield data
        self.fp.close()


class RootApp:
    """
    Main Dmedia WSGI app.
    """

    def __init__(self, env):
        self.user_id = env['user_id']
        obj = {
            'user_id': env['user_id'],
            'machine_id': env['machine_id'],
            'version': dmedia.__version__,
            'user': USER,
            'host': HOST,
        }
        self.info = dumps(obj).encode('utf-8')
        self.info_length = str(len(self.info))
        self.proxy = ProxyApp(env)
        self.files = FilesApp(env)
        self.map = {
            '': self.get_info,
            'couch': self.proxy,
            'files': self.files,
        }

    def __call__(self, environ, start_response):
        if environ.get('SSL_CLIENT_VERIFY') != 'SUCCESS':
            raise WSGIError('403 Forbidden SSL')
        if environ.get('SSL_CLIENT_I_DN_CN') != self.user_id:
            raise WSGIError('403 Forbidden Issuer')
        key = shift_path_info(environ)
        if key in self.map:
            return self.map[key](environ, start_response)
        raise WSGIError('410 Gone')

    def get_info(self, environ, start_response):
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        start_response('200 OK',
            [
                ('Content-Length', self.info_length),
                ('Content-Type', 'application/json'),
            ]
        )
        return [self.info]


class ProxyApp:
    def __init__(self, env):
        self.client = CouchBase(env)
        self.target_host = self.client.ctx.t.netloc
        self.basic_auth = basic_auth_header(env['basic'])

    def __call__(self, environ, start_response):
        (method, path, body, headers) = request_args(environ)
        db = shift_path_info(environ)
        if db and db.startswith('_'):
            raise WSGIError('403 Forbidden')
        headers['host'] = self.target_host
        headers['authorization'] = self.basic_auth

        lines = [('>' * 80), '{} {}'.format(method, path)]
        for key in sorted(headers):
            lines.append('{}: {}'.format(key, headers[key]))
        lines.append('')

        if method in {'PUT', 'POST'}:
            if 'content-length' in headers:
                headers['content-length'] = len(body)
        else:
            headers.pop('content-length', None)
            body = None

        response = self.client.raw_request(method, path, body, headers)
        status = '{} {}'.format(response.status, response.reason)
        lines.append(status)
        for key in sorted(response.headers):
            lines.append('{}: {}'.format(key, response.headers[key]))
        lines.append('<' * 80)
        print('\n'.join(lines))
        
        start_response(status, list(response.headers.items()))
        if response.body is None:
            return []
        return [response.body.read()]


class FilesApp:
    def __init__(self, env):
        self.local = local.LocalSlave(env)

    def __call__(self, environ, start_response):
        method = environ['REQUEST_METHOD']
        if method not in ('GET', 'HEAD'):
            raise WSGIError('405 Method Not Allowed')
        _id = shift_path_info(environ)
        if not isdb32(_id):
            raise WSGIError('400 Bad File ID')
        if len(_id) != DIGEST_B32LEN:
            raise WSGIError('400 Bad File ID Length')
        if environ['PATH_INFO'] != '':
            raise WSGIError('410 Gone')
        if environ['QUERY_STRING']:
            raise WSGIError('400 No Query For You')
        if method == 'HEAD' and 'HTTP_RANGE' in environ:
            raise WSGIError('400 Cannot Range with HEAD')
        try:
            doc = self.local.get_doc(_id)
            st = self.local.stat2(doc)
            fp = open(st.name, 'rb')
        except local.FileNotLocal:
            log.info('Not Found: %s', _id)
            raise WSGIError('404 Not Found')
        except Exception:
            log.exception('%r', environ)
            raise WSGIError('404 Not Found')

        if method == 'HEAD':
            start_response('200 OK', [('Content-Length', st.size)])
            return []

        if 'HTTP_RANGE' in environ:
            (start, stop) = range_to_slice(environ['HTTP_RANGE'], st.size)
            status = '206 Partial Content'
        else:
            start = 0
            stop = st.size
            status = '200 OK'

        log.info('Sending bytes %s[%d:%d] to %s:%s from %r', _id, start, stop,
            environ['REMOTE_ADDR'], environ['REMOTE_PORT'], st.name
        )
        file_slice = FileSlice(fp, start, stop)
        headers = [('Content-Length', file_slice.content_length)]
        if status == '206 Partial Content':
            headers.append(
                slice_to_content_range(start, stop, st.size)
            )
        start_response(status, headers)
        return file_slice


class InfoApp:
    """
    WSGI app initially used by the client-end of the peering process.
    """

    def __init__(self, _id):
        self.id = _id
        obj = {
            'id': _id,
            'version': dmedia.__version__,
            'user': USER,
            'host': HOST,
        }
        self.info = dumps(obj).encode('utf-8')
        self.info_length = str(len(self.info))

    def __call__(self, environ, start_response):
        if environ['wsgi.multithread'] is not False:
            raise WSGIError('500 Internal Server Error')
        if environ['PATH_INFO'] != '/':
            raise WSGIError('410 Gone')
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        start_response('200 OK',
            [
                ('Content-Length', self.info_length),
                ('Content-Type', 'application/json'),
            ]
        )
        return [self.info]


class ClientApp:
    """
    WSGI app used by the client-end of the peering process.
    """

    allowed_states = (
        'ready',
        'gave_challenge',
        'in_response',
        'wrong_response',
        'response_ok',
    )

    forwarded_states = (
        'wrong_response',
        'response_ok',
    )

    def __init__(self, cr, queue):
        self.cr = cr
        self.queue = queue
        self.__state = None
        self.map = {
            '/challenge': self.get_challenge,
            '/response': self.post_response,
        }

    def get_state(self):
        return self.__state

    def set_state(self, state):
        if state not in self.__class__.allowed_states:
            self.__state = None
            log.error('invalid state: %r', state)
            raise Exception('invalid state: {!r}'.format(state))
        self.__state = state
        if state in self.__class__.forwarded_states:
            self.queue.put(state)

    state = property(get_state, set_state)

    def __call__(self, environ, start_response):
        if environ['wsgi.multithread'] is not False:
            raise WSGIError('500 Internal Server Error')
        if environ.get('SSL_CLIENT_VERIFY') != 'SUCCESS':
            raise WSGIError('403 Forbidden SSL')
        if environ.get('SSL_CLIENT_S_DN_CN') != self.cr.peer_id:
            raise WSGIError('403 Forbidden Subject')
        if environ.get('SSL_CLIENT_I_DN_CN') != self.cr.peer_id:
            raise WSGIError('403 Forbidden Issuer')

        path_info = environ['PATH_INFO']
        if path_info not in self.map:
            raise WSGIError('410 Gone')
        log.info('%s %s',
            environ.get('REQUEST_METHOD'),
            environ.get('PATH_INFO')
        )
        try:
            obj = self.map[path_info](environ)            
            data = dumps(obj).encode('utf-8')
            start_response('200 OK',
                [
                    ('Content-Length', str(len(data))),
                    ('Content-Type', 'application/json'),
                ]
            )
            return [data]
        except WSGIError as e:
            raise e
        except Exception:
            log.exception('500 Internal Server Error')
            raise WSGIError('500 Internal Server Error')

    def get_challenge(self, environ):
        if self.state != 'ready':
            raise WSGIError('400 Bad Request Order')
        self.state = 'gave_challenge'
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        return {
            'challenge': self.cr.get_challenge(),
        }

    def post_response(self, environ):
        if self.state != 'gave_challenge':
            raise WSGIError('400 Bad Request Order')
        self.state = 'in_response'
        if environ['REQUEST_METHOD'] != 'POST':
            raise WSGIError('405 Method Not Allowed')
        data = environ['wsgi.input'].read()
        obj = json.loads(data.decode('utf-8'))
        nonce = obj['nonce']
        response = obj['response']
        try:
            self.cr.check_response(nonce, response)
        except identity.WrongResponse:
            self.state = 'wrong_response'
            raise WSGIError('401 Unauthorized')
        self.state = 'response_ok'
        return {'ok': True}


class ServerApp(ClientApp):
    """
    WSGI app used by the server-end of the peering process.
    """

    allowed_states = (
        'info',
        'counter_response_ok',
        'in_csr',
        'bad_csr',
        'cert_issued',
    ) + ClientApp.allowed_states

    forwarded_states = (
        'bad_csr',
        'cert_issued',
    ) + ClientApp.forwarded_states

    def __init__(self, cr, queue, pki):
        super().__init__(cr, queue)
        self.pki = pki
        self.map['/'] = self.get_info
        self.map['/csr'] = self.post_csr

    def get_info(self, environ):
        if self.state != 'info':
            raise WSGIError('400 Bad Request State')
        self.state = 'ready'
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        return {
            'id': self.cr.id,
            'user': USER,
            'host': HOST,
        }

    def post_csr(self, environ):
        if self.state != 'counter_response_ok':
            raise WSGIError('400 Bad Request Order')
        self.state = 'in_csr'
        if environ['REQUEST_METHOD'] != 'POST':
            raise WSGIError('405 Method Not Allowed')
        data = environ['wsgi.input'].read()
        d = json.loads(data.decode('utf-8'))
        csr_data = b64decode(d['csr'].encode('utf-8'))
        try:
            self.cr.check_csr_mac(csr_data, d['mac'])
            self.pki.write_csr(self.cr.peer_id, csr_data)
            self.pki.issue_cert(self.cr.peer_id, self.cr.id)
            cert_data = self.pki.read_cert(self.cr.peer_id, self.cr.id)
            key_data = self.pki.read_key(self.cr.id)
        except Exception:
            log.exception('could not issue cert')
            self.state = 'bad_csr'
            raise WSGIError('401 Unauthorized')       
        self.state = 'cert_issued'
        return {
            'cert': b64encode(cert_data).decode('utf-8'),
            'mac': self.cr.cert_mac(cert_data),
            'key': b64encode(key_data).decode('utf-8'),
        }


def run_server(queue, couch_env, bind_address, ssl_config):
    try:
        app = RootApp(couch_env)
        server = make_server(app, bind_address, ssl_config)
        env = {'port': server.port, 'url': server.url}
        log.info('Starting Dmedia HTTPD on port %d', server.port)
        queue.put(env)
        server.serve_forever()
    except Exception as e:
        log.exception('Could not start Dmedia HTTPD!')
        queue.put(e)
