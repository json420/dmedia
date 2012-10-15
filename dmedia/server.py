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
from wsgiref.util import shift_path_info
import logging

from filestore import DIGEST_B32LEN, B32ALPHABET, LEAF_SIZE
import microfiber
from microfiber import dumps

import dmedia
from dmedia import __version__
from dmedia.httpd import WSGIError, make_server 
from dmedia import local, peering


USER = os.environ.get('USER')
HOST = socket.gethostname()
log = logging.getLogger()


def iter_headers(environ):
    for (key, value) in environ.items():
        if key in ('CONTENT_LENGHT', 'CONTENT_TYPE'):
            yield (key.replace('_', '-').lower(), value)
        elif key.startswith('HTTP_'):
            yield (key[5:].replace('_', '-').lower(), value)


def request_args(environ):
    headers = dict(iter_headers(environ))
    if environ['wsgi.input']._avail:
        body = environ['wsgi.input'].read()
    else:
        body = None
    return (environ['REQUEST_METHOD'], environ['PATH_INFO'], body, headers)


def get_slice(environ):
    parts = environ['PATH_INFO'].lstrip('/').split('/')
    if len(parts) > 3:
        raise BadRequest('too many slashes in request path')
    _id = parts[0]
    if not (len(_id) == DIGEST_B32LEN and set(_id).issubset(B32ALPHABET)):
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


def range_to_slice(value):
    """
    Convert from HTTP Range request to Python slice semantics.

    Python slice semantics are quite natural to deal with, whereas the HTTP
    Range semantics are a touch wacky, so this function will help prevent silly
    errors.

    For example, say we're requesting parts of a 10,000 byte long file.  This
    requests the first 500 bytes:

    >>> range_to_slice('bytes=0-499')
    (0, 500)

    This requests the second 500 bytes:

    >>> range_to_slice('bytes=500-999')
    (500, 1000)

    All three of these request the final 500 bytes:

    >>> range_to_slice('bytes=9500-9999')
    (9500, 10000)
    >>> range_to_slice('bytes=-500')
    (-500, None)
    >>> range_to_slice('bytes=9500-')
    (9500, None)

    For details on HTTP Range header, see:

      http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    unit = 'bytes='
    if not value.startswith(unit):
        raise WSGIError('400 Bad Range Units')
    value = value[len(unit):]
    if value.startswith('-'):
        try:
            return (int(value), None)
        except ValueError:
            raise WSGIError('400 Bad Range Negative Start')  
    parts = value.split('-')
    if not len(parts) == 2:
        raise WSGIError('400 Bad Range Format')
    try:
        start = int(parts[0])
    except ValueError:
        raise WSGIError('400 Bad Range Start')
    try:
        end = parts[1]
        stop = (int(end) + 1 if end else None)
    except ValueError:
        raise WSGIError('400 Bad Range End')
    if not (stop is None or start < stop):
        raise WSGIError('400 Bad Range')
    return (start, stop)


def slice_to_content_range(start, stop, length):
    """
    Convert Python slice to HTTP Content-Range.

    For example, a slice containing the first 500 bytes of a 1234 byte file:

    >>> slice_to_content_range(0, 500, 1234)
    'bytes 0-499/1234'

    Or the 2nd 500 bytes:

    >>> slice_to_content_range(500, 1000, 1234)
    'bytes 500-999/1234'

    """
    assert 0 <= start < stop <= length
    return 'bytes {}-{}/{}'.format(start, stop - 1, length)


MiB = 1024 * 1024


class FileSlice:
    __slots__ = ('fp', 'start', 'stop')

    def __init__(self, fp, start=0, stop=None):
        self.fp = fp
        self.start = start
        self.stop = stop

    def __iter__(self):
        self.fp.seek(self.start)
        remaining = self.stop - self.start
        while remaining:
            read = min(remaining, MiB)
            remaining -= read
            data = self.fp.read(read)
            assert len(data) == read
            yield data
        assert remaining == 0


class RootApp:
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
        self.map = {
            '': self.get_info,
            'couch': ProxyApp(env),
            'files': FilesApp(env),
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
    def __init__(self, env, debug=False):
        self.debug = debug
        self.client = microfiber.CouchBase(env)
        self.target_host = self.client.ctx.t.netloc
        self.basic_auth = microfiber.basic_auth_header(env['basic'])

    def __call__(self, environ, start_response):
        (method, path, body, headers) = request_args(environ)
        db = shift_path_info(environ)
        if db and db.startswith('_'):
            raise WSGIError('403 Forbidden')
        if self.debug:
            print('')
            print('{REQUEST_METHOD} {PATH_INFO}'.format(**environ))
            for key in sorted(headers):
                print('{}: {}'.format(key, headers[key]))
        headers['host'] = self.target_host
        headers['authorization'] = self.basic_auth

        response = self.client.raw_request(method, path, body, headers)
        status = '{} {}'.format(response.status, response.reason)
        headers = response.getheaders()
        if self.debug:
            print('-' * 80)
            print(status)
            for (key, value) in headers:
                print('{}: {}'.format(key, value))
        start_response(status, headers)
        body = response.read()
        if body:
            return [body]
        return []


class FilesApp:
    def __init__(self, env):
        self.local = local.LocalSlave(env)

    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        _id = shift_path_info(environ)
        if not (len(_id) == DIGEST_B32LEN and set(_id).issubset(B32ALPHABET)):
            raise WSGIError('400 Bad Request ID')
        try:
            doc = self.local.get_doc(_id)
            st = self.local.stat2(doc)
            fp = open(st.name, 'rb')
        except Exception:
            raise WSGIError('404 Not Found')

        if 'HTTP_RANGE' in environ:
            (start, stop) = range_to_slice(environ['HTTP_RANGE'])                
            status = '206 Partial Content'
        else:
            start = 0
            stop = None
            status = '200 OK'
            
        # '416 Requested Range Not Satisfiable'

        stop = (st.size if stop is None else min(st.size, stop))
        length = str(stop - start)
        headers = [('Content-Length', length)]
        if 'HTTP_RANGE' in environ:
            headers.append(
                ('Content-Range', slice_to_content_range(start, stop, st.size))
            )
        start_response(status, headers)
        return FileSlice(fp, start, stop)


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
            '/response': self.put_response,
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

    def put_response(self, environ):
        if self.state != 'gave_challenge':
            raise WSGIError('400 Bad Request Order')
        self.state = 'in_response'
        if environ['REQUEST_METHOD'] != 'PUT':
            raise WSGIError('405 Method Not Allowed')
        data = environ['wsgi.input'].read()
        obj = json.loads(data.decode('utf-8'))
        nonce = obj['nonce']
        response = obj['response']
        try:
            self.cr.check_response(nonce, response)
        except peering.WrongResponse:
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
        csr_data = base64.b64decode(d['csr'].encode('utf-8'))
        try:
            self.cr.check_csr_mac(csr_data, d['mac'])
            self.pki.write_csr(self.cr.peer_id, csr_data)
            self.pki.issue_cert(self.cr.peer_id, self.cr.id)
            cert_data = self.pki.read_cert(self.cr.peer_id, self.cr.id)
        except Exception as e:
            log.exception('could not issue cert')
            self.state = 'bad_csr'
            raise WSGIError('401 Unauthorized')       
        self.state = 'cert_issued'
        return {
            'cert': base64.b64encode(cert_data).decode('utf-8'),
            'mac': self.cr.cert_mac(cert_data),
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
