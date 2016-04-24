# dmedia: distributed media library
# Copyright (C) 2014 Novacut Inc
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
Dmedia RGI applications.

Experimental port of the old `dmedia.server` WSGI application to our new RGI
specification (REST Gateway Interface).
"""

from urllib.parse import urlparse
import os
import socket
import logging
import re
import json
import ssl
from base64 import b64encode, b64decode

from dbase32 import isdb32, random_id
from degu.client import Client
from microfiber import basic_auth_header, dumps
from filestore import DIGEST_B32LEN, FileNotFound

from .local import LocalSlave, FileNotLocal, NoSuchFile
from . import __version__, identity


USER = os.environ.get('USER')
HOST = socket.gethostname()
RE_RANGE = re.compile('^bytes=(\d+)-(\d+)$')
log = logging.getLogger()


class RootApp:
    """
    Main Dmedia RGI app.
    """

    def __init__(self, env):
        self.user_id = env['user_id']
        obj = {
            'user_id': env['user_id'],
            'machine_id': env['machine_id'],
            'version': __version__,
            'user': USER,
            'host': HOST,
        }
        self.info = dumps(obj).encode('utf-8')
        self.info_length = len(self.info)
        self.proxy = ProxyApp(env)
        self.files = FilesApp(env)
        self.map = {
            '': self.get_info,
            'couch': self.proxy,
            'files': self.files,
        }
        self._marker = random_id()

    def __call__(self, session, request, bodies):
        if session.store.get('_marker') != self._marker:
            raise Exception(
                'session marker {!r} != {!r}, on_connect() was not called'.format(
                    session.store.get('_marker'), self._marker
                )
            )
        if not request.path:
            return self.get_info(session, request, bodies)
        handler = self.map.get(request.shift_path())
        if handler is None:
            return (410, 'Gone', {}, None)
        return handler(session, request, bodies)

    def on_connect(self, session, sock):
        if not isinstance(sock, ssl.SSLSocket):
            log.error('Non SSL connection from %r', session.address)
            return False
        if sock.context.verify_mode != ssl.CERT_REQUIRED:
            log.error('sock.context.verify_mode != ssl.CERT_REQUIRED')
            return False
        session.store['_marker'] = self._marker
        return True

    def get_info(self, session, request, bodies):
        if request.method != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        headers = {
            'content-length': self.info_length,
            'content-type': 'application/json',
        }
        return (200, 'OK', headers, self.info)


class ProxyApp:
    """
    Reverse proxy app so Degu can be used as an SSL frontend for CouchDB.
    """

    def __init__(self, env):
        t = urlparse(env['url'])
        address = (t.hostname, t.port)
        self.client = Client(address)
        self._authorization = basic_auth_header(env['basic'])

    def __call__(self, session, request, bodies):
        conn = session.store.get('conn')
        if conn is None:
            conn = self.client.connect()
            session.store['conn'] = conn
        uri = request.build_proxy_uri()
        if uri.startswith('/_') and uri != '/_all_dbs':
            return (403, 'Forbidden', {}, None)
        request.headers['authorization'] = self._authorization
        return conn.request(request.method, uri, request.headers, request.body)


class FilesApp:
    def __init__(self, env):
        self.local = LocalSlave(env)

    def __call__(self, session, request, api):
        if request.method not in {'GET', 'HEAD'}:
            return (405, 'Method Not Allowed', {}, None)
        _id = request.shift_path()
        if not isdb32(_id):
            return (400, 'Bad File ID', {}, None)
        if len(_id) != DIGEST_B32LEN:
            return (400, 'Bad File ID Length', {}, None)
        if request.path:
            return (410, 'Gone', {}, None)
        if request.query:
            return (400, 'No Query For You', {}, None)
        if request.method == 'HEAD' and 'range' in request.headers:
            return (400, 'Cannot Range with HEAD', {}, None)
        try:
            doc = self.local.get_doc(_id)
            st = self.local.stat2(doc)
            fp = open(st.name, 'rb')
        except (NoSuchFile, FileNotLocal, FileNotFound):
            log.exception('Error requesting %s', _id)
            return (404, 'Not Found', {}, None)

        if request.method == 'HEAD':
            return (200, 'OK', {'content-length': st.size}, None)
        _range = request.headers.get('range')
        if _range is not None:
            start = _range.start
            stop = _range.stop
            content_length = stop - start
            fp.seek(start)
            status = 206
            reason = 'Partial Content'
            headers = {'content-range': api.ContentRange(start, stop, st.size)}
            log.info('Sending partial file %s[%d:%d] (%d bytes) to %r',
                _id, start, stop, content_length, session.address
            )
        else:
            content_length = st.size
            status = 200
            reason = 'OK'
            headers = {}
            log.info('Sending file %s (%d bytes) to %r',
                _id, content_length, session.address
            )
        body = api.Body(fp, content_length)
        return (status, reason, headers, body)


class InfoApp:
    """
    RGI app initially used by the client-end of the peering process.
    """

    def __init__(self, _id):
        self.id = _id
        obj = {
            'id': _id,
            'version': __version__,
            'user': USER,
            'host': HOST,
        }
        self.body = dumps(obj).encode()

    def __call__(self, session, request, bodies):
        log.info('InfoApp: %s: %s %s', session, request.method, request.uri)
        if request.path:
            return (410, 'Gone', {}, None)
        if request.method != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        return (200, 'OK', {'content-type': 'application/json'}, self.body)


class ClientApp:
    """
    RGI app used by the client-end of the peering process.
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
            ('challenge',): self.get_challenge,
            ('response',): self.post_response,
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

    def __call__(self, session, request, bodies):
        # FIXME: We need to replace with a Degu style app.on_connect() handler
#        if environ['wsgi.multithread'] is not False:
#            raise WSGIError('500 Internal Server Error')
#        if environ.get('SSL_CLIENT_VERIFY') != 'SUCCESS':
#            raise WSGIError('403 Forbidden SSL')
#        if environ.get('SSL_CLIENT_S_DN_CN') != self.cr.peer_id:
#            raise WSGIError('403 Forbidden Subject')
#        if environ.get('SSL_CLIENT_I_DN_CN') != self.cr.peer_id:
#            raise WSGIError('403 Forbidden Issuer')

        log.info('ClientApp: %s: %s %s', session, request.method, request.uri)
        handler = self.map.get(tuple(request.path))
        if handler is None:
            return (410, 'Gone', {}, None)
        return handler(session, request, bodies)

    def get_challenge(self, session, request, bodies):
        if request.method != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        if self.state != 'ready':
            return (400, 'Bad Request Order', {}, None)
        self.state = 'gave_challenge'
        obj = {'challenge': self.cr.get_challenge()}
        body = dumps(obj).encode()
        return (200, 'OK', {'content-type': 'application/json'}, body)

    def post_response(self, session, request, bodies):
        if request.method != 'POST':
            return (405, 'Method Not Allowed', {}, None)
        if self.state != 'gave_challenge':
            return (400, 'Bad Request Order', {}, None)
        self.state = 'in_response'
        obj = json.loads(request.body.read().decode())
        nonce = obj['nonce']
        response = obj['response']
        try:
            self.cr.check_response(nonce, response)
        except identity.WrongResponse:
            self.state = 'wrong_response'
            return (401, 'Unauthorized', {}, None)
        self.state = 'response_ok'
        obj = {'ok': True}
        body = dumps(obj).encode()
        return (200, 'OK', {'content-type': 'application/json'}, body)


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
        self.map[tuple()] = self.get_info
        self.map[('csr',)] = self.post_csr
        info = {
            'id': cr.id,
            'user': USER,
            'host': HOST,
        }
        self.info_body = dumps(info).encode()

    def get_info(self, session, request, bodies):
        if request.method != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        if self.state != 'info':
            return (400, 'Bad Request State', {}, None)
        self.state = 'ready'
        return (200, 'OK', {'content-type': 'application/json'}, self.info_body)

    def post_csr(self, session, request, bodies):
        if request.method != 'POST':
            return (405, 'Method Not Allowed', {}, None)
        if self.state != 'counter_response_ok':
            return (400, 'Bad Request Order', {}, None)
        self.state = 'in_csr'
        d = json.loads(request.body.read().decode())
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
            return (401, 'Unauthorized', {}, None)       
        self.state = 'cert_issued'
        obj = {
            'cert': b64encode(cert_data).decode('utf-8'),
            'mac': self.cr.cert_mac(cert_data),
            'key': b64encode(key_data).decode('utf-8'),
        }
        body = dumps(obj).encode()
        return (200, 'OK', {'content-type': 'application/json'}, body)



def build_root_app(couch_env):
    return RootApp(couch_env)

