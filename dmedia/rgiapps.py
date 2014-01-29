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

import threading
from urllib.parse import urlparse
import os
import socket
import logging

from dbase32 import isdb32
from degu.base import build_uri, make_output_from_input
from degu.server import shift_path
from degu.client import Client
from microfiber import basic_auth_header, dumps
from filestore import DIGEST_B32LEN

from .local import LocalSlave
from . import __version__


USER = os.environ.get('USER')
HOST = socket.gethostname()
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

    def __call__(self, request):
        if request['path'] == [] or request['path'] == ['']:
            return self.get_info(request)
        key = shift_path(request)
        if key in self.map:
            return self.map[key](request)
        return (410, 'Gone', {}, None)

    def get_info(self, request):
        if request['method'] != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        headers = {
            'content-length': self.info_length,
            'content-type': 'application/json',
        }
        return (200, 'OK', headers, self.info)


class ProxyApp:
    def __init__(self, env):
        self.threadlocal = threading.local()
        t = urlparse(env['url'])
        self.hostname = t.hostname
        self.port = t.port
        self.netloc = t.netloc
        self.basic_auth = basic_auth_header(env['basic'])

    def get_client(self):
        if not hasattr(self.threadlocal, 'client'):
            self.threadlocal.client = Client(self.hostname, self.port)
        return self.threadlocal.client

    def __call__(self, request):
        client = self.get_client()
        try:
            method = request['method']
            uri = build_uri(request['path'], request['query'])
            headers = request['headers'].copy()
            headers['authorization'] = self.basic_auth
            headers['host'] = self.netloc
            body = make_output_from_input(request['body'])
            response = client.request(method, uri, headers, body)
            return (
                response.status,
                response.reason,
                response.headers,
                make_output_from_input(response.body)
            )
        except Exception:
            client.close()
            raise


class FilesApp:
    def __init__(self, env):
        self.local = LocalSlave(env)

    def __call__(self, request):
        if request['method'] not in {'GET', 'HEAD'}:
            return (405, 'Method Not Allowed', {}, None)
        _id = shift_path(request)
        if not isdb32(_id):
            return (400, 'Bad File ID', {}, None)
        if len(_id) != DIGEST_B32LEN:
            return (400, 'Bad File ID Length', {}, None)
        if request['path'] != []:
            return (410, 'Gone', {}, None)
        if request['query']:
            return (400, 'No Query For You', {}, None)
        if request['method'] == 'HEAD' and 'range' in request['headers']:
            return (400, 'Cannot Range with HEAD', {}, None)
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


def build_root_app(couch_env):
    return RootApp(couch_env)
