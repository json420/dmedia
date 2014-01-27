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

from degu.base import build_uri, make_output_from_input
from degu.client import Client
from microfiber import basic_auth_header


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

    def get_info(self, request):
        if request['method'] != 'GET':
            return (405, 'Method Not Allowed', {}, None)
        headers = {
            {'content-length': self.info_length},
            {'content-type': 'application/json'},
        }
        return (200, 'OK', headers, self.info)


class ProxyApp:
    def __init__(self, env):
        self.threadlocal = threading.local()
        t = urlparse(env['url'])
        self.hostname = t.hostname
        self.port = t.port
        self.target_host = t.netloc
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
            headers['host'] = self.target_host
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
