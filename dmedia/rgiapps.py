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

from dbase32 import isdb32
from degu.util import shift_path, relative_uri, output_from_input
from degu.client import Client
from microfiber import basic_auth_header, dumps
from filestore import DIGEST_B32LEN

from .local import LocalSlave, FileNotLocal
from . import __version__


USER = os.environ.get('USER')
HOST = socket.gethostname()
RE_RANGE = re.compile('^bytes=(\d+)-(\d+)$')
log = logging.getLogger()


class RGIError(Exception):
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        super().__init__('{} {}'.format(status, reason))


def range_to_slice(value, file_size):
    """
    No bullshit HTTP Range parser from the wrong side of the tracks.

    Converts a byte-wise HTTP Range into a sane Python-esque byte-wise slice,
    and then checks that the following condition is met::

        0 <= start < stop <= file_size

    If not, a `RGIError` is raised, aborting the request handling.  This is a
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
    dmedia.rgiapps.RGIError: 416 Requested Range Not Satisfiable

    For details on the HTTP Range header, see:

        http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    assert isinstance(file_size, int)
    assert file_size > 0
    match = RE_RANGE.match(value)
    if match is None:
        raise RGIError(400, 'Bad Range Request')
    start = int(match.group(1))
    end = int(match.group(2))
    stop = end + 1
    if not (0 <= start < stop <= file_size):
        raise RGIError(416, 'Requested Range Not Satisfiable')
    return (start, stop)


def slice_to_content_range(start, stop, file_size):
    """
    Convert Python slice to HTTP Content-Range.

    For example, a slice containing the first 500 bytes of a 1234 byte file:

    >>> slice_to_content_range(0, 500, 1234)
    'bytes 0-499/1234'

    Or the 2nd 500 bytes:

    >>> slice_to_content_range(500, 1000, 1234)
    'bytes 500-999/1234'

    """
    assert 0 <= start < stop <= file_size
    end = stop - 1
    return 'bytes {}-{}/{}'.format(start, end, file_size)


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

    def __call__(self, connection, request):
        if request['path'] == [] or request['path'] == ['']:
            return self.get_info(connection, request)
        key = shift_path(request)
        if key in self.map:
            try:
                return self.map[key](connection, request)
            except RGIError as e:
                return (e.status, e.reason, {}, None)
        return (410, 'Gone', {}, None)

    def get_info(self, connection, request):
        if request['method'] != 'GET':
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
        base_headers = {
            'authorization': basic_auth_header(env['basic']),
            'host': t.netloc,
        }
        self.client = Client(address, base_headers)

    def __call__(self, connection, request):
        if '__conn' not in connection:
            connection['__conn'] = self.client.connect()
        conn = connection['__conn']
        uri = relative_uri(request)
        if uri.startswith('/_') and uri != '/_all_dbs':
            return (403, 'Forbidden', {}, None)
        response = conn.request(
            request['method'],
            uri,
            request['headers'],
            output_from_input(connection, request['body']) 
        )
        return (
            response.status,
            response.reason,
            response.headers,
            output_from_input(connection, response.body)
        )


class FilesApp:
    def __init__(self, env):
        self.local = LocalSlave(env)

    def __call__(self, connection, request):
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
        except FileNotLocal:
            return (404, 'Not Found', {}, None)

        if request['method'] == 'HEAD':
            return (200, 'OK', {'content-length': st.size}, None)
        if 'range' in request['headers']:
            (start, stop) = range_to_slice(request['headers']['range'], st.size)
            (status, reason) = (206, 'Partial Content')
            headers = {
                'content-range': slice_to_content_range(start, stop, st.size),
                'content-length': (stop - start),
            }
        else:
            start = 0
            stop = st.size
            (status, reason) = (200, 'OK')
            headers = {'content-length': st.size}
        fp.seek(start)
        body = connection['rgi.FileOutput'](fp, headers['content-length'])
        log.info(
            'Sending bytes %s[%d:%d] to %r', _id, start, stop, connection['client']
        )
        return (status, reason, headers, body)


def build_root_app(couch_env):
    return RootApp(couch_env)
