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
dmedia HTTP server.

== Security Note ==

To help prevent cross-site scripting attacks, the `HTTPError` raised for any
invalid request should not include any data supplied in the request.

It is helpful to include a meaningful bit a text in the response body, plus it
allows us to test that an `HTTPError` is being raised because of the condition
we expected it to be raised for.  But include only static messages.

For example, this is okay:

>>> raise BadRequest('too many slashes in request path')  #doctest: +SKIP

But this is not okay:

>>> raise BadRequest('bad path: {}'.format(environ['PATH_INFO']))  #doctest: +SKIP

"""

import json

from filestore import DIGEST_B32LEN, B32ALPHABET, LEAF_SIZE

from dmedia import __version__
from dmedia import local


HTTP_METHODS = ('PUT', 'POST', 'GET', 'DELETE', 'HEAD')


class HTTPError(Exception):
    def __init__(self, body=b'', headers=None):
        if isinstance(body, str):
            body = body.encode('utf-8')
            headers = [('Content-Type', 'text/plain; charset=utf-8')]
        self.body = body
        self.headers = ([] if headers is None else headers)
        super().__init__(self.status)


class BadRequest(HTTPError):
    status = '400 Bad Request'


class NotFound(HTTPError):
    status = '404 Not Found'


class MethodNotAllowed(HTTPError):
    status = '405 Method Not Allowed'


class Conflict(HTTPError):
    status = '409 Conflict'


class LengthRequired(HTTPError):
    status = '411 Length Required'


class PreconditionFailed(HTTPError):
    status = '412 Precondition Failed'


class BadRangeRequest(HTTPError):
    status = '416 Requested Range Not Satisfiable'


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
        raise BadRangeRequest('bad range units')
    value = value[len(unit):]
    if value.startswith('-'):
        try:
            return (int(value), None)
        except ValueError:
            raise BadRangeRequest('range -start is not an integer')  
    parts = value.split('-')
    if not len(parts) == 2:
        raise BadRangeRequest('not formatted as bytes=start-end')
    try:
        start = int(parts[0])
    except ValueError:
        raise BadRangeRequest('range start is not an integer')
    try:
        end = parts[1]
        stop = (int(end) + 1 if end else None)
    except ValueError:
        raise BadRangeRequest('range end is not an integer')
    if not (stop is None or start < stop):
        raise BadRangeRequest('range end must be less than or equal to start')
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
    assert 0 <= start < length
    assert start < stop <= length
    return 'bytes {}-{}/{}'.format(start, stop - 1, length)


class BaseWSGIMeta(type):
    def __new__(meta, name, bases, dict):
        http_methods = []
        cls = type.__new__(meta, name, bases, dict)
        for name in filter(lambda n: n in HTTP_METHODS, dir(cls)):
            method = getattr(cls, name)
            if callable(method):
                http_methods.append(name)
        cls.http_methods = frozenset(http_methods)
        return cls


class BaseWSGI(metaclass=BaseWSGIMeta):
    def __call__(self, environ, start_response):
        try:
            name = environ['REQUEST_METHOD']
            if name not in self.__class__.http_methods:
                raise MethodNotAllowed()
            return getattr(self, name)(environ, start_response)
        except HTTPError as e:
            start_response(e.status, e.headers)
            return [e.body]


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


class ReadOnlyApp(BaseWSGI):
    def __init__(self, env):
        self.local = local.LocalSlave(env)
        info = {
            'Dmedia': 'Welcome',
            'version': __version__,
            'machine_id': env.get('machine_id'),
        }
        self._info = json.dumps(info, sort_keys=True).encode('utf-8')

    def server_info(self, environ, start_response):
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [self._info]

    def GET(self, environ, start_response):
        path_info = environ['PATH_INFO']
        if path_info == '/':
            return self.server_info(environ, start_response)

        _id = path_info.lstrip('/')
        if not (len(_id) == DIGEST_B32LEN and set(_id).issubset(B32ALPHABET)):
            raise NotFound()
        try:
            doc = self.local.get_doc(_id)
            st = self.local.stat2(doc)
            fp = open(st.name, 'rb')
        except Exception:
            raise NotFound()

        if doc.get('content_type'):
            headers = [('Content-Type', doc['content_type'])]
        else:
            headers = []

        if 'HTTP_RANGE' in environ:
            (start, stop) = range_to_slice(environ['HTTP_RANGE'])                
            status = '206 Partial Content'
        else:
            start = 0
            stop = None
            status = '200 OK'

        stop = (st.size if stop is None else min(st.size, stop))
        length = str(stop - start)
        headers.append(('Content-Length', length))
        if 'HTTP_RANGE' in environ:
            headers.append(
                ('Content-Range', slice_to_content_range(start, stop, st.size))
            )

        start_response(status, headers)
        return FileSlice(fp, start, stop)


class ReadWriteApp(ReadOnlyApp):
    def PUT(self, environ, start_response):
        pass

    def POST(self, environ, start_response):
        pass

