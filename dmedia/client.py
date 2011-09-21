# dmedia: dmedia hashing protocol and file layout
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
dmedia HTTP client.
"""

from urllib.parse import urlparse
from http.client import HTTPConnection, HTTPSConnection

from filestore import LEAF_SIZE, Leaf, SmartQueue, _start_thread

from dmedia import __version__


USER_AGENT = 'dmedia {}'.format(__version__)


class HTTPError(Exception):
    """
    Base class for custom HTTP client exceptions.
    """

    def __init__(self, response, method, path):
        self.response = response
        self.method = method
        self.path = path
        self.data = response.read()
        super().__init__(
            '{} {}: {} {}'.format(response.status, response.reason, method, path)
        )


class ClientError(HTTPError):
    """
    Base class for all 4xx Client Error exceptions.
    """


class BadRequest(ClientError):
    """
    400 Bad Request.
    """


class Unauthorized(ClientError):
    """
    401 Unauthorized.
    """


class Forbidden(ClientError):
    """
    403 Forbidden.
    """


class NotFound(ClientError):
    """
    404 Not Found.
    """


class MethodNotAllowed(ClientError):
    """
    405 Method Not Allowed.
    """


class NotAcceptable(ClientError):
    """
    406 Not Acceptable.
    """


class Conflict(ClientError):
    """
    409 Conflict.

    Raised when the request resulted in an update conflict.
    """


class PreconditionFailed(ClientError):
    """
    412 Precondition Failed.
    """


class BadContentType(ClientError):
    """
    415 Unsupported Media Type.
    """


class BadRangeRequest(ClientError):
    """
    416 Requested Range Not Satisfiable.
    """


class ExpectationFailed(ClientError):
    """
    417 Expectation Failed.

    Raised when a bulk operation failed.
    """


class ServerError(HTTPError):
    """
    Used to raise exceptions for any 5xx Server Errors.
    """


errors = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    405: MethodNotAllowed,
    406: NotAcceptable,
    409: Conflict,
    412: PreconditionFailed,
    415: BadContentType,
    416: BadRangeRequest,
    417: ExpectationFailed,
}


def http_conn(url, **options):
    """
    Return (connection, parsed) tuple.

    For example:

    >>> (conn, parsed) = http_conn('http://foo.s3.amazonaws.com/')

    The returned connection will be either an ``HTTPConnection`` or
    ``HTTPSConnection`` instance based on the *url* scheme.

    The 2nd item in the returned tuple will be *url* parsed with ``urlparse()``.
    """
    u = urlparse(url)
    if u.scheme not in ('http', 'https'):
        raise ValueError('url scheme must be http or https: {!r}'.format(url))
    if not u.netloc:
        raise ValueError('bad url: {!r}'.format(url))
    klass = (HTTPConnection if u.scheme == 'http' else HTTPSConnection)
    conn = klass(u.netloc, **options)
    return (conn, u)


def bytes_range(start, stop=None):
    """
    Convert from Python slice semantics to an HTTP Range request.

    Python slice semantics are quite natural to deal with, whereas the HTTP
    Range semantics are a touch wacky, so this function will help prevent silly
    errors.

    For example, say we're requesting parts of a 10,000 byte long file.  This
    requests the first 500 bytes:

    >>> bytes_range(0, 500)
    'bytes=0-499'

    This requests the second 500 bytes:

    >>> bytes_range(500, 1000)
    'bytes=500-999'

    All three of these request the final 500 bytes:

    >>> bytes_range(9500, 10000)
    'bytes=9500-9999'
    >>> bytes_range(-500)
    'bytes=-500'
    >>> bytes_range(9500)
    'bytes=9500-'

    For details on HTTP Range header, see:

      http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    if start < 0:
        assert stop is None
        return 'bytes={}'.format(start)
    end = ('' if stop is None else stop - 1)
    return 'bytes={}-{}'.format(start, end)


def range_header(file_size, start_index=0, stop_index=None):
    if start_index == 0 and stop_index is None:
        return {}
    if start_index < 0:
        raise ValueError('start_index must be >=0; got {!r}'.format(start_index))
    if file_size < 1:
        raise ValueError('file_size must be >=1; got %r' % file_size)
    start = start_index * LEAF_SIZE
    if start >= file_size:
        raise ValueError(
            'past end of file: file_size={}, start_index={}'.format(
                file_size, start_index)
        )
    if stop_index is None:
        stop = None
    else:
        if stop_index <= start_index:
            raise ValueError(
                'stop_index <= start_index: {} <= {}'.format(stop_index, start_index)
            )
        stop = min(file_size, stop_index * LEAF_SIZE)
    return {'Range': bytes_range(start, stop)}


def response_reader(response, queue, start_index=0):
    try:
        index = start_index
        while True:
            data = response.read(LEAF_SIZE)
            if not data:
                queue.put(None)
                break
            queue.put(Leaf(index, data))
            index += 1
    except Exception as e:
        queue.put(e)


def response_iter(response, start_index=0):
    q = SmartQueue(4)
    thread = _start_thread(response_reader, response, q, start_index)
    while True:
        leaf = q.get()
        if leaf is None:
            break
        yield leaf
    thread.join()  # Make sure reader() terminates


class HTTPClient:
    def __init__(self, url, debug=False):
        (self.conn, u) = http_conn(url)
        self.basepath = (u.path if u.path.endswith('/') else u.path + '/')
        self.url = ''.join([u.scheme, '://', u.netloc, self.basepath])
        self.u = u
        if debug:
            self.conn.set_debuglevel(1)

    def request(self, method, relpath, body=None, headers=None):
        assert not relpath.startswith('/')
        path = self.basepath + relpath
        h = {'User-Agent': USER_AGENT}
        if headers:
            h.update(headers)
        try:
            self.conn.request(method, path, body, h)
            response = self.conn.getresponse()
        except Exception as e:
            self.conn.close()
            raise e
        if response.status >= 500:
            raise ServerError(response, method, path)
        if response.status >= 400:
            E = errors.get(response.status, ClientError)
            raise E(response, method, path)
        return response

    def get_leaves(self, ch, start_index=0, stop_index=None):
        headers = range_header(ch.file_size, start_index, stop_index)
        return self.request('GET', ch.id, headers=headers)

    def iter_leaves(self, ch, start_index=0, stop_index=None):
        response = self.get_leaves(ch, start_index, stop_index)
        return response_iter(response, start_index)
#        index = start_index
#        while True:
#            data = response.read(LEAF_SIZE)
#            if not data:
#                break
#            yield Leaf(index, data)
#            index += 1
        

