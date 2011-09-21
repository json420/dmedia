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


class HTTPClient:
    def __init__(self, url):
        (self.conn, u) = http_conn(url)
        self.basepath = (u.path if u.path.endswith('/') else u.path + '/')
        self.url = ''.join([u.scheme, '://', u.netloc, self.basepath])
        self.u = u

    def request(self, method, relpath, body=None, headers=None):
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

