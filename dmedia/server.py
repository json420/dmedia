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

from filestore import DIGEST_B32LEN, B32ALPHABET


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
            return e.body


class Server(BaseWSGI):
    def POST(self, environ, start_response):
        pass

    def PUT(self, environ, start_response):
        pass

    def GET(self, environ, start_response):
        pass

