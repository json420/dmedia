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
"""


class HTTPError(Exception):
    def __init__(self, body=b'', headers=None):
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


HTTP_METHODS = ('PUT', 'POST', 'GET', 'DELETE', 'HEAD')

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


def get_slice(environ):
    parts = environ['PATH_INFO'].lstrip('/').split('/')
    if len(parts) > 3:
        raise BadRequest()
    try:
        _id = check_id(parts[0])
    except IDError:
        raise BadRequest()
    if len(parts) > 1:
        try:
            start = int(parts[1])
        except ValueError:
            raise BadRequest()
    else:
        start = 0
    if len(parts) > 2:
        try:
            stop = int(parts[2])
        except ValueError:
            raise BadRequest()
    else:
        stop = None
    if start < 0:
        raise BadRequest()
    if not (stop is None or start < stop):
        raise BadRequest()
    return (_id, start, stop)


class Server(BaseWSGI):
    def POST(self, environ, start_response):
        pass

    def PUT(self, environ, start_response):
        pass

    def GET(self, environ, start_response):
        pass


print(BaseWSGI.http_methods)
print(Server.http_methods)

