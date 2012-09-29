# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
A tiny WSGI HTTP 1.1 server with IPv6 and SSL support.

This server is focused on two goals:

    1. High throughput and low latency for the moderate concurrency needed by a
       desktop HTTP server

    2. Security, even at the expense of being a fully complaint HTTP 1.1 server,
       or a fully compliant WSGI 1.0 server

As such, this is a strict HTTP 1.1 server that only supports HTTP 1.1 clients.
The server is only concerned with two HTTP clients: Microfiber, and the CouchDB
replicator.

Some notable HTTP 1.1 features not supported:

    * Does not support multi-line headers

    * Dose not support multiple occurrences of the same header (in parsing,
      the last occurrence replaces the previous)

    * Parses the request-line and header-lines more strictly than required by
      RFC 2616

    * Only supports the GET, HEAD, DELETE, PUT, and POST methods, while blocking
      everything else

Some missing WSGI features, and deviations from PEP 3333:

    * start_response() does not return a write() callable

    * Does not try to guess the response Content-Length, requires app always to
      explicitly provide the Content-Length when it provides a response body

And some general security restrictions:

    * To prevent directory traversal attacks, it rejects any requests for URI
      containing ".." as a substring.

    * For similar reasons, it rejects any requests for URI not starting with "/"

    * Errors never return any error text in the response body (no traces, etc)


For additional info, see RFC 2616:

    http://www.w3.org/Protocols/rfc2616/rfc2616.html

And PEP-3333:

    http://www.python.org/dev/peps/pep-3333/
"""

import socket
import ssl
import threading
import platform

from dmedia import __version__


SERVER_SOFTWARE = 'Dmedia/{} ({} {}; {})'.format(__version__, 
    platform.dist()[0], platform.dist()[1], platform.machine()
)
MAX_LINE = 4 * 1024
MAX_HEADER_COUNT = 10
TYPE_ERROR = '{}: need a {!r}; got a {!r}: {!r}'


class WSGIError(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__(status)


def build_server_ssl_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.load_cert_chain(config['cert_file'], config['key_file'])
    if 'ca_file' in config or 'ca_path' in config:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    return ctx


def start_thread(target, *args):
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


def parse_request(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Bad Request Line Missing CRLF')
    line = line_bytes[:-2].decode('latin_1')
    parts = line.split(' ')
    if len(parts) != 3:
        raise WSGIError('400 Bad Request Line')
    return parts


def parse_header(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Bad Header Line Missing CRLF')
    line = line_bytes[:-2].decode('latin_1')
    parts = line.split(': ', 1)
    if len(parts) != 2:
        raise WSGIError('400 Bad Header Line')
    (name, value) = parts
    if '_' in name:
        raise WSGIError('400 Bad Header Name')
    name = name.replace('-', '_').upper()
    if name in ('CONTENT_TYPE', 'CONTENT_LENGTH'):
        return (name, value)
    return ('HTTP_' + name, value)


def request_content_length(environ):
    content_length = environ.get('CONTENT_LENGTH')
    if content_length is None:
        return
    try:
        content_length = int(content_length)
    except ValueError:
        raise WSGIError('400 Bad Content Length')
    if content_length < 0:
        raise WSGIError('400 Negative Content Length')
    return content_length   


def response_content_length(response_headers):
    for (name, value) in response_headers:
        if name.lower() == 'content-length':
            return int(value)


def iter_response_lines(status, headers):
    yield 'HTTP/1.1 {}\r\n'.format(status)
    for (key, value) in headers:
        yield '{}: {}\r\n'.format(key, value)
    yield '\r\n'


class Input:
    """
    Used for environ['wsgi.input']
    """

    __slots__ = ('_rfile', '_avail', '_method')

    def __init__(self, rfile, environ):
        self._rfile = rfile
        self._avail = request_content_length(environ)
        self._method = environ['REQUEST_METHOD']

    def __repr__(self):
        return 'httpd.Input({})'.format(self._avail)

    def read(self, size=None):
        if self._method not in ('PUT', 'POST'):
            raise WSGIError('500 Internal Server Error')
        if self._avail is None:
            raise WSGIError('411 Length Required')
        if self._avail == 0:
            return b''
        if not (size is None or size > 0):
            raise WSGIError('500 Internal Server Error')
        size = (self._avail if size is None else min(self._avail, size))
        self._avail -= size
        buf = self._rfile.read(size)
        assert len(buf) == size
        return buf


MiB = 1024 * 1024

class FileWrapper:
    """
    Used for environ['wsgi.file_wrapper']
    """

    __slots__ = ('fp', 'content_length', '_closed')

    def __init__(self, fp, content_length):
        assert callable(fp.read)
        assert isinstance(content_length, int)
        assert content_length > 0
        self.fp = fp
        self.content_length = content_length
        self._closed = False

    def __iter__(self):
        assert not self._closed
        remaining = self.content_length
        while remaining:
            read = min(remaining, MiB)
            remaining -= read
            data = self.fp.read(read)
            assert len(data) == read
            yield data
        assert remaining == 0
        self._closed = True


class Handler:
    __slots__ = ('app', 'environ', 'conn', 'rfile', 'wfile', 'start')

    def __init__(self, app, environ, conn):
        self.app = app
        self.environ = environ
        self.conn = conn
        self.rfile = conn.makefile('rb')
        self.wfile = conn.makefile('wb')

    def handle_many(self):
        while self.handle_one():
            pass

    def handle_one(self):
        self.start = None
        environ = self.environ.copy()
        try:
            environ.update(self.build_request_environ())
            result = self.app(environ, self.start_response)
        except WSGIError as e:
            self.send_status_only(e.status)
            return False
        self.send_response(environ, result)
        return True

    def build_request_environ(self):
        """
        Builds the *environ* fragment unique to a single HTTP request.
        """
        environ = {}

        # Parse the request line
        request_line = self.rfile.readline(MAX_LINE + 1)
        if len(request_line) > MAX_LINE:
            raise WSGIError('414 Request-URI Too Long')
        (method, uri, protocol) = parse_request(request_line)
        if protocol != 'HTTP/1.1':
            raise WSGIError('505 HTTP Version Not Supported')
        if method not in ('GET', 'HEAD', 'POST', 'PUT', 'DELETE'):
            raise WSGIError('405 Method Not Allowed')
        if not uri.startswith('/'):
            raise WSGIError('400 Bad Request Path')
        if '..' in uri:  # Prevent path-traversal attacks
            raise WSGIError('400 Bad Request Path Naughty')
        parts = uri.split('?')
        if len(parts) > 2:
            raise WSGIError('400 Bad Request URI')
        if len(parts) == 2:
            (path, query) = parts
        else:
            (path, query) = (parts[0], '')
        environ['REQUEST_METHOD'] = method
        environ['PATH_INFO'] = path
        environ['QUERY_STRING'] = query

        # Parse the headers
        count = 0
        while True:
            header_line = self.rfile.readline(MAX_LINE + 1)
            if len(header_line) > MAX_LINE:
                raise WSGIError('431 Request Header Line Too Long')
            if header_line == b'\r\n':
                break
            count +=1
            if count > MAX_HEADER_COUNT:
                raise WSGIError('431 Too Many Request Headers')
            (name, value) = parse_header(header_line)
            environ[name] = value

        # Setup wsgi.input
        environ['wsgi.input'] = Input(self.rfile, environ)

        return environ

    def start_response(self, status, response_headers, exc_info=None):
        self.start = (status, response_headers)

    def send_response(self, environ, result):
        (status, response_headers) = self.start
        content_length = response_content_length(response_headers)
        if content_length is None:
            assert result == []
        response_headers.append(
            ('Server', SERVER_SOFTWARE)
        )
        preample = ''.join(iter_response_lines(status, response_headers))
        self.wfile.write(preample.encode('latin_1'))
        if content_length is not None:
            total = 0
            for buf in result:
                total += len(buf)
                assert total <= content_length
                self.wfile.write(buf)
            assert total == content_length
        self.wfile.flush()

    def send_status_only(self, status):
        preample = ''.join(iter_response_lines(status, []))
        self.wfile.write(preample.encode('latin_1'))
        self.wfile.flush()


class HTTPServer:
    def __init__(self, app, bind_address='::1', context=None, threaded=False):
        if not callable(app):
            raise TypeError('app not callable: {!r}'.format(app))
        if bind_address not in ('::1', '::'):
            raise ValueError('invalid bind_address: {!r}'.format(bind_address))
        if not (context is None or isinstance(context, ssl.SSLContext)):
            raise TypeError(TYPE_ERROR.format(
                'context', ssl.SSLContext, type(context), context)
            )
        if context is not None and context.protocol != ssl.PROTOCOL_TLSv1:
            raise Exception('context.protocol must be ssl.PROTOCOL_TLSv1')
        self.app = app
        self.socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        self.socket.bind((bind_address, 0))
        (host, port) = self.socket.getsockname()[:2]
        self.name = socket.getfqdn(host)
        self.port = port
        self.context = context
        self.threaded = threaded
        self.scheme = ('http' if context is None else 'https')
        self.url = '{}://[::1]:{}/'.format(self.scheme, self.port)
        self.environ = self.build_base_environ()

    def build_base_environ(self):
        """
        Builds the base *environ* used throughout instance lifetime.
        """
        environ = {
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'SERVER_SOFTWARE': SERVER_SOFTWARE,
            'SERVER_NAME': self.name,
            'SERVER_PORT': str(self.port),
            'SCRIPT_NAME': '',
            'wsgi.version': '(1, 0)',
            'wsgi.url_scheme': self.scheme,
            'wsgi.multithread': self.threaded,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
            'wsgi.file_wrapper': FileWrapper
        }
        if self.context is not None:
            environ['SSL_PROTOCOL'] = 'TLSv1'
        return environ

    def build_connection_environ(self, conn, address):
        """
        Builds the *environ* fragment unique to a TCP (and SSL) connection.
        """
        environ = {
            'REMOTE_ADDR': address[0],
            'REMOTE_PORT': str(address[1]),
        }
        if self.context is None:
            return environ

        peercert = conn.getpeercert()
        if peercert is None:
            if self.context.verify_mode == ssl.CERT_REQUIRED:
                raise Exception(
                    'peercert is None but verify_mode == CERT_REQUIRED'
                )
            return environ

        if self.context.verify_mode == ssl.CERT_REQUIRED:
            environ['SSL_CLIENT_VERIFY'] = 'SUCCESS'
        subject = dict(peercert['subject'][0])
        if 'commonName' in subject:
            environ['SSL_CLIENT_S_DN_CN'] = subject['commonName']
        issuer = dict(peercert['issuer'][0])
        if 'commonName' in issuer:
            environ['SSL_CLIENT_I_DN_CN'] = issuer['commonName']
        return environ

    def serve_forever(self):
        self.socket.listen(5)
        while True:
            (conn, address) = self.socket.accept()
            if self.threaded:
                conn.settimeout(32)
                start_thread(self.handle_connection, conn, address)
            else:
                conn.settimeout(1)
                self.handle_connection(conn, address)

    def handle_connection(self, conn, address):
        try:
            if self.context is not None:
                conn = self.context.wrap_socket(conn, server_side=True)
            self.handle_requests(conn, address)
            conn.shutdown(socket.SHUT_RDWR)
        except socket.error:
            pass       
        finally:
            conn.close()

    def handle_requests(self, conn, address):
        environ = self.environ.copy()
        environ.update(
            self.build_connection_environ(conn, address)
        )
        handler = Handler(self.app, environ, conn)
        if self.threaded:
            handler.handle_many()
        else:
            handler.handle_one()

