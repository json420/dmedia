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
"""

import socket
import ssl
import select
import threading
import platform
from microfiber import dumps
import logging

from usercouch import bind_socket, build_url
from dmedia import __version__


SERVER_SOFTWARE = 'Dmedia/{} ({} {}; {})'.format(__version__, 
    platform.dist()[0], platform.dist()[1], platform.machine()
)
MAX_LINE = 8 * 1024
MAX_HEADER_COUNT = 10
TYPE_ERROR = '{}: need a {!r}; got a {!r}: {!r}'
log = logging.getLogger()


def start_thread(target, *args):
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread

 
def bind_socket(bind_address):
    """
    Bind a socket to *bind_address* and a random port.

    For IPv4, *bind_address* must be ``'127.0.0.1'`` to listen only internally,
    or ``'0.0.0.0'`` to accept outside connections.  For example:

    >>> sock = bind_socket('127.0.0.1')

    For IPv6, *bind_address* must be ``'::1'`` to listen only internally, or
    ``'::'`` to accept outside connections.  For example:

    >>> sock = bind_socket('::1')

    The random port will be chosen by the operating system based on currently
    available ports.
    """
    if bind_address in ('127.0.0.1', '0.0.0.0'):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    elif bind_address in ('::1', '::'):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    else:
        raise ValueError('invalid bind_address: {!r}'.format(bind_address))
    #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((bind_address, 0))
    return sock


def build_ssl_server_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.load_cert_chain(config['cert_file'],
        keyfile=config.get('key_file')
    )
    if 'ca_file' in config or 'ca_path' in config:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    return ctx


def do_ssl_handshake(conn):   
    while True:
        try:
            conn.do_handshake()
            break
        except ssl.SSLError as err:
            if err.args[0] == ssl.SSL_ERROR_WANT_READ:
                select.select([conn], [], [])
            elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                select.select([], [conn], [])
            else:
                raise err


def parse_request(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise ValueError('Does not end with CRLF')
    line = line_bytes[:-2].decode('latin_1')
    if '\r' in line:
        raise ValueError('Line contains other CR')
    if '\n' in line:
        raise ValueError('Line contains other LF')
    parts = line.split(' ')
    if len(parts) != 3:
        raise ValueError('Does not have exactly 3 parts')
    return parts


def parse_header(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise ValueError('Does not end with CRLF')
    line = line_bytes[:-2].decode('latin_1')
    if '\r' in line:
        raise ValueError('Line contains other CR')
    if '\n' in line:
        raise ValueError('Line contains other LF')
    parts = line.split(': ', 1)
    if len(parts) != 2:
        raise ValueError('Does not have exactly 2 parts')
    (name, value) = parts
    if '_' in name:
        raise ValueError("Unexpected '_' in header name")
    key = name.replace('-', '_').upper()
    if key in ('CONTENT_TYPE', 'CONTENT_LENGTH'):
        return (key, value)
    return ('HTTP_' + key, value)


def iter_response_lines(status, headers):
    yield 'HTTP/1.1 {}\r\n'.format(status)
    for (key, value) in headers:
        yield '{}: {}\r\n'.format(key, value)
    yield '\r\n'


class Handler:
    def __init__(self, app, environ, conn):
        self.app = app
        self.environ = environ
        self.conn = conn
        self.rfile = conn.makefile('rb')
        self.wfile = conn.makefile('wb')

    def run(self):
        count = 1
        while True:
            log.info('handling %d', count)
            self.handle_request()
            count += 1

    def handle_request(self):
        self.start = None
        environ = self.environ.copy()
        error = self.parse_request(environ)
        if error is None:
            body = self.app(environ, self.start_response)
            self.send_response(body)
        else:
            self.start_response(error, [])
            self.send_response(None)

    def parse_request(self, environ):
        # Parse the request line
        request_line = self.rfile.readline(MAX_LINE + 1)
        if len(request_line) > MAX_LINE:
            return '414 Request-URI Too Long'
        try:
            (method, uri, protocol) = parse_request(request_line)
        except Exception:
            return '400 Bad Request'
        if protocol != 'HTTP/1.1':
            return '400 Wrong HTTP Protocol'
        if method not in ('GET', 'HEAD', 'POST', 'PUT', 'DELETE'):
            return '405 Method Not Allowed'
        parts = uri.split('?')
        if len(parts) > 2:
            return '400 Bad Request'
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
                return '431 Request Header Field Too Large'
            if header_line == b'\r\n':
                return
            if count > MAX_HEADER_COUNT:
                return '431 Too Many Request Header Fields'
            try:
                (key, value) = parse_header(header_line)
            except Exception:
                return '400 Bad Request'
            environ[key] = value
            count += 1

    def start_response(self, status, response_headers, exc_info=None):
        self.start = (status, response_headers)

    def send_response(self, result):
        (status, response_headers) = self.start
        response_headers.append(
            ('Server', SERVER_SOFTWARE)
        )
        preample = ''.join(iter_response_lines(status, response_headers))
        if isinstance(result, (list, tuple)) and len(result) == 1:
            body = result[0]
        #self.wfile.write(preample.encode('latin_1') + body)
        self.wfile.write(preample.encode('latin_1'))
        self.wfile.write(body)
        self.wfile.flush()


class Server:
    def __init__(self, app, bind_address='::1', context=None, threaded=False):
        if not callable(app):
            raise TypeError('app not callable: {!r}'.format(app))
        if not (context is None or isinstance(context, ssl.SSLContext)):
            raise TypeError(TYPE_ERROR.format(
                'context', ssl.SSLContext, type(context), context)
            )
        self.app = app
        self.socket = bind_socket(bind_address)
        (host, port) = self.socket.getsockname()[:2]
        self.name = socket.getfqdn(host)
        self.port = port
        self.context = context
        self.threaded = threaded
        self.scheme = ('http' if context is None else 'https')
        self.url = build_url(self.scheme, bind_address, port)
        self.environ = self.build_base_environ()

    def build_base_environ(self):
        return {
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
        }

    def build_connection_environ(self, conn, address):
        environ = {
            'REMOTE_ADDR': address[0],
            'REMOTE_PORT': str(address[1]),
        }
        if hasattr(conn, 'getpeercert'):
            d = conn.getpeercert()
        return environ

    def serve_forever(self):
        self.socket.listen(5)
        while True:
            (conn, address) = self.socket.accept()
            log.info('connection from %r', address[:2])
            if self.threaded:
                start_thread(self.handle_connection, conn, address)
            else:
                self.handle_connection(conn, address)

    def handle_connection(self, conn, address):
        try:
            if self.context is not None:
                conn = self.context.wrap_socket(conn, server_side=True)
            self.handle_requests(conn, address)
        except Exception:
            log.exception('Error handling %r', address[:2])
        finally:
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()

    def handle_requests(self, conn, address):
        environ = self.environ.copy()
        environ.update(
            self.build_connection_environ(conn, address)
        )
        handler = Handler(self.app, environ, conn)
        handler.run()

