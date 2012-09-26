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
from urllib.parse import unquote_to_bytes
import logging

from microfiber import dumps
from usercouch import bind_socket, build_url
from dmedia import __version__


SERVER_SOFTWARE = 'Dmedia/{} ({} {}; {})'.format(__version__, 
    platform.dist()[0], platform.dist()[1], platform.machine()
)
MAX_LINE = 8 * 1024
MAX_HEADER_COUNT = 10
TYPE_ERROR = '{}: need a {!r}; got a {!r}: {!r}'
log = logging.getLogger()


class WSGIError(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__(status)


def start_thread(target, *args):
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


def bind_socket(bind_address):
    if bind_address in ('127.0.0.1', '0.0.0.0'):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    elif bind_address in ('::1', '::'):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    else:
        raise ValueError('invalid bind_address: {!r}'.format(bind_address))
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


def parse_request(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Does not end with CRLF')
    line = line_bytes[:-2].decode('latin_1')
    if '\r' in line:
        raise WSGIError('400 Line contains other CR')
    if '\n' in line:
        raise WSGIError('400 Line contains other LF')
    parts = line.split(' ')
    if len(parts) != 3:
        raise WSGIError('400 Does not have exactly three parts')
    return parts


def parse_header(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Does not end with CRLF')
    line = line_bytes[:-2].decode('latin_1')
    if '\r' in line:
        raise WSGIError('400 Line contains other CR')
    if '\n' in line:
        raise WSGIError('400 Line contains other LF')
    parts = line.split(': ', 1)
    if len(parts) != 2:
        raise WSGIError('400 Does not have exactly two parts')
    (name, value) = parts
    if '_' in name:
        raise WSGIError('400 Underscore in header name')
    key = name.replace('-', '_').upper()
    if key in ('CONTENT_TYPE', 'CONTENT_LENGTH'):
        return (key, value)
    return ('HTTP_' + key, value)


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


def get_content_length(response_headers):
    for (key, value) in response_headers:
        if key.lower() == 'content-length':
            return int(value)


def iter_response_lines(status, headers):
    yield 'HTTP/1.1 {}\r\n'.format(status)
    for (key, value) in headers:
        yield '{}: {}\r\n'.format(key, value)
    yield '\r\n'


class wsgi_input:
    __slots__ = ('_rfile', '_avail')

    def __init__(self, rfile, environ):
        self._rfile = rfile
        self._avail = request_content_length(environ)

    def read(self, size=None):
        if self._avail is None:
            raise WSGIError('411 Length Required')
        if self._avail == 0:
            return b''
        assert size is None or size > 0
        size = (self._avail if size is None else min(self._avail, size))
        self._avail -= size
        buf = self._rfile.read(size)
        assert len(buf) == size
        return buf


class Handler:
    def __init__(self, app, environ, conn, address):
        self.app = app
        self.environ = environ
        self.conn = conn
        self.address = address
        self.rfile = conn.makefile('rb')
        self.wfile = conn.makefile('wb')

    def handle_many(self):
        while self.handle_one():
            pass

    def handle_one(self):
        self.start = None
        environ = self.environ.copy()
        try:
            self.parse_request(environ)
            result = self.app(environ, self.start_response)
            self.send_response(environ, result)
            return True
        except socket.error:
            return False
        except WSGIError as e:
            self.start_response(e.status, [])
            self.send_response(environ, [])
            return True

    def parse_request(self, environ):
        # Parse the request line
        request_line = self.rfile.readline(MAX_LINE + 1)
        if len(request_line) > MAX_LINE:
            raise WSGIError('414 Request-URI Too Long')
        (method, uri, protocol) = parse_request(request_line)
        if protocol != 'HTTP/1.1':
            raise WSGIError('400 Wrong HTTP Protocol')
        if method not in ('GET', 'HEAD', 'POST', 'PUT', 'DELETE'):
            raise WSGIError('405 Method Not Allowed')
        parts = uri.split('?')
        if len(parts) > 2:
            raise WSGIError('400 Bad Request')
        if len(parts) == 2:
            (path, query) = parts
        else:
            (path, query) = (parts[0], '')
        environ['REQUEST_METHOD'] = method
        environ['PATH_INFO'] = unquote_to_bytes(path).decode('latin_1')
        environ['QUERY_STRING'] = query

        # Parse the headers
        count = 0
        while True:
            header_line = self.rfile.readline(MAX_LINE + 1)
            if len(header_line) > MAX_LINE:
                raise WSGIError('431 Request Header Field Too Large')
            if header_line == b'\r\n':
                break
            if count > MAX_HEADER_COUNT:
                raise WSGIError('431 Too Many Request Header Fields')
            (key, value) = parse_header(header_line)
            environ[key] = value
            count += 1

        # Setup wsgi.input
        environ['wsgi.input'] = wsgi_input(self.rfile, environ)

    def start_response(self, status, response_headers, exc_info=None):
        self.start = (status, response_headers)

    def send_response(self, environ, result):
        (status, response_headers) = self.start
        content_length = get_content_length(response_headers)
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


class Server:
    def __init__(self, app, bind_address='::1', context=None, threaded=False):
        if not callable(app):
            raise TypeError('app not callable: {!r}'.format(app))
        if not (context is None or isinstance(context, ssl.SSLContext)):
            raise TypeError(TYPE_ERROR.format(
                'context', ssl.SSLContext, type(context), context)
            )
        if context is not None:
            assert context.protocol is ssl.PROTOCOL_TLSv1
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
        }
        if self.context is not None:
            environ['SSL_PROTOCOL'] = 'TLSv1'
        return environ

    def build_connection_environ(self, conn, address):
        environ = {
            'REMOTE_ADDR': address[0],
            'REMOTE_PORT': str(address[1]),
        }
        if not hasattr(conn, 'getpeercert'):
            return environ
        d = conn.getpeercert()
        if d is not None:
            subject = dict(d['subject'][0])
            if 'commonName' in subject:
                environ['SSL_CLIENT_S_DN_CN'] = subject['commonName']
            issuer = dict(d['issuer'][0])
            if 'commonName' in issuer:
                environ['SSL_CLIENT_I_DN_CN'] = issuer['commonName']
        return environ

    def serve_forever(self):
        self.socket.listen(5)
        while True:
            (conn, address) = self.socket.accept()
            #log.info('Connection from %r', address[:2])
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
        handler = Handler(self.app, environ, conn, address)
        if self.threaded:
            handler.handle_many()
        else:
            handler.handle_one()

