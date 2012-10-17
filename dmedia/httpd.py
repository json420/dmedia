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
A tiny WSGI HTTP 1.1 server with SSL support.

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

    * Does not support multiple occurrences of the same header

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
import json
from hashlib import md5
import logging

from dmedia import __version__

# Monkey patch python3.2 to add ssl.OP_NO_COMPRESSION available in python3.3:
if not hasattr(ssl, 'OP_NO_COMPRESSION'):
    ssl.OP_NO_COMPRESSION = 131072    


SERVER_SOFTWARE = 'Dmedia/{} ({} {}; {})'.format(__version__, 
    platform.dist()[0], platform.dist()[1], platform.machine()
)
MAX_LINE = 4 * 1024
MAX_HEADER_COUNT = 10
SOCKET_TIMEOUT = 30
log = logging.getLogger()


class WSGIError(Exception):
    """
    Raised to shortcut the request handling when a request is "suspicious".

    The `Handler` class is only designed to support extremely well-behaved
    requests.  If at any point in the request handling a `WSGIError` is raised,
    the request handling is immediately aborted.  The response will contain only
    the status line, and no headers, no response body.

    Importantly, the TCP connection is closed after a `WSGIError` is raised.
    Otherwise the `Handler.rfile` and `Handler.wfile` could be in a badly
    defined state that would possibly allow the attack to be escalated on
    subsequent requests.

    So when a `WSGIError` is raised, we always make an attacker start over from
    ground zero, including having to go through the SSL handshake again.

    This should not be raised for expected HTTP error conditions like a
    "404 Not Found" returned from CouchDB.  Is this case, we don't want the
    connection closed (for performance reasons).
    """
    def __init__(self, status):
        self.status = status
        super().__init__(status)


def build_server_ssl_context(config):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ctx.options |= ssl.OP_NO_COMPRESSION  # Protect against CRIME-like attacks
    # ctx.set_ciphers('RC4')  # Example of how to change ciphers
    ctx.load_cert_chain(config['cert_file'], config['key_file'])
    if 'ca_file' in config or 'ca_path' in config:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(
            cafile=config.get('ca_file'),
            capath=config.get('ca_path'),
        )
    return ctx


def parse_request(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Bad Request Line Missing CRLF')
    line = line_bytes[:-2].decode('latin_1')
    # io.BufferedReader.readline() only matches b'\n':
    if '\r' in line:
        raise WSGIError('400 Bad Request Line Internal CR')
    parts = line.split(' ')
    if len(parts) != 3:
        raise WSGIError('400 Bad Request Line')
    return parts


def parse_header(line_bytes):
    if not line_bytes.endswith(b'\r\n'):
        raise WSGIError('400 Bad Header Line Missing CRLF')
    line = line_bytes[:-2].decode('latin_1')
    # io.BufferedReader.readline() only matches b'\n':
    if '\r' in line:
        raise WSGIError('400 Bad Header Line Internal CR')
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
    for (name, value) in headers:
        yield '{}: {}\r\n'.format(name, value)
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
        self._closed = True


class Handler:
    """
    Handles one or more HTTP requests.

    A `Handler` instance is created per TCP connection.
    """

    __slots__ = ('app', 'environ', 'conn', 'rfile', 'wfile', 'remote', 'start')

    def __init__(self, app, environ, conn):
        self.app = app
        self.environ = environ
        self.conn = conn
        self.rfile = conn.makefile('rb')
        self.wfile = conn.makefile('wb')
        self.remote = '{REMOTE_ADDR} {REMOTE_PORT}'.format(**environ)
        self.start = None

    def handle(self):
        if self.environ['wsgi.multithread']:
            self.handle_many()
        else:
            self.handle_one()

    def handle_many(self):
        count = 0
        try:
            while self.handle_one():
                count += 1
        finally:
            log.info('%s\tHandled %r Requests', self.remote, count)

    def handle_one(self):
        self.start = None
        environ = self.environ.copy()
        try:
            environ.update(self.build_request_environ())
            result = self.app(environ, self.start_response)
        except WSGIError as e:
            if e.status:
                log.warning('%s\t%s', self.remote, e.status)
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
        if request_line == b'':
            raise WSGIError('')
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
            if name in environ:
                raise WSGIError('400 Bad Request Duplicate Header')
            environ[name] = value

        # Setup wsgi.input
        environ['wsgi.input'] = Input(self.rfile, environ)

        return environ

    def start_response(self, status, response_headers, exc_info=None):
        self.start = (status, response_headers)

    def send_response(self, environ, result):
        (status, response_headers) = self.start
        headers = dict(
            (name.lower(), value) for (name, value) in response_headers
        )
        headers.setdefault('server', SERVER_SOFTWARE)
        response_headers = list(
            (name, headers[name]) for name in sorted(headers)
        )
        preamble = ''.join(iter_response_lines(status, response_headers))
        self.wfile.write(preamble.encode('latin_1'))
        if result != []:
            content_length = response_content_length(response_headers)
            total = 0
            for buf in result:
                total += len(buf)
                assert total <= content_length
                self.wfile.write(buf)
            assert total == content_length
        self.wfile.flush()

    def send_status_only(self, status):
        preamble = ''.join(iter_response_lines(status, []))
        self.wfile.write(preamble.encode('latin_1'))
        self.wfile.flush()


class HTTPD:
    def __init__(self, app, bind_address='::1', context=None):
        if not callable(app):
            raise TypeError('app not callable: {!r}'.format(app))
        if bind_address not in ('::1', '::', '127.0.0.1', '0.0.0.0'):
            raise ValueError('invalid bind_address: {!r}'.format(bind_address))
        if context is not None:
            if not isinstance(context, ssl.SSLContext):
                raise TypeError(
                    'context must be a ssl.SSLContext; got {!r}'.format(context)
                )
            if context.protocol != ssl.PROTOCOL_TLSv1:
                raise Exception('context.protocol must be ssl.PROTOCOL_TLSv1')
            if not (context.options & ssl.OP_NO_COMPRESSION):
                raise Exception(
                    'context.options must have ssl.OP_NO_COMPRESSION'
                )
        # Safety against accidental misconfiguration:
        if bind_address in ('::', '0.0.0.0') and context is None:
            raise Exception('wont accept outside connections without SSL')
        self.app = app
        self.bind_address = bind_address
        self.context = context
        if bind_address in ('::1', '::'):
            template = '{}://[::1]:{}/'
            self.socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            template = '{}://127.0.0.1:{}/'
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((bind_address, 0))
        self.port = self.socket.getsockname()[1]
        self.scheme = ('http' if context is None else 'https')
        self.url = template.format(self.scheme, self.port)
        self.environ = self.build_base_environ()
        self.socket.listen(5)
        self.thread = None
        self.running = False

    def __del__(self):
        if self.running:
            self.shutdown()

    def build_base_environ(self):
        """
        Builds the base *environ* used throughout instance lifetime.
        """
        environ = {
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'SERVER_SOFTWARE': SERVER_SOFTWARE,
            'SERVER_NAME': self.bind_address,
            'SERVER_PORT': str(self.port),
            'SCRIPT_NAME': '',
            'wsgi.version': '(1, 0)',
            'wsgi.url_scheme': self.scheme,
            'wsgi.multithread': True,
            'wsgi.multiprocess': False,
            'wsgi.run_once': False,
            'wsgi.file_wrapper': FileWrapper
        }
        if self.context is not None:
            environ.update({
                'SSL_PROTOCOL': 'TLSv1',
                'SSL_COMPRESS_METHOD': 'NULL',
            })
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
        while True:
            (conn, address) = self.socket.accept()
            conn.settimeout(SOCKET_TIMEOUT)
            thread = threading.Thread(
                target=self.handle_connection,
                args=(conn, address),
            )
            thread.daemon = True
            thread.start()

    def start(self):
        assert self.thread is None
        assert self.running is False
        self.running = True
        self.thread = threading.Thread(
            target=self.serve_single_threaded,
        )
        self.thread.daemon = True
        self.thread.start()

    def shutdown(self):
        assert self.running is True
        self.running = False
        self.thread.join()
        self.thread = None

    def reconfigure(self, app, ssl_config):
        assert set(ssl_config) == set(['cert_file', 'key_file', 'ca_file'])
        self.shutdown()
        self.app = app
        self.context = build_server_ssl_context(ssl_config)
        self.start()

    def serve_single_threaded(self):
        self.environ['wsgi.multithread'] = False
        self.socket.settimeout(0.25)
        while self.running:
            try:
                (conn, address) = self.socket.accept()
                conn.settimeout(0.50)
                self.handle_connection(conn, address)
            except socket.timeout:
                pass

    def handle_connection(self, conn, address):
        #conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        try:
            if self.context is not None:
                conn = self.context.wrap_socket(conn, server_side=True)
            self.handle_requests(conn, address)
            conn.shutdown(socket.SHUT_RDWR)
        except socket.error:
            log.info('%s\tSocket Timeout/Error', address)
        except Exception:
            log.exception('%s\tUnhandled Exception', address)
        finally:
            conn.close()

    def handle_requests(self, conn, address):
        environ = self.environ.copy()
        environ.update(self.build_connection_environ(conn, address))
        handler = Handler(self.app, environ, conn)
        handler.handle()


############################
# Some misc helper functions

def echo_app(environ, start_response):
    def get_value(value):
        if value is FileWrapper:
            return 'httpd.FileWrapper'
        if isinstance(value, (str, int, float, bool)):
            return value
        return repr(value)

    obj = dict(
        (key, get_value(value))
        for (key, value) in environ.items()
    )
    if environ['wsgi.input']._avail:
        obj['echo.content_md5'] = md5(environ['wsgi.input'].read()).hexdigest()
    output = json.dumps(obj).encode('utf-8')

    status = '200 OK'
    response_headers = [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(output))),
    ]
    start_response(status, response_headers)
    return [output]


def make_server(app, bind_address='::1', ssl_config=None):
    if ssl_config is None:
        context = None
    else:
        context = build_server_ssl_context(ssl_config)
    return HTTPD(app, bind_address, context)


def run_server(queue, app, bind_address='::1', ssl_config=None):
    try:
        server = make_server(app, bind_address, ssl_config)
        env = {'port': server.port, 'url': server.url}
        queue.put(env)
        server.serve_forever()
    except Exception as e:
        queue.put(e)

