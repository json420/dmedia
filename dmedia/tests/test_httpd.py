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
Unit tests for `dmedia.httpd`.
"""

from unittest import TestCase
from random import SystemRandom
from wsgiref.simple_server import demo_app
import ssl
import socket
import os

from microfiber import random_id

from .base import TempDir
from dmedia import __version__
from dmedia.httpd import WSGIError
from dmedia import httpd


random = SystemRandom()


def random_port():
    return random.randint(1001, 50000)


class TestWSGIError(TestCase):
    def test_init(self):
        msg = '701 Foo Bar'
        e = httpd.WSGIError(msg)
        self.assertIs(e.status, msg)
        self.assertEqual(str(e), '701 Foo Bar')


class TestFunctions(TestCase):
    def test_parse_request(self):
        self.assertEqual(
            httpd.parse_request(b'GET / HTTP/1.1\r\n'),
            ['GET', '/', 'HTTP/1.1']
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_request(b'GET / HTTP/1.1\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Line Missing CRLF'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_request(b'GET /\r\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Line'
        )
        # For now, very strict on whitespace:
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_request(b'GET /  HTTP/1.1\r\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Line'
        )

    def test_parse_header(self):
        self.assertEqual(
            httpd.parse_header(b'Content-Type: application/json\r\n'),
            ('CONTENT_TYPE', 'application/json')
        )
        self.assertEqual(
            httpd.parse_header(b'Content-Length: 1819\r\n'),
            ('CONTENT_LENGTH', '1819')
        )
        self.assertEqual(
            httpd.parse_header(b'Foo-Bar: baz\r\n'),
            ('HTTP_FOO_BAR', 'baz')
        )
        self.assertEqual(
            httpd.parse_header(b'content-type: application/json\r\n'),
            ('CONTENT_TYPE', 'application/json')
        )
        self.assertEqual(
            httpd.parse_header(b'content-length: 1819\r\n'),
            ('CONTENT_LENGTH', '1819')
        )
        self.assertEqual(
            httpd.parse_header(b'foo-bar: baz\r\n'),
            ('HTTP_FOO_BAR', 'baz')
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_header(b'Content-Type: application/json\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Header Line Missing CRLF'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_header(b'Content-Type application/json\r\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Header Line'
        )
        # For now, very strict on whitespace:
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_header(b'Content-Type:application/json\r\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Header Line'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.parse_header(b'Content_Type: application/json\r\n')
        self.assertEqual(
            cm.exception.status,
            '400 Bad Header Name'
        )

    def test_request_content_length(self):
        self.assertIsNone(
            httpd.request_content_length({})
        )
        self.assertIsNone(
            httpd.request_content_length({'CONTENT_LENGTH': None})
        )
        self.assertEqual(
            httpd.request_content_length({'CONTENT_LENGTH': '0'}), 0
        )
        self.assertEqual(
            httpd.request_content_length({'CONTENT_LENGTH': '23'}), 23
        )
        self.assertEqual(
            httpd.request_content_length({'CONTENT_LENGTH': ' 0 '}), 0
        )
        self.assertEqual(
            httpd.request_content_length({'CONTENT_LENGTH': ' 18 '}), 18
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.request_content_length({'CONTENT_LENGTH': ''})
        self.assertEqual(
            cm.exception.status,
            '400 Bad Content Length'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.request_content_length({'CONTENT_LENGTH': 'foo'})
        self.assertEqual(
            cm.exception.status,
            '400 Bad Content Length'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.request_content_length({'CONTENT_LENGTH': '18.0'})
        self.assertEqual(
            cm.exception.status,
            '400 Bad Content Length'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.request_content_length({'CONTENT_LENGTH': '-1'})
        self.assertEqual(
            cm.exception.status,
            '400 Negative Content Length'
        )
        with self.assertRaises(WSGIError) as cm:
            httpd.request_content_length({'CONTENT_LENGTH': '-18'})
        self.assertEqual(
            cm.exception.status,
            '400 Negative Content Length'
        )

    def test_response_content_length(self):
        self.assertIsNone(httpd.response_content_length([]))
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('Content-Type', 'application/json'),
        ]
        self.assertIsNone(httpd.response_content_length(headers))
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('Content-Type', 'application/json'),
            ('Content-Length', '1819'),
        ]
        self.assertEqual(httpd.response_content_length(headers), 1819)
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('contenT-lengtH', '1725'),
            ('Content-Type', 'application/json'),
        ]
        self.assertEqual(httpd.response_content_length(headers), 1725)
        headers = [
            ('content-length', '2136'),
        ]
        self.assertEqual(httpd.response_content_length(headers), 2136)

    def test_iter_response_lines(self):
        self.assertEqual(
            list(httpd.iter_response_lines('414 Request-URI Too Long', [])),
            [
                'HTTP/1.1 414 Request-URI Too Long\r\n',
                '\r\n',   
            ]
        )
        headers = [
            ('Content-Type', 'application/json'),
            ('Content-Length', '784'),
        ]
        self.assertEqual(
            list(httpd.iter_response_lines('200 OK', headers)),
            [
                'HTTP/1.1 200 OK\r\n',
                'Content-Type: application/json\r\n',
                'Content-Length: 784\r\n',
                '\r\n',   
            ]
        )


class TestInput(TestCase):
    def test_init(self):
        tmp = TempDir()
        filename = tmp.write(b'hello', 'rfile.1')
        fp = open(filename, 'rb')

        environ = {'CONTENT_LENGTH': '5', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertIs(inst._rfile, fp)
        self.assertEqual(fp.tell(), 0)
        self.assertEqual(inst._avail, 5)
        self.assertEqual(inst._method, 'PUT')

        environ = {'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertIs(inst._rfile, fp)
        self.assertEqual(fp.tell(), 0)
        self.assertIsNone(inst._avail)
        self.assertEqual(inst._method, 'PUT')

        environ = {'CONTENT_LENGTH': None, 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertIs(inst._rfile, fp)
        self.assertEqual(fp.tell(), 0)
        self.assertIsNone(inst._avail)
        self.assertEqual(inst._method, 'PUT')

        # Test with bad CONTENT_LENGTH
        environ = {'CONTENT_LENGTH': '5.0', 'REQUEST_METHOD': 'PUT'}
        with self.assertRaises(WSGIError) as cm:
            httpd.Input(fp, environ)
        self.assertEqual(
            cm.exception.status,
            '400 Bad Content Length'
        )
        self.assertEqual(fp.tell(), 0)

        environ = {'CONTENT_LENGTH': '-5', 'REQUEST_METHOD': 'PUT'}
        with self.assertRaises(WSGIError) as cm:
            httpd.Input(fp, environ)
        self.assertEqual(
            cm.exception.status,
            '400 Negative Content Length'
        )
        self.assertEqual(fp.tell(), 0)

        # Test with missing REQUEST_METHOD
        environ = {'CONTENT_LENGTH': '5'}
        with self.assertRaises(KeyError) as cm:
            httpd.Input(fp, environ)
        self.assertEqual(
            str(cm.exception),
            "'REQUEST_METHOD'"
        )
        self.assertEqual(fp.tell(), 0)

    def test_read(self):
        tmp = TempDir()
        filename = tmp.write(b'hello', 'rfile.1')
        fp = open(filename, 'rb')

        # Test when method isn't PUT or POST
        for method in ('GET', 'HEAD', 'DELETE', 'put', 'post'):
            environ = {'CONTENT_LENGTH': '5', 'REQUEST_METHOD': method}
            inst = httpd.Input(fp, environ)
            with self.assertRaises(WSGIError) as cm:
                inst.read()
            self.assertEqual(
                cm.exception.status,
                '500 Internal Server Error'
            )
            self.assertEqual(fp.tell(), 0)

        # Test when there is no CONTENT_LENGTH:
        for method in ('PUT', 'POST'):
            environ = {'REQUEST_METHOD': method}
            inst = httpd.Input(fp, environ)
            with self.assertRaises(WSGIError) as cm:
                inst.read()
            self.assertEqual(
                cm.exception.status,
                '411 Length Required'
            )
            self.assertEqual(fp.tell(), 0)

        # Test when it's all good
        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '5', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertEqual(inst.read(), b'hello')
        self.assertEqual(fp.tell(), 5)
        self.assertEqual(inst.read(), b'')
        self.assertEqual(fp.tell(), 5)

        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '5', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertEqual(inst.read(5), b'hello')
        self.assertEqual(fp.tell(), 5)
        self.assertEqual(inst.read(5), b'')
        self.assertEqual(fp.tell(), 5)

        # Test that it wont read past the CONTENT_LENGTH:
        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '4', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertEqual(inst.read(), b'hell')
        self.assertEqual(fp.tell(), 4)
        self.assertEqual(inst.read(500), b'')
        self.assertEqual(fp.tell(), 4)

        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '4', 'REQUEST_METHOD': 'POST'}
        inst = httpd.Input(fp, environ)
        self.assertEqual(inst.read(3), b'hel')
        self.assertEqual(fp.tell(), 3)
        self.assertEqual(inst.read(500), b'l')
        self.assertEqual(fp.tell(), 4)
        self.assertEqual(inst.read(), b'')
        self.assertEqual(fp.tell(), 4)

        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '0', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        self.assertEqual(inst.read(500), b'')
        self.assertEqual(fp.tell(), 0)
        self.assertEqual(inst.read(), b'')
        self.assertEqual(fp.tell(), 0)

        # Test when CONTENT_LENGTH is longer than content
        fp = open(filename, 'rb')
        environ = {'CONTENT_LENGTH': '6', 'REQUEST_METHOD': 'PUT'}
        inst = httpd.Input(fp, environ)
        with self.assertRaises(AssertionError) as cm:
            inst.read()


class TestHandler(TestCase):
    def test_parse_request(self):
        class Subclass(httpd.Handler):
            def __init__(self):
                pass

        tmp = TempDir()
        filename = tmp.join('rfile')
        inst = Subclass()

        fp = open(filename, 'wb')
        fp.write(b'A' * 5000)
        fp.write(b'\r\n') 
        fp.close()
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '414 Request-URI Too Long'
        )
        self.assertEqual(fp.tell(), 4097)

        requestline = b'GET /some/stuff HTTP/1.1\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Junk\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Line Missing CRLF'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET /some /things HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Junk\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Line'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET /stuff HTTP/1.0\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Junk\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '505 HTTP Version Not Supported'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'COPY /stuff HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Junk\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '405 Method Not Allowed'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET /stuff?junk=true?more=true HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Foo\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request URI'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET stuff/junk HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Foo\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Path'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET /stuff/../private HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Foo\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Path Naughty'
        )
        self.assertEqual(fp.tell(), len(requestline))

        requestline = b'GET /stuff?sneaky=.. HTTP/1.1\r\n'
        open(filename, 'wb').write(
            requestline + b'User-Agent: Foo\r\n\r\n'
        )
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '400 Bad Request Path Naughty'
        )
        self.assertEqual(fp.tell(), len(requestline))

        # Test will all allowed methods
        rest =  ' /stuff?junk=true HTTP/1.1\r\nUser-Agent: Foo\r\n\r\n'
        for method in ('POST', 'PUT', 'GET', 'HEAD', 'DELETE'):
            preamble = (method + rest).encode('latin_1')
            open(filename, 'wb').write(preamble + os.urandom(50))
            fp = open(filename, 'rb')
            inst.rfile = fp
            environ = inst.parse_request()
            wsgi_input = environ['wsgi.input']
            self.assertIsInstance(wsgi_input, httpd.Input)
            self.assertIs(wsgi_input._rfile, fp)
            self.assertEqual(environ,
                {
                    'REQUEST_METHOD': method,
                    'PATH_INFO': '/stuff',
                    'QUERY_STRING': 'junk=true',
                    'HTTP_USER_AGENT': 'Foo',
                    'wsgi.input': wsgi_input,
                }
            )
            self.assertEqual(fp.tell(), len(preamble))

            # Make sure it's case-sensitive
            preamble = (method.lower() + rest).encode('latin_1')
            open(filename, 'wb').write(preamble + os.urandom(50))
            fp = open(filename, 'rb')
            inst.rfile = fp
            with self.assertRaises(WSGIError) as cm:
                inst.parse_request()
            self.assertEqual(
                cm.exception.status,
                '405 Method Not Allowed'
            )

        # Now test header parsing
        requestline = b'GET /foo?bar=baz HTTP/1.1\r\n'
        fp = open(filename, 'wb')
        fp.write(requestline)
        fp.write(b'H' * 5000)
        fp.close()
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '431 Request Header Line Too Long'
        )
        self.assertEqual(fp.tell(), len(requestline) + 4097)

        letters = 'abcdefghijk'
        assert len(set(letters)) == 11
        headers = ''.join(
            '{}: {}\r\n'.format(l, l) for l in letters
        )
        preamble = requestline + headers.encode('latin_1') + b'\r\n'
        open(filename, 'wb').write(preamble)
        fp = open(filename, 'rb')
        inst.rfile = fp
        with self.assertRaises(WSGIError) as cm:
            inst.parse_request()
        self.assertEqual(
            cm.exception.status,
            '431 Too Many Request Headers'
        )
        self.assertEqual(fp.tell(), len(preamble) - 2)

        letters = 'abcdefghij'
        assert len(set(letters)) == 10
        headers = ''.join(
            '{}: {}\r\n'.format(l, l) for l in letters
        )
        preamble = requestline + headers.encode('latin_1') + b'\r\n'
        open(filename, 'wb').write(preamble + os.urandom(100))
        fp = open(filename, 'rb')
        inst.rfile = fp
        environ = inst.parse_request()
        wsgi_input = environ['wsgi.input']
        self.assertIsInstance(wsgi_input, httpd.Input)
        self.assertIs(wsgi_input._rfile, fp)
        self.assertEqual(environ,
            {
                'REQUEST_METHOD': 'GET',
                'PATH_INFO': '/foo',
                'QUERY_STRING': 'bar=baz',
                'wsgi.input': wsgi_input,
                'HTTP_A': 'a',
                'HTTP_B': 'b',
                'HTTP_C': 'c',
                'HTTP_D': 'd',
                'HTTP_E': 'e',
                'HTTP_F': 'f',
                'HTTP_G': 'g',
                'HTTP_H': 'h',
                'HTTP_I': 'i',
                'HTTP_J': 'j',
            }
        )
        self.assertEqual(fp.tell(), len(preamble))

        # Test a good request and its environ['wsgi.input']
        lines = [
            b'PUT /some/thing HTTP/1.1\r\n', 
            b'User-Agent: FooBar/18\r\n',
            b'Content-Length: 120\r\n',
            b'Content-Type: application/json\r\n',
            b'\r\n',
        ]
        preamble = b''.join(lines)
        chunk1 = os.urandom(69)
        chunk2 = os.urandom(51)
        extra = os.urandom(21)

        fp = open(filename, 'wb')
        fp.write(preamble)
        fp.write(chunk1)
        fp.write(chunk2)
        fp.write(extra)
        fp.close()
        fp = open(filename, 'rb')
        inst.rfile = fp
        environ = inst.parse_request()
        wsgi_input = environ['wsgi.input']
        self.assertIsInstance(wsgi_input, httpd.Input)
        self.assertIs(wsgi_input._rfile, fp)
        self.assertEqual(wsgi_input._avail, 120)
        self.assertEqual(wsgi_input._method, 'PUT')
        self.assertEqual(environ,
            {
                'REQUEST_METHOD': 'PUT',
                'PATH_INFO': '/some/thing',
                'QUERY_STRING': '',
                'wsgi.input': wsgi_input,
                'CONTENT_TYPE': 'application/json',
                'CONTENT_LENGTH': '120',
                'HTTP_USER_AGENT': 'FooBar/18',
            }
        )
        self.assertEqual(wsgi_input.read(69), chunk1)
        self.assertEqual(wsgi_input._avail, 51)
        self.assertEqual(wsgi_input.read(51), chunk2)
        self.assertEqual(wsgi_input._avail, 0)
        self.assertEqual(wsgi_input.read(21), b'')
        self.assertEqual(wsgi_input.read(), b'')


class TestHTTPServer(TestCase):
    def test_init(self):
        class App:
            pass

        # App not callable
        app = App()
        with self.assertRaises(TypeError) as cm:
            httpd.HTTPServer(app, '::1')
        self.assertEqual(
            str(cm.exception),
            'app not callable: {!r}'.format(app)
        )

        # Bad bind_address
        with self.assertRaises(ValueError) as cm:
            httpd.HTTPServer(demo_app, '127.0.0.1')
        self.assertEqual(
            str(cm.exception),
            "invalid bind_address: '127.0.0.1'"
        )

        # context not a ssl.SSLContext
        with self.assertRaises(TypeError) as cm:
            httpd.HTTPServer(demo_app, '::1', 17)
        self.assertEqual(
            str(cm.exception),
            httpd.TYPE_ERROR.format('context', ssl.SSLContext, int, 17)
        )

        server = httpd.HTTPServer(demo_app)
        self.assertIs(server.app, demo_app)
        self.assertIsInstance(server.socket, socket.socket)
        self.assertEqual(server.name, socket.getfqdn('::1'))
        self.assertIsInstance(server.port, int)
        self.assertEqual(server.socket.getsockname(),
            ('::1', server.port, 0, 0)
        )
        self.assertIsNone(server.context)
        self.assertIs(server.threaded, False)
        self.assertEqual(server.scheme, 'http')
        self.assertEqual(server.url, 'http://[::1]:{}/'.format(server.port))
        self.assertEqual(server.environ, server.build_base_environ())

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        server = httpd.HTTPServer(demo_app, '::', ctx, True)
        self.assertIs(server.app, demo_app)
        self.assertIsInstance(server.socket, socket.socket)
        self.assertEqual(server.name, socket.getfqdn('::'))
        self.assertIsInstance(server.port, int)
        self.assertEqual(server.socket.getsockname(),
            ('::', server.port, 0, 0)
        )
        self.assertIs(server.context, ctx)
        self.assertIs(server.threaded, True)
        self.assertEqual(server.scheme, 'https')
        self.assertEqual(server.url, 
            'https://[::1]:{}/'.format(server.port)
        )
        self.assertEqual(server.environ, server.build_base_environ())

    def test_build_base_environ(self):
        class Subclass(httpd.HTTPServer):
            def __init__(self):
                self.scheme = random_id()
                self.threaded = random_id()
                self.name = random_id()
                self.port = random_port()
                self.context = 'foo'

        server = Subclass()
        self.assertEqual(
            server.build_base_environ(),
            {
                'SERVER_PROTOCOL': 'HTTP/1.1',
                'SERVER_SOFTWARE': httpd.SERVER_SOFTWARE,
                'SERVER_NAME': server.name,
                'SERVER_PORT': str(server.port),
                'SCRIPT_NAME': '',
                'wsgi.version': '(1, 0)',
                'wsgi.url_scheme': server.scheme,
                'wsgi.multithread': server.threaded,
                'wsgi.multiprocess': False,
                'wsgi.run_once': False,
                'SSL_PROTOCOL': 'TLSv1',
            }
        )

        server = httpd.HTTPServer(demo_app, '::1')
        self.assertEqual(
            server.build_base_environ(),
            {
                'SERVER_PROTOCOL': 'HTTP/1.1',
                'SERVER_SOFTWARE': httpd.SERVER_SOFTWARE,
                'SERVER_NAME': socket.getfqdn('::1'),
                'SERVER_PORT': str(server.port),
                'SCRIPT_NAME': '',
                'wsgi.version': '(1, 0)',
                'wsgi.url_scheme': 'http',
                'wsgi.multithread': False,
                'wsgi.multiprocess': False,
                'wsgi.run_once': False,
            }
        )
        

