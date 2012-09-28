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

from microfiber import random_id

from dmedia import __version__
from dmedia.httpd import WSGIError
from dmedia import httpd


random = SystemRandom()


def random_port():
    return random.randint(1001, 50000)


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

    def test_get_content_length(self):
        self.assertIsNone(httpd.get_content_length([]))
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('Content-Type', 'application/json'),
        ]
        self.assertIsNone(httpd.get_content_length(headers))
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('Content-Type', 'application/json'),
            ('Content-Length', '1819'),
        ]
        self.assertEqual(httpd.get_content_length(headers), 1819)
        headers = [
            ('Server', 'Dmedia/12.09.0 (Ubuntu 12.10; x86_64)'),
            ('contenT-lengtH', '1725'),
            ('Content-Type', 'application/json'),
        ]
        self.assertEqual(httpd.get_content_length(headers), 1725)
        headers = [
            ('content-length', '2136'),
        ]
        self.assertEqual(httpd.get_content_length(headers), 2136)

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
        server = httpd.HTTPServer(demo_app, '127.0.0.1', ctx, True)
        self.assertIs(server.app, demo_app)
        self.assertIsInstance(server.socket, socket.socket)
        self.assertEqual(server.name, socket.getfqdn('127.0.0.1'))
        self.assertIsInstance(server.port, int)
        self.assertEqual(server.socket.getsockname(),
            ('127.0.0.1', server.port)
        )
        self.assertIs(server.context, ctx)
        self.assertIs(server.threaded, True)
        self.assertEqual(server.scheme, 'https')
        self.assertEqual(server.url, 
            'https://127.0.0.1:{}/'.format(server.port)
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
        

