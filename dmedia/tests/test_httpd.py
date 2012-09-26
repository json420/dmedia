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
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET / HTTP/1.1\n')
        self.assertEqual(
            str(cm.exception),
            'Does not end with CRLF'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET / HTTP/1.1\r\n\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other CR'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET /\r HTTP/1.1\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other CR'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET /\n HTTP/1.1\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other LF'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET /\r\n')
        self.assertEqual(
            str(cm.exception),
            'Does not have exactly 3 parts'
        )
        # For now, very strict on whitespace:
        with self.assertRaises(ValueError) as cm:
            httpd.parse_request(b'GET /  HTTP/1.1\r\n')
        self.assertEqual(
            str(cm.exception),
            'Does not have exactly 3 parts'
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
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type: application/json\n')
        self.assertEqual(
            str(cm.exception),
            'Does not end with CRLF'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type: application/json\r\n\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other CR'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type: applicat\rion/json\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other CR'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type: applicat\nion/json\r\n')
        self.assertEqual(
            str(cm.exception),
            'Line contains other LF'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type application/json\r\n')
        self.assertEqual(
            str(cm.exception),
            'Does not have exactly 2 parts'
        )
        # For now, very strict on whitespace:
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content-Type:application/json\r\n')
        self.assertEqual(
            str(cm.exception),
            'Does not have exactly 2 parts'
        )
        with self.assertRaises(ValueError) as cm:
            httpd.parse_header(b'Content_Type: application/json\r\n')
        self.assertEqual(
            str(cm.exception),
            "Unexpected '_' in header name"
        )

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
        
        


class TestServer(TestCase):
    def test_init(self):
        class App:
            pass

        # App not callable
        app = App()
        with self.assertRaises(TypeError) as cm:
            httpd.Server(app, '::1')
        self.assertEqual(
            str(cm.exception),
            'app not callable: {!r}'.format(app)
        )

        # context not a ssl.SSLContext
        with self.assertRaises(TypeError) as cm:
            httpd.Server(demo_app, '::1', 17)
        self.assertEqual(
            str(cm.exception),
            httpd.TYPE_ERROR.format('context', ssl.SSLContext, int, 17)
        )

        server = httpd.Server(demo_app)
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
        server = httpd.Server(demo_app, '127.0.0.1', ctx, True)
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
        class Subclass(httpd.Server):
            def __init__(self):
                self.scheme = random_id()
                self.threaded = random_id()
                self.software = random_id()
                self.name = random_id()
                self.port = random_port()

        server = Subclass()
        self.assertEqual(
            server.build_base_environ(),
            {
                'SERVER_PROTOCOL': 'HTTP/1.1',
                'SERVER_SOFTWARE': server.software,
                'SERVER_NAME': server.name,
                'SERVER_PORT': str(server.port),
                'SCRIPT_NAME': '',
                'wsgi.version': '(1, 0)',
                'wsgi.url_scheme': server.scheme,
                'wsgi.multithread': server.threaded,
                'wsgi.multiprocess': False,
                'wsgi.run_once': False,
            }
        )

        server = httpd.Server(demo_app, '::1')
        self.assertEqual(
            server.build_base_environ(),
            {
                'SERVER_PROTOCOL': 'HTTP/1.1',
                'SERVER_SOFTWARE': ('Dmedia/' + __version__),
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
        

