# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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

"""
Attempt to make JavaScript unit testing more sane.

Goals:

    1. Run JavaScript unit tests from `setup.py` without human intervention

    2. By default run tests in embedded WebKit, have option of launching tests
       in a browser

    3. Do arbitrary setUp/tearDown from Python - the most critical JavaScript in
       dmedia and Novacut will interact with CouchDB using XMLHttpRequest, so we
       need to put a CouchDB database into a known state prior to running the
       JavaScript test

    4. Do out-of-band checks from Python to verify that the JavaScript executed
       correctly - the obvious one being checking the state of a CouchDB
       database after the JavaScript tests have run

"""

from unittest import TestCase
import multiprocessing
from Queue import Empty
from wsgiref.simple_server import make_server
import json
from textwrap import dedent

from genshi.template import MarkupTemplate


class ResultsApp(object):
    """
    Simple WSGI app for collecting results from JavaScript tests.

    REST API
    ========

    To retrieve the test HTML page (will have appropriate JavaScript):

        ::

            GET / HTTP/1.1

    To test an assertion (assertEqual, assertTrue, etc):

        ::

            POST / HTTP/1.1
            Content-Type: application/json

            {"method": "assertEqual", "args": ["foo", "bar"]}

    To report an unhandled exception:

        ::

            POST /error HTTP/1.1
            Content-Type: text/plain

            Oh no, caught an unhandled JavaScript exception!

    Finally, to conclude a test:

        ::

            POST /complete HTTP/1.1
    """
    def __init__(self, q, content, mime='text/html'):
        self.q = q
        self.content = content
        self.mime = mime

    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] not in ('GET', 'POST'):
            self.q.put(('bad_method', environ['REQUEST_METHOD']))
            start_response('405 Method Not Allowed', [])
            return ''
        if environ['REQUEST_METHOD'] == 'GET' and environ['PATH_INFO'] == '/':
            self.q.put(('get', None))
            headers = [
                ('Content-Type', self.mime),
                ('Content-Length', str(len(self.content))),
            ]
            start_response('200 OK', headers)
            return self.content
        if environ['REQUEST_METHOD'] == 'POST':
            if environ['PATH_INFO'] == '/':
                content = environ['wsgi.input'].read()
                self.q.put(('test', content))
                start_response('202 Accepted', [])
                return ''
            if environ['PATH_INFO'] == '/error':
                content = environ['wsgi.input'].read()
                self.q.put(('error', content))
                start_response('202 Accepted', [])
                return ''
            if environ['PATH_INFO'] == '/complete':
                self.q.put(('complete', None))
                start_response('202 Accepted', [])
                return ''
        self.q.put(
            ('bad_request', '%(REQUEST_METHOD)s %(PATH_INFO)s' % environ)
        )
        start_response('400 Bad Request', [])
        return ''


def results_server(q, content, mime):
    app = WSGIApp(q, content, mime)
    httpd = make_server('', 8000, app)
    httpd.serve_forever()


class JavaScriptError(StandardError):
    pass


class JavaScriptTimeout(StandardError):
    pass


class InvalidTestMethod(StandardError):
    pass


METHODS = (
    'assertTrue',
    'assertEqual',
    'assertNotEqual',
    'assertAlmostEqual',
    'assertNotAlmostEqual',
    'assertGreater',
    'assertGreaterEqual',
    'assertLess',
    'assertLessEqual',
    'assertRegexpMatches',
    'assertNotRegexpMatches',
    'assertIn',
    'assertNotIn',
    'assertItemsEqual',
)


class JSTestCase(TestCase):
    js_files = tuple()
    q = None
    server = None
    client = None

    template = """
    <html
        xmlns="http://www.w3.org/1999/xhtml"
        xmlns:py="http://genshi.edgewall.org/"
    >
    <head>
    <title py:content="title" />
    <script py:content="inline_js" type="text/javascript" />
    </head>
    <body />
    </html>
    """

    @classmethod
    def setUpClass(cls):
        cls.template = dedent(cls.template).strip()
        cls.template_t = MarkupTemplate(cls.template)

    def setUp(self):
        self.q = multiprocessing.Queue()
        self.messages = []

    def render(self, **kw):
        return self.template_t.generate(**kw).render('xhtml', doctype='html5')

    def start_results_server(self, content, mime='text/html'):
        self.server = multiprocessing.Process(
            target=results_server,
            args=(self.q, content, mime),
        )
        self.server.daemon = True
        self.server.start()

    def collect_results(self, timeout=5):
        while True:
            try:
                (action, data) = self.q.get(timeout=timeout)
                self.messages.append((action, data))
            except Empty:
                raise JavaScriptTimeout()
            self.assertIn(action, ['get', 'test', 'error', 'complete'])
            if action == 'error':
                raise JavaScriptError(data)
            if action == 'complete':
                break
            if action == 'test':
                d = json.loads(data)
                if d['method'] not in METHODS:
                    raise InvalidTestMethod(data)
                method = getattr(self, d['method'])
                method(*d['args'])

    def tearDown(self):
        if self.server is not None:
            self.server.terminate()
            self.server.join()
        self.server = None
        self.q = None
        if self.client is not None:
            self.client.terminate()
            self.client.join()
        self.client = None
