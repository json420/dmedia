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
import sys
from os import path
from subprocess import Popen
import multiprocessing
from Queue import Empty
from wsgiref.simple_server import make_server
import json
from textwrap import dedent

from genshi.template import MarkupTemplate

from .ui import render_var


tree = path.dirname(path.dirname(path.abspath(__file__)))
if path.exists(path.join(tree, 'setup.py')):
    dummy_client = path.join(tree, 'dummy-client')
else:
    dummy_client = path.join(sys.prefix, 'lib', 'dmedia', 'dummy-client')
assert path.isfile(dummy_client)


def read_input(environ):
    try:
        length = int(environ.get('CONTENT_LENGTH', '0'))
    except ValueError:
        return ''
    return environ['wsgi.input'].read(length)


class ResultsApp(object):
    """
    Simple WSGI app for collecting results from JavaScript tests.

    REST API
    ========

    To retrieve the test HTML page (will have appropriate JavaScript):

        ::

            GET / HTTP/1.1

    To retrieve a JavaScript file:

        ::

            GET /scripts/foo.js HTTP/1.1

    To test an assertion (assertEqual, assertTrue, etc):

        ::

            POST /assert HTTP/1.1
            Content-Type: application/json

            {"method": "assertEqual", "args": ["foo", "bar"]}

    To report an unhandled exception:

        ::

            POST /error HTTP/1.1
            Content-Type: application/json

            "Oh no, caught an unhandled JavaScript exception!"

    Finally, to conclude a test:

        ::

            POST /complete HTTP/1.1
    """
    def __init__(self, q, scripts, index, mime='text/html'):
        self.q = q
        self.scripts = scripts
        self.index = index
        self.mime = mime

    def __call__(self, environ, start_response):
        method = environ['REQUEST_METHOD']
        if method not in ('GET', 'POST'):
            self.q.put(('bad_method', method))
            start_response('405 Method Not Allowed', [])
            return ''
        path_info = environ['PATH_INFO']
        if method == 'GET':
            if path_info == '/':
                headers = [
                    ('Content-Type', self.mime),
                    ('Content-Length', str(len(self.index))),
                ]
                self.q.put(('get', path_info))
                start_response('200 OK', headers)
                return self.index
            s = '/scripts/'
            if path_info.startswith(s):
                name = path_info[len(s):]
                if name in self.scripts:
                    script = self.scripts[name]
                    headers = [
                        ('Content-Type', 'application/javascript'),
                        ('Content-Length', str(len(script))),
                    ]
                    self.q.put(('get', path_info))
                    start_response('200 OK', headers)
                    return script
            self.q.put(('not_found', path_info))
            start_response('404 Not Found', [])
            return ''
        if method == 'POST':
            if path_info == '/assert':
                content = read_input(environ)
                self.q.put(('assert', content))
                start_response('202 Accepted', [])
                return ''
            if path_info == '/error':
                content = read_input(environ)
                self.q.put(('error', content))
                start_response('202 Accepted', [])
                return ''
            if path_info == '/complete':
                self.q.put(('complete', None))
                start_response('202 Accepted', [])
                return ''
        self.q.put(
            ('bad_request', '%(REQUEST_METHOD)s %(PATH_INFO)s' % environ)
        )
        start_response('400 Bad Request', [])
        return ''


def results_server(q, content, mime):
    app = ResultsApp(q, {}, content, mime)
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
    'assertFalse',
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


javascript = """
var py = {

    /* Synchronously POST results to ResultsApp */
    post: function(path, obj) {
        var request = new XMLHttpRequest();
        request.open('POST', path, false);
        if (obj) {
            request.setRequestHeader('Content-Type', 'application/json');
            request.send(JSON.stringify(obj));
        }
        else {
            request.send();
        }
    },

    /* Initialize the py.assertFoo() functions */
    init: function() {
        py.data.assertMethods.forEach(function(name) {
            py[name] = function() {
                var args = Array.prototype.slice.call(arguments);
                py.post('/assert', {method: name, args: args});
            };
        });
    },

    /* Run the test function indicated by py.data.methodName */
    run: function() {
        try {
            py.init();
            var method = py[py.data.methodName];
            method();
        }
        catch (e) {
            py.post('/error', e);
        }
        finally {
            py.post('/complete');
        }
    },

    /* Run a selftest on the tester */
    test_self: function() {
        py.assertTrue(true);
        py.assertFalse(false);
        py.assertEqual('foo', 'foo');
        py.assertNotEqual('foo', 'bar');
        py.assertAlmostEqual(1.2, 1.2);
        py.assertNotAlmostEqual(1.2, 1.3);
        py.assertGreater(3, 2);
        py.assertGreaterEqual(3, 3);
        py.assertLess(2, 3);
        py.assertLessEqual(2, 2);
        py.assertIn('bar', ['foo', 'bar', 'baz']);
        py.assertNotIn('car', ['foo', 'bar', 'baz']);
        py.assertItemsEqual(['foo', 'bar', 'baz'], ['baz', 'foo', 'bar']);
    },
};
"""


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
    <body onload="py.run()" />
    </html>
    """

    @classmethod
    def setUpClass(cls):
        cls.template = dedent(cls.template).strip()
        cls.template_t = MarkupTemplate(cls.template)

    def setUp(self):
        self.title = '%s.%s' % (self.__class__.__name__, self._testMethodName)
        self.q = multiprocessing.Queue()
        self.messages = []

    def run_js(self, **extra):
        content = self.build_page(**extra)
        self.start_results_server(content)
        self.start_dummy_client()
        self.collect_results()

    def build_data(self, **extra):
        data = {
            'methodName': self._testMethodName,
            'assertMethods': METHODS,
        }
        data.update(extra)
        return data

    def build_inline_js(self, **extra):
        data = self.build_data(**extra)
        return '\n'.join([javascript, render_var('py.data', data, 4)])

    def render(self, **kw):
        return self.template_t.generate(**kw).render('xhtml', doctype='html5')

    def build_page(self, **extra):
        kw = dict(
            title=self.title,
            inline_js=self.build_inline_js(**extra),
        )
        return self.render(**kw)

    def start_results_server(self, content, mime='text/html'):
        self.server = multiprocessing.Process(
            target=results_server,
            args=(self.q, content, mime),
        )
        self.server.daemon = True
        self.server.start()

    def start_dummy_client(self):
        cmd = [dummy_client, 'http://localhost:8000/']
        self.client = Popen(cmd)

    def collect_results(self, timeout=5):
        while True:
            try:
                (action, data) = self.q.get(timeout=timeout)
                self.messages.append((action, data))
            except Empty:
                raise JavaScriptTimeout()
            self.assertIn(
                action,
                ['get', 'not_found', 'assert', 'error', 'complete']
            )
            # Note that no action is taken for 'get' and 'not_found'.
            # 'not_found' is allowed because of things like GET /favicon.ico
            if action == 'error':
                raise JavaScriptError(data)
            if action == 'complete':
                break
            if action == 'assert':
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
            self.client.wait()
        self.client = None
