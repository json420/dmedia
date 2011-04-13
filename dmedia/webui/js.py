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
Allow ``unittest.TestCase`` assert methods to be called from JavaScript.


Example
=======

On the Python side, you will need a `JSTestCase` subclass with one or more
test methods, something like this:

>>> class TestFoo(JSTestCase):
...     js_files = (
...         '/abs/path/to/script/foo.js',
...         '/abs/path/to/script/test_foo.js',
...     )
...
...     def test_bar(self):
...         # 1. Optionally do something cool like initialize a Couch DB
...         self.run_js()  # 2. Call test_bar() JavaScript function
...         # 3. Optionally do something cool like verify changes in a Couch DB


Your foo.js file (containing the ``bar()`` function) would look something like
this:

    ::

        function bar() {
            return 'My JS unit testing is awesome';
        }


And your test_foo.js file (containing the ``test_bar()`` function) would look
something like this:

    ::

        py.test_bar = function() {
            py.assertEqual(bar(), 'My JS unit testing is awesome');
            py.assertNotEqual(bar(), 'My JS unit testing is lame');
        };


When you call, say, ``py.assertEqual()`` from your JavaScript test, the test
harness will ``JSON.stringify()`` the arguments and forward the call to the test
server using a synchronous XMLHttpRequest.  When the main process receives this
request from the `ResultsApp`, the actual test is performed using
``TestCase.assertEqual()``.

All the reporting is handled by the Python unit test framework, and you get an
overall pass/fail for the combined Python and JavaScript tests when you run:

    ::

        ./setup.py test


Oh, and the tests run headless in embedded WebKit making this viable for
automatic tests run with, say, Tarmac:

    https://launchpad.net/tarmac


Why this is Awesome
===================

You might be thinking, "Why reinvent the wheel, why not just use an existing
JavaScript unit testing framework?"

Fair enough, but the entire client side piece for this test framework is just 39
lines of JavaScript plus a bit of JSON data that gets written automatically.
Yet those 39 lines of JavaScript give you access to all these rich test methods:

    - ``TestCase.assertTrue()``
    - ``TestCase.assertFalse()``
    - ``TestCase.assertEqual()``
    - ``TestCase.assertNotEqual()``
    - ``TestCase.assertAlmostEqual()``
    - ``TestCase.assertNotAlmostEqual()``
    - ``TestCase.assertGreater()``
    - ``TestCase.assertGreaterEqual()``
    - ``TestCase.assertLess()``
    - ``TestCase.assertLessEqual()``
    - ``TestCase.assertIn()``
    - ``TestCase.assertNotIn()``
    - ``TestCase.assertItemsEqual()``
    - ``TestCase.assertIsNone()``
    - ``TestCase.assertIsNotNone()``
    - ``TestCase.assertRegexpMatches()``
    - ``TestCase.assertNotRegexpMatches()``


Existing JavaScript unit test frameworks don't tend to lend themselves to
automatic testing as they typically report the results graphically in the
browser... which is totally worthless if you want to run tests as part of an
automatic pre-commit step.  Remember what the late, great Ian Clatworthy taught
us: pre-commit continuous integration is 100% awesome.

If this weren't enough, there are some reasons why dmedia and Novacut in
particular benefit from this type of JavaScript testing.  Both use HTML5 user
interfaces that do everything, and I mean *everything*, by talking directly to
CouchDB with XMLHttpRequest.  The supporting core JavaScript needs to be very
solid to enable great user experiences to be built atop.

Considering the importance of these code paths, it's very handy to do setup from
Python before calling `JSTestCase.run_js()`... for example, putting a CouchDB
database into a known state.  Likewise, it's very handy to do out-of-band checks
from Python after `JSTestCase.run_js()` completes... for example, verifying that
the expected changes were made to the CouchDB  database.


How it Works
============

When you call `JSTestCase.run_js()`, the following happens:

    1. The correct HTML and JavaScript for the test are prepared

    2. The `ResultsApp` is fired up in a ``multiprocessing.Process``

    3. The ``dummy-client`` script is launched using ``subprocess.Popen()``

    4. The ``dummy-client`` requests the HTML and JavaScript from the
       `ResultsApp`

    5. The ``dummy-client`` executes the ``py.run()`` JavaScript function and
       posts the results to the `ResultsApp` as the test runs

    6. The `ResultsApp` puts the results in a ``multiprocessing.Queue`` as it
       receives them

    7. In the main process, `JSTestCase.collect_results()` gets results from the
       queue and tests the assertions till the test completes, an exception is
       raised, or the 5 second timeout is exceeded

    8. The `JSTestCase.tearDown()` method terminates the ``dummy-client`` and
       `ResultsApp` processes


FIXME:

    1. So that tests can be better organized and mirror how the Python tests are
       arranged, we should call by class name and method name, ie self.run_js()
       calls the JavaScript function:

       ``py.ClassName.method_name()``

    2. Need to be able to supply arbitrary files to ResultsApp so tests can GET
       these files... this is especially important for testing on binary data,
       which we can't directly make available to JavaScript.  We can borrow code
       from test-server.py, which already makes this totally generic.
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

from .util import render_var


tree = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
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


def results_server(q, scripts, index, mime):
    """
    Start HTTP server with `ResponseApp`.

    This function is the target of a ``multiprocessing.Process`` when the
    response server is started by `JSTestCase.start_results_server()`.

    :param q: a ``multiprocessing.Queue`` used to send results to main process
    :param scripts: a ``dict`` mapping script names to script content
    :param index: the HTML/XHTML to send to client
    :param mime: the content-type of the index page, eg ``'text/html'``
    """
    app = ResultsApp(q, scripts, index, mime)
    httpd = make_server('', 8000, app)
    httpd.serve_forever()


class JavaScriptError(StandardError):
    pass


class JavaScriptTimeout(StandardError):
    pass


class InvalidTestMethod(StandardError):
    pass

# unittest.TestCase methods that we allow to be called from JavaScript
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

    'assertIn',
    'assertNotIn',
    'assertItemsEqual',

    'assertIsNone',
    'assertIsNotNone',

    'assertRegexpMatches',
    'assertNotRegexpMatches',
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
    <script py:content="js_inline" type="text/javascript" />
    <script
        py:for="link in js_links"
        type="text/javascript"
        src="${link}"
    />
    </head>
    <body onload="py.run()">
    <div id="example" />
    </body>
    </html>
    """

    # The entire client-side test harness is only 39 lines of JavaScript!
    #
    # Note that py.data is dynamically written by JSTestCase.build_js_inline(),
    # and that this JavaScript will be inline in the first <script> tag in the
    # test HTML/XHTML page.
    #
    # The JavaScript files containing implementation and tests will follow in
    # the order they were defined in the JSTestClass.js_files subclass
    # attribute.
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
                var name = py.data.methodName;
                var method = py[name];
                if (!method) {
                    py.post('/error', 'Missing function py.' + name);
                }
                else {
                    method();
                }
            }
            catch (e) {
                py.post('/error', e);
            }
            finally {
                py.post('/complete');
            }
        },
    };
    """

    @classmethod
    def setUpClass(cls):
        cls.template = dedent(cls.template).strip()
        cls.template_t = MarkupTemplate(cls.template)
        cls.javascript = dedent(cls.javascript).strip()
        cls.scripts = tuple(cls.load_scripts())
        cls.js_links = tuple(
            '/scripts/' + name for (name, script) in cls.scripts
        )

    @classmethod
    def load_scripts(cls):
        for filename in cls.js_files:
            yield (
                path.basename(filename),
                open(filename, 'rb').read()
            )

    def setUp(self):
        self.title = '%s.%s' % (self.__class__.__name__, self._testMethodName)
        self.q = multiprocessing.Queue()
        self.messages = []

    def run_js(self, **extra):
        index = self.build_page(**extra)
        self.start_results_server(dict(self.scripts), index)
        self.start_dummy_client()
        self.collect_results()

    def build_data(self, **extra):
        data = {
            'methodName': self._testMethodName,
            'assertMethods': METHODS,
        }
        data.update(extra)
        return data

    def build_js_inline(self, **extra):
        data = self.build_data(**extra)
        return '\n'.join([self.javascript, render_var('py.data', data, 4)])

    def render(self, **kw):
        return self.template_t.generate(**kw).render('xhtml', doctype='html5')

    def build_page(self, **extra):
        kw = dict(
            title=self.title,
            js_inline=self.build_js_inline(**extra),
            js_links=self.js_links,
        )
        return self.render(**kw)

    def start_results_server(self, scripts, index, mime='text/html'):
        self.server = multiprocessing.Process(
            target=results_server,
            args=(self.q, scripts, index, mime),
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
