# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.js` module.
"""

from unittest import TestCase
import json
import subprocess
import time
import multiprocessing
import multiprocessing.queues

from dmedia import js
from dmedia.ui import datafile, load_datafile
from .helpers import DummyQueue, raises


class StartResponse(object):
    status = None
    headers = None

    def __call__(self, status, headers):
        assert self.status is None
        assert self.headers is None
        self.status = status
        self.headers = headers


class Input(object):
    def __init__(self, content):
        self.content = content

    def read(self, length):
        return self.content


class test_ResultsApp(TestCase):
    klass = js.ResultsApp

    def test_init(self):
        q = DummyQueue()
        scripts = {
            'mootools.js': 'here be mootools',
            'dmedia.js': 'here be dmedia',
        }
        index = 'foo'
        inst = self.klass(q, scripts, index)
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.scripts is scripts)
        self.assertTrue(inst.index is index)
        self.assertEqual(inst.mime, 'text/html')

        inst = self.klass(q, scripts, index, mime='application/xhtml+xml')
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.scripts is scripts)
        self.assertTrue(inst.index is index)
        self.assertEqual(inst.mime, 'application/xhtml+xml')

    def test_call(self):
        q = DummyQueue()
        scripts = {
            'mootools.js': 'here be mootools',
            'dmedia.js': 'here be dmedia',
        }
        index = 'foo bar'
        inst = self.klass(q, scripts, index)

        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), 'foo bar')
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(
            sr.headers,
            [
                ('Content-Type', 'text/html'),
                ('Content-Length', '7'),
            ]
        )
        self.assertEqual(q.messages, [('get', '/')])

        post1 = json.dumps({'args': ('one', 'two'), 'method': 'assertEqual'})
        env = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/assert',
            'wsgi.input': Input(post1),
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '202 Accepted')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            q.messages,
            [
                ('get', '/'),
                ('assert', post1),
            ]
        )

        post2 = 'oh no, it no worky!'
        env = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/error',
            'wsgi.input': Input(post2),
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '202 Accepted')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            q.messages,
            [
                ('get', '/'),
                ('assert', post1),
                ('error', post2),
            ]
        )

        env = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/complete',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '202 Accepted')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            q.messages,
            [
                ('get', '/'),
                ('assert', post1),
                ('error', post2),
                ('complete', None),
            ]
        )

        # Test with bad requests
        q = DummyQueue()
        index = 'foo bar'
        inst = self.klass(q, scripts, index)
        env = {'REQUEST_METHOD': 'PUT'}
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '405 Method Not Allowed')
        self.assertEqual(sr.headers, [])
        self.assertEqual(q.messages, [('bad_method', 'PUT')])

        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/error',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '404 Not Found')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            q.messages,
            [
                ('bad_method', 'PUT'),
                ('not_found', '/error'),
            ]
        )

        env = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': '/nope',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '400 Bad Request')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            q.messages,
            [
                ('bad_method', 'PUT'),
                ('not_found', '/error'),
                ('bad_request', 'POST /nope'),
            ]
        )

        # Test script reqests
        q = DummyQueue()
        index = 'foo bar'
        inst = self.klass(q, scripts, index)

        # /scripts/mootools.js
        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/scripts/mootools.js',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), 'here be mootools')
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(
            sr.headers,
            [
                ('Content-Type', 'application/javascript'),
                ('Content-Length', '16'),
            ]
        )
        self.assertEqual(inst.q.messages, [('get', '/scripts/mootools.js')])

        # /scripts/dmedia.js
        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/scripts/dmedia.js',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), 'here be dmedia')
        self.assertEqual(sr.status, '200 OK')
        self.assertEqual(
            sr.headers,
            [
                ('Content-Type', 'application/javascript'),
                ('Content-Length', '14'),
            ]
        )
        self.assertEqual(
            inst.q.messages,
            [
                ('get', '/scripts/mootools.js'),
                ('get', '/scripts/dmedia.js'),
            ]
        )

        # /scripts/foo.js
        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/scripts/foo.js',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '404 Not Found')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            inst.q.messages,
            [
                ('get', '/scripts/mootools.js'),
                ('get', '/scripts/dmedia.js'),
                ('not_found', '/scripts/foo.js'),
            ]
        )

        # /mootools.js
        env = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/mootools.js',
        }
        sr = StartResponse()
        self.assertEqual(inst(env, sr), '')
        self.assertEqual(sr.status, '404 Not Found')
        self.assertEqual(sr.headers, [])
        self.assertEqual(
            inst.q.messages,
            [
                ('get', '/scripts/mootools.js'),
                ('get', '/scripts/dmedia.js'),
                ('not_found', '/scripts/foo.js'),
                ('not_found', '/mootools.js'),
            ]
        )


expected = """
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>Hello Naughty Nurse!</title>
<script type="text/javascript">var foo = "bar";</script>
<script type="text/javascript" src="/scripts/dmedia.js"></script>
</head>
<body onload="py.run()"></body>
</html>
""".strip()


class test_JSTestCase(js.JSTestCase):

    def test_load_scripts(self):
        klass = self.__class__
        self.assertEqual(list(klass.load_scripts()), [])
        klass.js_files = (
            datafile('browser.js'),
            datafile('dmedia.js'),
        )
        self.assertEqual(
            list(klass.load_scripts()),
            [
                ('browser.js', load_datafile('browser.js')),
                ('dmedia.js', load_datafile('dmedia.js')),
            ]
        )

    def test_start_results_server(self):
        self.assertEqual(
            self.title, 'test_JSTestCase.test_start_results_server'
        )
        self.start_results_server({}, 'foo bar')
        self.assertTrue(isinstance(self.q, multiprocessing.queues.Queue))
        self.assertTrue(isinstance(self.server, multiprocessing.Process))
        time.sleep(1)
        self.assertTrue(self.server.daemon)
        self.assertTrue(self.server.is_alive())
        self.assertEqual(
            self.server._args,
            (self.q, {}, 'foo bar', 'text/html')
        )
        self.assertEqual(self.server._kwargs, {})
        self.server.terminate()
        self.server.join()

    def test_start_dummy_client(self):
        self.assertEqual(
            self.title, 'test_JSTestCase.test_start_dummy_client'
        )
        self.assertEqual(self.client, None)
        self.assertEqual(self.start_dummy_client(), None)
        self.assertIsInstance(self.client, subprocess.Popen)

    def test_build_data(self):
        self.assertEqual(self.title, 'test_JSTestCase.test_build_data')
        self.assertEqual(
            self.build_data(),
            {
                'methodName': 'test_build_data',
                'assertMethods': js.METHODS,
            }
        )
        self.assertEqual(
            self.build_data(foo='bar', stuff=17),
            {
                'methodName': 'test_build_data',
                'assertMethods': js.METHODS,
                'foo': 'bar',
                'stuff': 17,
            }
        )

    def test_build_js_inline(self):
        self.assertEqual(self.title, 'test_JSTestCase.test_build_js_inline')
        data = {
            'methodName': 'test_build_js_inline',
            'assertMethods': js.METHODS,
        }
        data_s = json.dumps(data, sort_keys=True, indent=4)
        var = 'py.data = %s;' % data_s
        self.assertEqual(
            self.build_js_inline(),
            '\n'.join([self.javascript, var])
        )

    def test_render(self):
        self.assertEqual(self.title, 'test_JSTestCase.test_render')
        kw = dict(
            title='Hello Naughty Nurse!',
            js_inline='var foo = "bar";',
            js_links=['/scripts/dmedia.js'],
        )
        self.assertMultiLineEqual(self.render(**kw), expected)

    def test_build_page(self):
        s = self.build_page()
        self.assertTrue(s.startswith('<!DOCTYPE html>'))

    def test_collect_results(self):
        self.assertEqual(self.title, 'test_JSTestCase.test_collect_results')

        # Test when client times out:
        self.q.put(('get', '/'))
        e = raises(js.JavaScriptTimeout, self.collect_results, timeout=1)
        self.assertEqual(self.messages, [('get', '/')])

        # Test when unhandled JavaScript exception is reported:
        self.q.put(('error', 'messed up'))
        e = raises(js.JavaScriptError, self.collect_results)
        self.assertEqual(str(e), 'messed up')
        self.assertEqual(
            self.messages,
            [
                ('get', '/'),
                ('error', 'messed up'),
            ]
        )

        # Test when complete is recieved:
        self.q.put(('complete', None))
        self.assertEqual(self.collect_results(), None)
        self.assertEqual(
            self.messages,
            [
                ('get', '/'),
                ('error', 'messed up'),
                ('complete', None),
            ]
        )

        # Test with invalid test method
        data1 = json.dumps({'method': 'assertNope'})
        self.q.put(('assert', data1))
        e = raises(js.InvalidTestMethod, self.collect_results)
        self.assertEqual(str(e), data1)
        self.assertEqual(
            self.messages,
            [
                ('get', '/'),
                ('error', 'messed up'),
                ('complete', None),
                ('assert', data1),
            ]
        )

        # Test with a correct test method and passing test
        data2 = json.dumps({'method': 'assertNotEqual', 'args': ['foo', 'bar']})
        self.q.put(('assert', data2))
        self.q.put(('complete', None))
        self.assertEqual(self.collect_results(), None)
        self.assertEqual(
            self.messages,
            [
                ('get', '/'),
                ('error', 'messed up'),
                ('complete', None),
                ('assert', data1),
                ('assert', data2),
                ('complete', None),
            ]
        )

        # Test with a correct test method and failing test
        data3 = json.dumps({'method': 'assertEqual', 'args': ['foo', 'bar']})
        self.q.put(('assert', data3))
        e = raises(AssertionError, self.collect_results)
        self.assertEqual(
            self.messages,
            [
                ('get', '/'),
                ('error', 'messed up'),
                ('complete', None),
                ('assert', data1),
                ('assert', data2),
                ('complete', None),
                ('assert', data3),
            ]
        )

    def test_METHODS(self):
        self.assertEqual(self.title, 'test_JSTestCase.test_METHODS')
        for name in js.METHODS:
            self.assertTrue(callable(getattr(self, name, None)), name)


class test_SelfTest(js.JSTestCase):
    js_files = (datafile('selftest.js'),)

    def test_self(self):
        self.run_js()
