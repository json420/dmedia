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

from dmedia import js
from .helpers import DummyQueue


class StartResponse(object):
    status = None
    headers = None

    def __call__(self, status, headers):
        assert self.status is None
        assert self.headers is None
        self.status = status
        self.headers = headers


class test_WSGIApp(TestCase):
    klass = js.WSGIApp

    def test_init(self):
        q = DummyQueue()
        content = 'foo'
        inst = self.klass(q, content)
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.content is content)
        self.assertEqual(inst.mime, 'text/html')

        inst = self.klass(q, content, mime='application/xhtml+xml')
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.content is content)
        self.assertEqual(inst.mime, 'application/xhtml+xml')

    def test_call(self):
        q = DummyQueue()
        content = 'foo bar'
        inst = self.klass(q, content)

        env = {'REQUEST_METHOD': 'GET'}
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
        self.assertEqual(q.messages, [('init', None)])
