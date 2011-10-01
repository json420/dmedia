# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2011 Novacut Inc
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
Unit tests for `dmedia.client`.
"""

from unittest import TestCase
import os
from http.client import HTTPConnection, HTTPSConnection

from microfiber import random_id
from filestore import ContentHash, TYPE_ERROR, DIGEST_BYTES

from dmedia import client


class FakeResponse:
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        self._data = os.urandom(16)

    def read(self):
        return self._data


class TestErrors(TestCase):
    def test_errors(self):
        self.assertEqual(
            client.errors,
            {
                400: client.BadRequest,
                401: client.Unauthorized,
                403: client.Forbidden,
                404: client.NotFound,
                405: client.MethodNotAllowed,
                406: client.NotAcceptable,
                409: client.Conflict,
                412: client.PreconditionFailed,
                415: client.BadContentType,
                416: client.BadRangeRequest,
                417: client.ExpectationFailed,
            }
        )
        method = 'MOST'
        path = '/restful?and=awesome'
        for (status, klass) in client.errors.items():
            reason = random_id()
            r = FakeResponse(status, reason)
            inst = klass(r, method, path)
            self.assertIs(inst.response, r)
            self.assertEqual(inst.method, method)
            self.assertEqual(inst.path, path)
            self.assertEqual(inst.data, r._data)
            self.assertEqual(
                str(inst),
                '{} {}: {} {}'.format(status, reason, method, path)
            )


class TestFunctions(TestCase):
    def test_http_conn(self):
        f = client.http_conn

        # Test with bad scheme
        with self.assertRaises(ValueError) as cm:
            (conn, t) = f('ftp://foo.s3.amazonaws.com/')
        self.assertEqual(
            str(cm.exception),
            "url scheme must be http or https: 'ftp://foo.s3.amazonaws.com/'"
        )

        # Test with bad url
        with self.assertRaises(ValueError) as cm:
            (inst, t) = f('http:foo.s3.amazonaws.com/')
        self.assertEqual(
            str(cm.exception),
            "bad url: 'http:foo.s3.amazonaws.com/'"
        )

        # Test with HTTP
        (conn, t) = f('http://foo.s3.amazonaws.com/')
        self.assertIsInstance(conn, HTTPConnection)
        self.assertNotIsInstance(conn, HTTPSConnection)
        self.assertEqual(t, ('http', 'foo.s3.amazonaws.com', '/', '', '', ''))

        # Test with HTTPS
        (conn, t) = f('https://foo.s3.amazonaws.com/')
        self.assertIsInstance(conn, HTTPSConnection)
        self.assertEqual(t, ('https', 'foo.s3.amazonaws.com', '/', '', '', ''))

    def test_bytes_range(self):
        f = client.bytes_range
        self.assertEqual(f(0, 500), 'bytes=0-499')
        self.assertEqual(f(500, 1000), 'bytes=500-999')
        self.assertEqual(f(-500), 'bytes=-500')
        self.assertEqual(f(9500), 'bytes=9500-')

    def test_check_slice(self):
        ch = ContentHash('foo', None, (1, 2, 3))

        # Test all valid slices
        client.check_slice(ch, 0, None)
        client.check_slice(ch, 1, None)
        client.check_slice(ch, 2, None)
        client.check_slice(ch, 0, 1)
        client.check_slice(ch, 0, 2)
        client.check_slice(ch, 1, 2)
        client.check_slice(ch, 0, 3)
        client.check_slice(ch, 1, 3)
        client.check_slice(ch, 2, 3)

        # ch type
        with self.assertRaises(TypeError) as cm:
            bad = ('foo', None, (1, 2, 3))
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('ch', ContentHash, tuple, bad)
        )

        # ch.leaf_hashes type
        with self.assertRaises(TypeError) as cm:
            bad = ContentHash('foo', None, os.urandom(DIGEST_BYTES))
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            'ch.leaf_hashes not unpacked for ch.id=foo'
        )

        # empty ch.leaf_hashes
        with self.assertRaises(ValueError) as cm:
            bad = ContentHash('foo', None, tuple())
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            'got empty ch.leaf_hashes for ch.id=foo'
        )

        # start type
        with self.assertRaises(TypeError) as cm:
            client.check_slice(ch, 0.0, None)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('start', int, float, 0.0)
        )

        # stop type
        with self.assertRaises(TypeError) as cm:
            client.check_slice(ch, 0, 1.0)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('stop', int, float, 1.0)
        )

        # start value
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, -1, None)
        self.assertEqual(
            str(cm.exception),
            'Need 0 <= start < 3; got start=-1'
        )
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 3, None)
        self.assertEqual(
            str(cm.exception),
            'Need 0 <= start < 3; got start=3'
        )

        # stop value
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 0, 0)
        self.assertEqual(
            str(cm.exception),
            'Need 1 <= stop <= 3; got stop=0'
        )
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 0, 4)
        self.assertEqual(
            str(cm.exception),
            'Need 1 <= stop <= 3; got stop=4'
        )

        # start < stop
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 2, 1)
        self.assertEqual(
            str(cm.exception),
            'Need start < stop; got start=2, stop=1'
        )
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 1, 1)
        self.assertEqual(
            str(cm.exception),
            'Need start < stop; got start=1, stop=1'
        )


class TestHTTPClient(TestCase):        
    def test_init(self):
        bad = 'sftp://localhost:5984/'
        with self.assertRaises(ValueError) as cm:
            inst = client.HTTPClient(bad)
        self.assertEqual(
            str(cm.exception),
            'url scheme must be http or https: {!r}'.format(bad)
        )
        bad = 'http:localhost:5984/foo/bar'
        with self.assertRaises(ValueError) as cm:
            inst = client.HTTPClient(bad)
        self.assertEqual(
            str(cm.exception),
            'bad url: {!r}'.format(bad)
        )

        inst = client.HTTPClient('https://localhost:5984/couch?foo=bar/')
        self.assertEqual(inst.url, 'https://localhost:5984/couch/')
        self.assertEqual(inst.basepath, '/couch/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = client.HTTPClient('http://localhost:5984?/')
        self.assertEqual(inst.url, 'http://localhost:5984/')
        self.assertEqual(inst.basepath, '/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = client.HTTPClient('http://localhost:5001/')
        self.assertEqual(inst.url, 'http://localhost:5001/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = client.HTTPClient('http://localhost:5002')
        self.assertEqual(inst.url, 'http://localhost:5002/')
        self.assertIsInstance(inst.conn, HTTPConnection)

        inst = client.HTTPClient('https://localhost:5003/')
        self.assertEqual(inst.url, 'https://localhost:5003/')
        self.assertIsInstance(inst.conn, HTTPSConnection)

        inst = client.HTTPClient('https://localhost:5004')
        self.assertEqual(inst.url, 'https://localhost:5004/')
        self.assertIsInstance(inst.conn, HTTPSConnection)