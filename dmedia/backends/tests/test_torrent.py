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
Unit tests for the `dmedia.backends.torrent` module.
"""

from unittest import TestCase
import httplib

from dmedia.backends import torrent


class TestTorrentBackend(TestCase):
    klass = torrent.TorrentBackend

    def test_init(self):
        url = 'https://foo.s3.amazonaws.com/'
        inst = self.klass({'url': url})
        self.assertEqual(inst.url, url)
        self.assertEqual(inst.basepath, '/')
        self.assertEqual(
            inst.t,
            ('https', 'foo.s3.amazonaws.com', '/', '', '', '')
        )
        self.assertIsInstance(inst.conn, httplib.HTTPSConnection)

        url = 'http://example.com/bar'
        inst = self.klass({'url': url})
        self.assertEqual(inst.url, url)
        self.assertEqual(inst.basepath, '/bar/')
        self.assertEqual(
            inst.t,
            ('http', 'example.com', '/bar', '', '', '')
        )
        self.assertIsInstance(inst.conn, httplib.HTTPConnection)
        self.assertNotIsInstance(inst.conn, httplib.HTTPSConnection)

        with self.assertRaises(ValueError) as cm:
            inst = self.klass({'url': 'ftp://example.com/'})
        self.assertEqual(
            str(cm.exception),
            "url scheme must be http or https; got 'ftp://example.com/'"
        )

        with self.assertRaises(ValueError) as cm:
            inst = self.klass({'url': 'http:example.com/bar'})
        self.assertEqual(
            str(cm.exception),
            "bad url: 'http:example.com/bar'"
        )
