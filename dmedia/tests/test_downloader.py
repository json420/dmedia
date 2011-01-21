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
Unit tests for `dmedia.downloader` module.
"""

from unittest import TestCase
from hashlib import sha1
from base64 import b32encode
from .helpers import raises, TempDir
from dmedia.errors import DownloadFailure
from dmedia import downloader


def b32hash(chunk):
    return b32encode(sha1(chunk).digest())


class test_functions(TestCase):
    def test_bytes_range(self):
        f = downloader.bytes_range
        self.assertEqual(f(0, 500), 'bytes=0-499')
        self.assertEqual(f(500, 1000), 'bytes=500-999')
        self.assertEqual(f(-500), 'bytes=-500')
        self.assertEqual(f(9500), 'bytes=9500-')

    def test_range_request(self):
        f = downloader.range_request

        e = raises(ValueError, f, -2, 1024, 3001)
        self.assertEqual(str(e), 'i must be >=0; got -2')
        e = raises(ValueError, f, 0, 500, 3001)
        self.assertEqual(str(e), 'leaf_size must be >=1024; got 500')
        e = raises(ValueError, f, 0, 1024, 0)
        self.assertEqual(str(e), 'file_size must be >=1; got 0')

        self.assertEqual(f(0, 1024, 3001), 'bytes=0-1023')
        self.assertEqual(f(1, 1024, 3001), 'bytes=1024-2047')
        self.assertEqual(f(2, 1024, 3001), 'bytes=2048-3000')

        e = raises(ValueError, f, 3, 1024, 3001)
        self.assertEqual(
            str(e),
            'past end of file: i=3, leaf_size=1024, file_size=3001'
        )

class DummyFP(object):
    _chunk = None

    def write(self, chunk):
        assert chunk is not None
        assert self._chunk is None
        self._chunk = chunk


class test_Downloader(TestCase):
    klass = downloader.Downloader

    def test_init(self):
        tmp = TempDir()
        dst_fp = open(tmp.join('dst'), 'wb')
        leaves = ['OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE']
        lsize = 1024
        fsize = 2311

        url = 'http://cdn.novacut.com/novacut_test_video.tgz'
        inst = self.klass(dst_fp, url, leaves, lsize, fsize)
        self.assertEqual(inst.url, url)
        self.assertEqual(inst.c.scheme, 'http')
        self.assertEqual(inst.c.netloc, 'cdn.novacut.com')
        self.assertEqual(inst.c.path, '/novacut_test_video.tgz')

        url = 'https://cdn.novacut.com/novacut_test_video.tgz'
        inst = self.klass(dst_fp, url, leaves, lsize, fsize)
        self.assertEqual(inst.url, url)
        self.assertEqual(inst.c.scheme, 'https')
        self.assertEqual(inst.c.netloc, 'cdn.novacut.com')
        self.assertEqual(inst.c.path, '/novacut_test_video.tgz')

        url = 'ftp://cdn.novacut.com/novacut_test_video.tgz'
        e = raises(ValueError, self.klass, dst_fp, url, leaves, lsize, fsize)
        self.assertEqual(
            str(e),
            'url scheme must be http or https; got %r' % url
        )

    def test_process_leaf(self):
        a = 'a' * 1024
        b = 'b' * 1024
        a_hash = b32hash(a)
        b_hash = b32hash(b)

        class Example(self.klass):
            def __init__(self, *chunks):
                self._chunks = chunks
                self._i = 0
                self.dst_fp = DummyFP()

            def download_leaf(self, i):
                assert i == 7
                chunk = self._chunks[self._i]
                self._i += 1
                return chunk

        # Test that DownloadFailure is raised after 3 attempts
        inst = Example(b, b, b, a)
        e = raises(DownloadFailure, inst.process_leaf, 7, a_hash)
        self.assertEqual(e.leaf, 7)
        self.assertEqual(e.expected, a_hash)
        self.assertEqual(e.got, b_hash)

        # Test that it will try 3 times:
        inst = Example(b, b, a)
        self.assertEqual(inst.process_leaf(7, a_hash), a)
        self.assertEqual(inst.dst_fp._chunk, a)

        # Test that it will return first correct response:
        inst = Example(a, b, b)
        self.assertEqual(inst.process_leaf(7, a_hash), a)
        self.assertEqual(inst.dst_fp._chunk, a)
