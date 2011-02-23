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

import os
from os import path
from unittest import TestCase
from hashlib import sha1
from base64 import b32encode
from .helpers import raises, TempDir
from .helpers import sample_mov, sample_thm
from .helpers import mov_hash, thm_hash
from dmedia.constants import TYPE_ERROR, LEAF_SIZE
from dmedia.errors import DownloadFailure, DuplicateFile, IntegrityError
from dmedia.filestore import FileStore, HashList
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


class test_TorrentDownloader(TestCase):
    klass = downloader.TorrentDownloader

    def test_init(self):
        tmp = TempDir()
        fs = FileStore(tmp.path)

        e = raises(TypeError, self.klass, '', 17, mov_hash)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('fs', FileStore, int, 17)
        )

        inst = self.klass('', fs, mov_hash)
        self.assertEqual(inst.torrent, '')
        self.assertTrue(inst.fs is fs)
        self.assertEqual(inst.chash, mov_hash)
        self.assertEqual(inst.ext, None)

        inst = self.klass('', fs, mov_hash, ext='mov')
        self.assertEqual(inst.torrent, '')
        self.assertTrue(inst.fs is fs)
        self.assertEqual(inst.chash, mov_hash)
        self.assertEqual(inst.ext, 'mov')

    def test_get_tmp(self):
        # Test with ext='mov'
        tmp = TempDir()
        fs = FileStore(tmp.path)
        inst = self.klass('', fs, mov_hash, 'mov')
        d = tmp.join('transfers')
        f = tmp.join('transfers', mov_hash + '.mov')
        self.assertFalse(path.exists(d))
        self.assertFalse(path.exists(f))
        self.assertEqual(inst.get_tmp(), f)
        self.assertTrue(path.isdir(d))
        self.assertFalse(path.exists(f))

        # Test with ext=None
        tmp = TempDir()
        fs = FileStore(tmp.path)
        inst = self.klass('', fs, mov_hash)
        d = tmp.join('transfers')
        f = tmp.join('transfers', mov_hash)
        self.assertFalse(path.exists(d))
        self.assertFalse(path.exists(f))
        self.assertEqual(inst.get_tmp(), f)
        self.assertTrue(path.isdir(d))
        self.assertFalse(path.exists(f))

    def test_finalize(self):
        tmp = TempDir()
        fs = FileStore(tmp.path)
        inst = self.klass('', fs, mov_hash, 'mov')

        src_d = tmp.join('transfers')
        src = tmp.join('transfers', mov_hash + '.mov')
        dst_d = tmp.join(mov_hash[:2])
        dst = tmp.join(mov_hash[:2], mov_hash[2:] + '.mov')

        # Test when transfers/ dir doesn't exist:
        e = raises(IOError, inst.finalize)
        self.assertFalse(path.exists(src_d))
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when transfers/ exists but file does not:
        self.assertEqual(fs.tmp(mov_hash, 'mov', create=True), src)
        self.assertTrue(path.isdir(src_d))
        e = raises(IOError, inst.finalize)
        self.assertFalse(path.exists(src))
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when file has wrong content hash and wrong size:
        open(src, 'wb').write(open(sample_thm, 'rb').read())
        e = raises(IntegrityError, inst.finalize)
        self.assertEqual(e.got, thm_hash)
        self.assertEqual(e.expected, mov_hash)
        self.assertEqual(e.filename, src)
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test when file has wrong content hash and *correct* size:
        fp1 = open(sample_mov, 'rb')
        fp2 = open(src, 'wb')
        while True:
            chunk = fp1.read(LEAF_SIZE)
            if not chunk:
                break
            fp2.write(chunk)
        fp1.close()

        # Now change final byte at end file:
        fp2.seek(-1, os.SEEK_END)
        fp2.write('A')
        fp2.close()
        self.assertEqual(path.getsize(sample_mov), path.getsize(src))

        e = raises(IntegrityError, inst.finalize)
        self.assertEqual(e.got, 'AYDIKK2IYAYTP7H5QCDK5FQ55F7QH4EN')
        self.assertEqual(e.expected, mov_hash)
        self.assertEqual(e.filename, src)
        self.assertFalse(path.exists(dst_d))
        self.assertFalse(path.exists(dst))

        # Test with correct content hash:
        fp1 = open(sample_mov, 'rb')
        fp2 = open(src, 'wb')
        while True:
            chunk = fp1.read(LEAF_SIZE)
            if not chunk:
                break
            fp2.write(chunk)
        fp1.close()
        fp2.close()
        self.assertEqual(inst.finalize(), dst)
        self.assertTrue(path.isdir(src_d))
        self.assertFalse(path.exists(src))
        self.assertTrue(path.isdir(dst_d))
        self.assertTrue(path.isfile(dst))

        # Check content hash of file in canonical location
        fp = open(dst, 'rb')
        self.assertEqual(HashList(fp).run(), mov_hash)


class test_S3Transfer(TestCase):
    klass = downloader.S3Transfer

    def test_init(self):
        inst = self.klass('novacut', 'foo', 'bar')
        self.assertEqual(inst.bucketname, 'novacut')
        self.assertEqual(inst.keyid, 'foo')
        self.assertEqual(inst.secret, 'bar')
        self.assertEqual(inst._bucket, None)

    def test_repr(self):
        inst = self.klass('novacut', 'foo', 'bar')
        self.assertEqual(repr(inst), "S3Transfer('novacut', <keyid>, <secret>)")

    def test_key(self):
        self.assertEqual(
            self.klass.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            self.klass.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext=None),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            self.klass.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext='mov'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'
        )

    def test_bucket(self):
        inst = self.klass('novacut', 'foo', 'bar')
        inst._bucket = 'whatever'
        self.assertEqual(inst.bucket, 'whatever')
