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
Unit tests for `dmedia.transfers` module.
"""

from unittest import TestCase
from hashlib import sha1
from os import urandom
from base64 import b32encode
from multiprocessing import current_process
import time
import httplib

from dmedia import transfers, schema, filestore, errors
from dmedia.errors import DownloadFailure, DuplicateFile, IntegrityError

from .helpers import DummyQueue, mov_hash, mov_size, mov_leaves, raises, sample_mov
from .couch import CouchCase



def random_id(blocks=3):
    return b32encode(urandom(5 * blocks))


def b32hash(chunk):
    return sha1(chunk).digest()


class DummyProgress(object):
    def __init__(self):
        self._calls = []

    def __call__(self, completed):
        self._calls.append(completed)


class DummyFP(object):
    _chunk = None

    def write(self, chunk):
        assert chunk is not None
        assert self._chunk is None
        self._chunk = chunk


class TestFunctions(TestCase):
    def setUp(self):
        transfers._uploaders.clear()
        transfers._downloaders.clear()

    def test_download_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.download_key(file_id, store_id),
            ('downloads', file_id)
        )

    def test_upload_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.upload_key(file_id, store_id),
            ('uploads', file_id, store_id)
        )

    def test_register_uploader(self):
        f = transfers.register_uploader

        class Junk(object):
            pass

        class Okay(transfers.TransferBackend):
            pass

        # Test with wrong subclass
        with self.assertRaises(TypeError) as cm:
            f('foo', Junk)
        self.assertEqual(
            str(cm.exception),
            'backend must be {!r} subclass; got {!r}'.format(
                transfers.TransferBackend, Junk
            )
        )

        # Test when already registered
        transfers._uploaders['foo'] = None
        with self.assertRaises(ValueError) as cm:
            f('foo', Okay)
        self.assertEqual(
            str(cm.exception),
            'uploader {!r} exists, cannot register {!r}'.format('foo', Okay)
        )

        # Test when all good
        self.assertIsNone(f('bar', Okay))
        self.assertEqual(set(transfers._uploaders), set(['foo', 'bar']))
        self.assertIs(transfers._uploaders['bar'], Okay)

    def test_register_downloader(self):
        f = transfers.register_downloader

        class Junk(object):
            pass

        class Okay(transfers.TransferBackend):
            pass

        # Test with wrong subclass
        with self.assertRaises(TypeError) as cm:
            f('foo', Junk)
        self.assertEqual(
            str(cm.exception),
            'backend must be {!r} subclass; got {!r}'.format(
                transfers.TransferBackend, Junk
            )
        )

        # Test when already registered
        transfers._downloaders['foo'] = None
        with self.assertRaises(ValueError) as cm:
            f('foo', Okay)
        self.assertEqual(
            str(cm.exception),
            'downloader {!r} exists, cannot register {!r}'.format('foo', Okay)
        )

        # Test when all good
        self.assertIsNone(f('bar', Okay))
        self.assertEqual(set(transfers._downloaders), set(['foo', 'bar']))
        self.assertIs(transfers._downloaders['bar'], Okay)

    def test_get_uploader(self):
        f = transfers.get_uploader
        doc = {'_id': random_id(), 'plugin': 'foo'}
        cb = DummyProgress()

        class Example(transfers.TransferBackend):
            pass
        transfers._uploaders['foo'] =  Example

        foo = f(doc, cb)
        self.assertIsInstance(foo, Example)
        self.assertIs(foo.store, doc)
        self.assertIs(foo.callback, cb)

        foo = f(doc)
        self.assertIsInstance(foo, Example)
        self.assertIs(foo.store, doc)
        self.assertIsNone(foo.callback)

    def test_get_downloader(self):
        f = transfers.get_downloader
        doc = {'_id': random_id(), 'plugin': 'foo'}
        cb = DummyProgress()

        class Example(transfers.TransferBackend):
            pass
        transfers._downloaders['foo'] =  Example

        foo = f(doc, cb)
        self.assertIsInstance(foo, Example)
        self.assertIs(foo.store, doc)
        self.assertIs(foo.callback, cb)

        foo = f(doc)
        self.assertIsInstance(foo, Example)
        self.assertIs(foo.store, doc)
        self.assertIsNone(foo.callback)

    def test_bytes_range(self):
        f = transfers.bytes_range
        self.assertEqual(f(0, 500), 'bytes=0-499')
        self.assertEqual(f(500, 1000), 'bytes=500-999')
        self.assertEqual(f(-500), 'bytes=-500')
        self.assertEqual(f(9500), 'bytes=9500-')

    def test_range_request(self):
        f = transfers.range_request

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

    def test_http_conn(self):
        f = transfers.http_conn

        # Test with bad scheme
        with self.assertRaises(ValueError) as cm:
            (conn, t) = f('ftp://foo.s3.amazonaws.com/')
        self.assertEqual(
            str(cm.exception),
            "url scheme must be http or https; got 'ftp://foo.s3.amazonaws.com/'"
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
        self.assertIsInstance(conn, httplib.HTTPConnection)
        self.assertNotIsInstance(conn, httplib.HTTPSConnection)
        self.assertEqual(t, ('http', 'foo.s3.amazonaws.com', '/', '', '', ''))

        # Test with HTTPS
        (conn, t) = f('https://foo.s3.amazonaws.com/')
        self.assertIsInstance(conn, httplib.HTTPSConnection)
        self.assertEqual(t, ('https', 'foo.s3.amazonaws.com', '/', '', '', ''))


class TestTransferBackend(TestCase):
    klass = transfers.TransferBackend

    def test_init(self):
        doc = {'_id': 'foo', 'type': 'dmedia/store'}
        cb = DummyProgress()

        inst = self.klass(doc)
        self.assertIs(inst.store, doc)
        self.assertEqual(inst.store, {'_id': 'foo', 'type': 'dmedia/store'})
        self.assertEqual(inst.store_id, 'foo')
        self.assertIsNone(inst.callback)

        inst = self.klass(doc, callback=cb)
        self.assertIs(inst.store, doc)
        self.assertEqual(inst.store, {'_id': 'foo', 'type': 'dmedia/store'})
        self.assertEqual(inst.store_id, 'foo')
        self.assertIs(inst.callback, cb)

        with self.assertRaises(TypeError) as cm:
            inst = self.klass(doc, 17)
        self.assertEqual(
            str(cm.exception),
            'callback must be a callable; got {!r}'.format(17)
        )

        # Test defaults:
        inst = self.klass({})
        self.assertIsNone(inst.store_id)
        self.assertEqual(inst.copies, 0)
        self.assertIs(inst.use_ext, False)
        self.assertIs(inst.use_subdir, False)

        # Test provided values:
        inst = self.klass({'_id': 'bar', 'copies': 2,
                'use_ext': True,
                'use_subdir': True,
            }
        )
        self.assertEqual(inst.store_id, 'bar')
        self.assertEqual(inst.copies, 2)
        self.assertIs(inst.use_ext, True)
        self.assertIs(inst.use_subdir, True)

    def test_repr(self):
        inst = self.klass({'_id': 'foobar'})
        self.assertEqual(repr(inst), "TransferBackend('foobar')")

        class ExampleBackend(self.klass):
            pass

        inst = ExampleBackend({'_id': 'hellonaughtynurse'})
        self.assertEqual(repr(inst), "ExampleBackend('hellonaughtynurse')")

    def test_key(self):

        inst = self.klass({})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )

        inst = self.klass({'use_ext': True})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'
        )

        inst = self.klass({'use_subdir': True})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov'),
            'ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )

        inst = self.klass({'use_subdir': True, 'use_ext': True})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov'),
            'ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'
        )

    def test_progress(self):
        # Test with a callback
        callback = DummyProgress()
        inst = self.klass({}, callback)
        self.assertEqual(callback._calls, [])
        self.assertIsNone(inst.progress(17))
        self.assertEqual(callback._calls, [17])
        self.assertIsNone(inst.progress(18))
        self.assertEqual(callback._calls, [17, 18])

        # Test without a callback
        inst = self.klass({})
        self.assertIsNone(inst.progress(17))
        self.assertIsNone(inst.progress(18))

    def test_download(self):
        inst = self.klass({})
        with self.assertRaises(NotImplementedError) as cm:
            inst.download('file doc', 'filestore', 'leaves')
        self.assertEqual(
            str(cm.exception),
            'TransferBackend.download()'
        )

    def test_upload(self):
        inst = self.klass({})
        with self.assertRaises(NotImplementedError) as cm:
            inst.upload('file doc', 'filestore', 'leaves')
        self.assertEqual(
            str(cm.exception),
            'TransferBackend.upload()'
        )


class TestHTTPBackend(TestCase):
    """
    Test the `dmedia.transfers.TransferBackend` class.
    """
    klass = transfers.HTTPBackend

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
                self._signals = []

            def callback(self, *args):
                self._signals.append(args)

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

        # Test that it will return first correct response:
        inst = Example(a, b, b)
        self.assertEqual(inst.process_leaf(7, a_hash), a)



class TestTransferWorker(CouchCase):
    klass = transfers.TransferWorker

    def setUp(self):
        super(TestTransferWorker, self).setUp()
        self.q = DummyQueue()
        self.pid = current_process().pid

    def new(self):
        self.store_id = schema.random_id()
        self.key = ('uploads', mov_hash, self.store_id)
        return self.klass(self.env, self.q, self.key, (mov_hash, self.store_id))

    def test_init(self):
        inst = self.new()
        self.assertIsInstance(inst.filestore, filestore.FileStore)
        self.assertEqual(inst.filestore.parent, self.env['filestore']['path'])
        self.assertEqual(inst.filestore_id, self.env['filestore']['_id'])

    def test_init_file(self):
        inst = self.new()
        doc = schema.create_file(mov_size, mov_leaves, self.store_id)
        self.assertEqual(doc['_id'], mov_hash)
        inst.db.save(doc)
        self.assertIsNone(inst.init_file(mov_hash))
        self.assertEqual(inst.file_id, mov_hash)
        self.assertEqual(
            inst.file.pop('_attachments')['leaves']['data'],
            doc.pop('_attachments')['leaves']['data']
        )
        self.assertEqual(inst.file, doc)
        self.assertEqual(inst.file_size, mov_size)

    def test_init_remote(self):
        inst = self.new()
        doc = {
            '_id': self.store_id,
            'ver': 0,
            'type': 'dmedia/store',
            'time': time.time(),
            'plugin': 's3',
            'copies': 3,
            'bucket': 'stuff',
        }
        inst.db.save(doc)
        self.assertIsNone(inst.init_remote(self.store_id))
        self.assertEqual(inst.remote_id, self.store_id)
        self.assertEqual(inst.remote, doc)

    def test_init_file(self):
        inst = self.new()
        doc = schema.create_file(mov_size, mov_leaves, self.store_id)
        self.assertEqual(doc['_id'], mov_hash)
        inst.db.save(doc)
        self.assertIsNone(inst.init_file(mov_hash))
        self.assertEqual(inst.file_id, mov_hash)
        self.assertEqual(
            inst.file.pop('_attachments')['leaves']['data'],
            doc.pop('_attachments')['leaves']['data']
        )
        self.assertEqual(inst.file, doc)
        self.assertEqual(inst.file_size, mov_size)
        self.assertEqual(inst.leaves, mov_leaves)

    def test_init_file_bad(self):
        """
        Test with bad tophash = hash(bytes+leaf_hashes) relationship.
        """
        inst = self.new()
        doc = schema.create_file(mov_size, mov_leaves, self.store_id)
        self.assertEqual(doc['_id'], mov_hash)
        doc['bytes'] += 1
        inst.db.save(doc)

        with self.assertRaises(errors.TopHashError) as cm:
            inst.init_file(mov_hash)

        e = cm.exception
        self.assertEqual(e.got, 'FWC4JXINQR2ZKVL3GYA3E5VQUWXLSOFK')
        self.assertEqual(e.expected, mov_hash)
        self.assertEqual(e.size, mov_size + 1)

    def test_execute(self):
        inst = self.new()

        doc = schema.create_file(mov_size, mov_leaves, self.store_id)
        inst.db.save(doc)

        remote = {
            '_id': self.store_id,
            'ver': 0,
            'type': 'dmedia/store',
            'time': time.time(),
            'plugin': 's3',
            'copies': 3,
            'bucket': 'stuff',
        }
        inst.db.save(remote)

        self.assertFalse(hasattr(inst, 'transfer_called'))
        self.assertEqual(self.q.messages, [])
        self.assertIsNone(inst.execute(mov_hash, self.store_id))
        self.assertTrue(inst.transfer_called)

        self.assertEqual(inst.remote_id, self.store_id)
        self.assertEqual(inst.remote, remote)

        self.assertEqual(inst.file_id, mov_hash)
        self.assertEqual(
            inst.file.pop('_attachments')['leaves']['data'],
            doc.pop('_attachments')['leaves']['data']
        )
        self.assertEqual(inst.file, doc)
        self.assertEqual(inst.file_size, mov_size)
        self.assertEqual(inst.leaves, mov_leaves)

        self.assertEqual(
            self.q.messages,
            [
                dict(
                    signal='started',
                    args=(self.key,),
                    worker='TransferWorker',
                    pid=self.pid,
                ),
                dict(
                    signal='progress',
                    args=(self.key, 0, mov_size),
                    worker='TransferWorker',
                    pid=self.pid,
                ),
                dict(
                    signal='progress',
                    args=(self.key, mov_size, mov_size),
                    worker='TransferWorker',
                    pid=self.pid,
                ),
                dict(
                    signal='finished',
                    args=(self.key,),
                    worker='TransferWorker',
                    pid=self.pid,
                ),
            ]
        )


class TestDownloadWorker(CouchCase):
    klass = transfers.DownloadWorker

    def setUp(self):
        super(TestDownloadWorker, self).setUp()
        self.q = DummyQueue()
        self.pid = current_process().pid
        transfers._uploaders.clear()
        transfers._downloaders.clear()

        class S3(transfers.TransferBackend):
            def download(self, doc, leaves, fs):
                self._args = (doc, leaves, fs)
                self.callback(333)
                size = doc['bytes']
                chash = doc['_id']
                ext = doc.get('ext')
                tmp_fp = fs.allocate_for_transfer(size, chash, ext)
                tmp_fp.write(open(sample_mov, 'rb').read())
                tmp_fp.close()

        transfers.register_downloader('s3', S3)
        self.Backend = S3

    def new(self):
        self.store_id = schema.random_id()

        self.key = ('downloads', mov_hash)
        inst = self.klass(self.env, self.q, self.key, (mov_hash, self.store_id))

        self.file = schema.create_file(mov_size, mov_leaves, self.store_id)
        inst.db.save(self.file)

        self.remote = {
            '_id': self.store_id,
            'ver': 0,
            'type': 'dmedia/store',
            'time': time.time(),
            'plugin': 's3',
            'copies': 3,
            'bucket': 'stuff',
        }
        inst.db.save(self.remote)

        return inst

    def test_transfer(self):
        inst = self.new()
        inst.init_file(mov_hash)
        inst.init_remote(self.store_id)
        self.assertEqual(self.q.messages, [])
        self.assertFalse(hasattr(inst, 'backend'))

        self.assertIsNone(inst.transfer())
        self.assertIsInstance(inst.backend, self.Backend)
        self.assertEqual(inst.backend.store, self.remote)
        self.assertEqual(inst.backend.callback, inst.on_progress)
        self.assertEqual(
            inst.backend._args,
            (inst.file, inst.leaves, inst.filestore)
        )

        self.assertEqual(
            self.q.messages,
            [
                dict(
                    signal='progress',
                    args=(self.key, 333, mov_size),
                    worker='DownloadWorker',
                    pid=self.pid,
                ),
            ]
        )


class TestUploadWorker(CouchCase):
    klass = transfers.UploadWorker

    def setUp(self):
        super(TestUploadWorker, self).setUp()
        self.q = DummyQueue()
        self.pid = current_process().pid
        transfers._uploaders.clear()
        transfers._downloaders.clear()

        class S3(transfers.TransferBackend):
            def upload(self, *args):
                self._args = args
                self.callback(444)

        transfers.register_uploader('s3', S3)
        self.Backend = S3

    def new(self):
        self.store_id = schema.random_id()

        self.key = ('uploads', mov_hash)
        inst = self.klass(self.env, self.q, self.key, (mov_hash, self.store_id))

        self.file = schema.create_file(mov_size, mov_leaves, self.store_id)
        inst.db.save(self.file)

        self.remote = {
            '_id': self.store_id,
            'ver': 0,
            'type': 'dmedia/store',
            'time': time.time(),
            'plugin': 's3',
            'copies': 3,
            'bucket': 'stuff',
        }
        inst.db.save(self.remote)

        return inst

    def test_transfer(self):
        inst = self.new()
        inst.init_file(mov_hash)
        inst.init_remote(self.store_id)
        self.assertEqual(self.q.messages, [])
        self.assertFalse(hasattr(inst, 'backend'))

        self.assertIsNone(inst.transfer())
        self.assertIsInstance(inst.backend, self.Backend)
        self.assertEqual(inst.backend.store, self.remote)
        self.assertEqual(inst.backend.callback, inst.on_progress)
        self.assertEqual(
            inst.backend._args,
            (inst.file, inst.leaves, inst.filestore)
        )

        self.assertEqual(
            self.q.messages,
            [
                dict(
                    signal='progress',
                    args=(self.key, 444, mov_size),
                    worker='UploadWorker',
                    pid=self.pid,
                ),
            ]
        )
