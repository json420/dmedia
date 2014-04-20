# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2012 Novacut Inc
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
Unit tests for `dmedia.service.background`.
"""

from unittest import TestCase
import multiprocessing
import threading
import time

from microfiber import Database
from usercouch.misc import TempCouch
from dbase32 import random_id

from dmedia.tests.base import TempDir, random_file_id
from dmedia.tests.couch import CouchCase
from dmedia.service import background


class TestSnapshots(CouchCase):
    def test_init(self):
        tmp = TempDir()

        def callback(name, success):
            raise Exception('should not be called')

        inst = background.Snapshots(self.env, tmp.dir, callback)
        self.assertIs(inst.env, self.env)
        self.assertIs(inst.dumpdir, tmp.dir)
        self.assertIs(inst.callback, callback)
        self.assertIsInstance(inst.in_q, multiprocessing.queues.Queue)
        self.assertIsInstance(inst.out_q, multiprocessing.queues.Queue)
        self.assertIsInstance(inst.in_flight, set)
        self.assertEqual(inst.in_flight, set())
        self.assertIsNone(inst.process)
        self.assertIsNone(inst.thread)

        # Test start()
        self.assertIsNone(inst.start())
        self.assertIsInstance(inst.process, multiprocessing.Process)
        self.assertTrue(inst.process.daemon)
        self.assertTrue(inst.process.is_alive())
        self.assertIsInstance(inst.thread, threading.Thread)
        self.assertTrue(inst.thread.daemon)
        self.assertTrue(inst.thread.is_alive())

        # Test shutdown()
        self.assertIsNone(inst.shutdown())
        self.assertEqual(inst.in_flight, set())
        self.assertIsNone(inst.process)
        self.assertIsNone(inst.thread)


class TestLazyAccess(TestCase):
    def test_init(self):
        db = Database('dmedia-1')
        inst = background.LazyAccess(db)
        self.assertIs(inst.db, db)
        self.assertIsInstance(inst.delay, int)
        self.assertEqual(inst.delay, 15 * 1000)
        self.assertEqual(inst.buf, {})
        self.assertIsNone(inst.timeout_id)

        inst = background.LazyAccess(db, seconds=45)
        self.assertIs(inst.db, db)
        self.assertIsInstance(inst.delay, int)
        self.assertEqual(inst.delay, 45 * 1000)
        self.assertEqual(inst.buf, {})
        self.assertIsNone(inst.timeout_id)

    def test_access(self):
        db = Database('dmedia-1')
        inst = background.LazyAccess(db)

        # So it's easier to test without requiring a mainloop:
        inst.timeout_id = 'dummy timeout id'

        _id = random_file_id()
        self.assertIsNone(inst.access(_id))
        self.assertEqual(inst.timeout_id, 'dummy timeout id')
        atime = inst.buf[_id]
        self.assertIsInstance(atime, int)
        self.assertLessEqual(atime, int(time.time()))
        self.assertEqual(inst.buf, {_id: atime})

        # Make sure the newest atime is used
        old = atime - 5
        inst.buf[_id] = old
        self.assertIsNone(inst.access(_id))
        atime = inst.buf[_id]
        self.assertIsInstance(atime, int)
        self.assertLessEqual(atime, int(time.time()))
        self.assertGreater(atime, old)
        self.assertEqual(inst.buf, {_id: atime})

    def test_on_timeout(self):
        class Dummy(background.LazyAccess):
            def __init__(self):
                self.flush_called = False
                self.timeout_id = 'dummy timeout id'

            def flush(self):
                assert self.flush_called is False
                self.flush_called = True

        inst = Dummy()
        self.assertIsNone(inst.on_timeout())
        self.assertIsNone(inst.timeout_id)
        self.assertIs(inst.flush_called, True)

    def test_flush(self):
        couch = TempCouch()
        db = Database('dmedia-1', couch.bootstrap())
        db.ensure()
        inst = background.LazyAccess(db)

        # Try flushing 30 atimes, all for docs that exist
        base = int(time.time())
        atimes = dict(
            (random_file_id(), base - i)
            for i in range(30)
        )
        docs = [{'_id': _id} for _id in atimes]
        db.save_many(docs)
        inst.buf.update(atimes)
        self.assertEqual(inst.flush(), 30)
        self.assertEqual(inst.buf, {})
        ids = sorted(atimes)
        docs = db.get_many(ids)
        for doc in docs:
            rev = doc.pop('_rev')
            self.assertTrue(rev.startswith('2-'))
            doc_id = doc.pop('_id')
            self.assertEqual(doc,
                {'atime': atimes[doc_id]}
            )

        # Try flushing 45 atimes, 25 for docs that don't exist
        base = int(time.time())
        atimes = dict(
            (random_file_id(), base - i)
            for i in range(20)
        )
        docs = [{'_id': _id} for _id in atimes]
        db.save_many(docs)
        exists = set(atimes)
        for i in range(25):  # No docs for these:
            nope_id = random_file_id()
            assert nope_id not in atimes
            atimes[nope_id] = base - i
        assert len(atimes) == 45
        inst.buf.update(atimes)
        assert len(inst.buf) == 45
        self.assertEqual(inst.flush(), 20)
        ids = sorted(atimes)
        docs = db.get_many(ids)
        for (_id, doc) in zip(ids, docs):
            if _id in exists:
                rev = doc.pop('_rev')
                self.assertTrue(rev.startswith('2-'))
                _id = doc.pop('_id')
                self.assertEqual(doc,
                    {'atime': atimes[_id]}
                )
            else:
                self.assertIsNone(doc)


class TestDownloads(TestCase):
    def test_init(self):
        env = random_id()
        ssl_config = random_id()
        downloads = background.Downloads(env, ssl_config)
        self.assertIs(downloads.env, env)
        self.assertIs(downloads.ssl_config, ssl_config)
        self.assertIsInstance(downloads.queue, multiprocessing.queues.Queue)
        self.assertIsNone(downloads.process)
