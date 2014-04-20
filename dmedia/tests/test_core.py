# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
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
Unit tests for `dmedia.core` module.
"""

from unittest import TestCase
import os
from os import path
import time
from base64 import b64encode
import queue
import multiprocessing
import threading

import microfiber
from dbase32 import random_id
import filestore
from filestore import FileStore
from filestore.misc import TempFileStore
from filestore.migration import Migration, b32_to_db32
from usercouch.misc import CouchTestCase

from dmedia.local import LocalStores
from dmedia import metastore
from dmedia.metastore import MetaStore, get_mtime
from dmedia.schema import project_db_name
from dmedia.parallel import start_process
from dmedia import util, core

from .couch import CouchCase
from .base import TempDir, write_random


class TestFunctions(TestCase):
    def test_has_thumbnail(self):
        self.assertIs(core.has_thumbnail({}), False)
        doc = {'_attachments': {}}
        self.assertIs(core.has_thumbnail(doc), False)
        doc = {'_attachments': {'foo': 'bar'}}
        self.assertIs(core.has_thumbnail(doc), False)
        doc = {'_attachments': {'thumbnail': 'yup'}}
        self.assertIs(core.has_thumbnail(doc), True)

    def test_encode_attachment(self):
        data = os.urandom(1776)
        self.assertEqual(
            core.encode_attachment('image/jpeg', data),
            {
                'content_type': 'image/jpeg',
                'data': b64encode(data).decode('utf-8'),
            }
        )


class TestCouchFunctions(CouchCase):
    def test_db_dump_iter(self):
        server = microfiber.Server(self.env)
        self.assertEqual(list(core.db_dump_iter(server)), [])
        server.put(None, 'thumbnails')
        self.assertEqual(list(core.db_dump_iter(server)), [])
        server.put(None, 'foo')
        self.assertEqual(list(core.db_dump_iter(server)), ['foo'])
        server.put(None, 'bar')
        self.assertEqual(list(core.db_dump_iter(server)), ['bar', 'foo'])

    def test_projects_iter(self):
        server = microfiber.Server(self.env)
        self.assertEqual(list(core.projects_iter(server)), [])
        ids = tuple(random_id() for i in range(20))
        for _id in ids:
            db_name = project_db_name(_id)
            server.put(None, db_name)
        self.assertEqual(
            list(core.projects_iter(server)),
            [(project_db_name(_id), _id) for _id in sorted(ids)]
        )

    def test_update_machine(self):
        # Empty stores and peers:
        doc = {}
        timestamp = time.time()
        stores = {}
        peers = {}
        self.assertIsNone(core.update_machine(doc, timestamp, stores, peers))
        self.assertEqual(doc, {
            'mtime': int(timestamp),
            'stores': {},
            'peers': {},
        })
        self.assertIs(doc['stores'], stores)
        self.assertIs(doc['peers'], peers)

        # Populated stores and peers:
        timestamp = time.time()

        store_id1 = random_id()
        parentdir1 = random_id()
        store_id2 = random_id()
        parentdir2 = random_id()
        stores = {
            store_id1: {
                'copies': 1,
                'parentdir': parentdir1,
            },
            store_id2: {
                'copies': 2,
                'parentdir': parentdir2,
            },
        }

        peer_id1 = random_id(30)
        url1 = random_id()
        peer_id2 = random_id(30)
        peers = {
            peer_id1: {
                'host': 'foo',
                'url': url1,
            },
            peer_id2: {
                'host': 'foo',
                'url': url1,
            },
        }

        self.assertIsNone(core.update_machine(doc, timestamp, stores, peers))
        self.assertEqual(doc, {
            'mtime': int(timestamp),
            'stores': {
                store_id1: {
                    'copies': 1,
                    'parentdir': parentdir1,
                },
                store_id2: {
                    'copies': 2,
                    'parentdir': parentdir2,
                },
            },
            'peers': {
                peer_id1: {
                    'host': 'foo',
                    'url': url1,
                },
                peer_id2: {
                    'host': 'foo',
                    'url': url1,
                },
            },
        })
        self.assertIs(doc['stores'], stores)
        self.assertIs(doc['peers'], peers)


class MockDB:
    def __init__(self, update_seq):
        self._update_seq = update_seq
        self._calls = 0

    def get(self):
        self._calls += 1
        return {'update_seq': self._update_seq}


class MockMetaStore:
    def __init__(self, db, docs):
        self.db = db
        self._docs = docs
        self._calls = []

    def iter_fragile_files(self, stop):
        self._calls.append(stop)
        for doc in self._docs:
            yield doc


class TestVigilanceMocked(TestCase):
    def test_process_backlog(self):
        class Mocked(core.Vigilance):
            def __init__(self, ms):
                self.ms = ms
                self._calls = []

            def update_remote(self):
                self._calls.append('update_remote')

            def wrap_up_rank(self, doc, threshold):
                self._calls.append((doc, threshold))

        docs = tuple(random_id() for i in range(10))
        ms = MockMetaStore(MockDB(17), docs)
        mocked = Mocked(ms)
        self.assertEqual(mocked.process_backlog(4), 17)
        self.assertEqual(ms.db._calls, 1)
        self.assertEqual(ms._calls, [4])
        self.assertEqual(mocked._calls,
            ['update_remote'] + [
                (doc, metastore.MIN_BYTES_FREE) for doc in docs
            ]
        )

        docs = tuple(random_id() for i in range(36))
        ms = MockMetaStore(MockDB(18), docs)
        mocked = Mocked(ms)
        self.assertEqual(mocked.process_backlog(6), 18)
        self.assertEqual(ms.db._calls, 1)
        self.assertEqual(ms._calls, [6])
        self.assertEqual(mocked._calls,
            ['update_remote'] + [
                (doc, metastore.MIN_BYTES_FREE) for doc in docs
            ]
        )

    def test_up_rank(self):
        class Mocked(core.Vigilance):
            def __init__(self, local, remote):
                self.local = frozenset(local)
                self.remote = frozenset(remote)
                self._calls = []

            def up_rank_by_verifying(self, doc, downgraded):
                self._calls.extend(('verify', doc, downgraded))
                return doc

            def up_rank_by_copying(self, doc, free, threshold):
                self._calls.extend(('copy', doc, free, threshold))
                return doc

            def up_rank_by_downloading(self, doc, remote, threshold):
                self._calls.extend(('download', doc, remote, threshold))
                return doc

        local = tuple(random_id() for i in range(2))
        remote = tuple(random_id() for i in range(2))
        mocked = Mocked(local, remote)

        # Verify, one local:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
            },
        }
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Verify, two local:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
                local[1]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Verify, one local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 0},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['verify', doc, {local[0]}]
        )

        # Copy, one local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 1},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['copy', doc, {local[1]}, 17]
        )

        # Copy, two local, one remote:
        doc = {
            'stored': {
                local[0]: {'copies': 1},
                local[1]: {'copies': 1},
                remote[0]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])

        # Download, one remote:
        doc = {
            'stored': {
                remote[0]: {'copies': 0},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['download', doc, {remote[0]}, 17]
        )

        # Download, two remote:
        doc = {
            'stored': {
                remote[0]: {'copies': 0},
                remote[1]: {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIs(mocked.up_rank(doc, 17), doc)
        self.assertEqual(mocked._calls,
            ['download', doc, set(remote), 17]
        )

        # Available in neither local nor remote:
        doc = {
            'stored': {
                random_id(): {'copies': 0},
                random_id(): {'copies': 1},
            },
        }
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])

        # Empty doc['stored']:
        doc = {'stored': {}}
        mocked._calls.clear()
        self.assertIsNone(mocked.up_rank(doc, 17))
        self.assertEqual(mocked._calls, [])


class TestVigilance(CouchCase):
    def test_init(self):
        db = util.get_db(self.env, True)
        ms = MetaStore(db)
        inst = core.Vigilance(ms, None)
        self.assertIs(inst.ms, ms)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertIsInstance(inst.local, frozenset)
        self.assertEqual(inst.local, frozenset())
        self.assertEqual(inst.clients, {})
        self.assertEqual(inst.peers, {})

    def test_update_remote(self):
        db = util.get_db(self.env, True)
        ms = MetaStore(db)
        inst = core.Vigilance(ms, None)
        self.assertFalse(hasattr(inst, 'remote'))
        self.assertFalse(hasattr(inst, 'store_to_peer'))

        # Test when peers is empty:
        self.assertIsNone(inst.update_remote())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote, frozenset())
        self.assertEqual(inst.store_to_peer, {})

        peer_id1 = random_id(30)
        peer_id2 = random_id(30)
        store_id1 = random_id()
        store_id2 = random_id()
        store_id3 = random_id()
        store_id4 = random_id()

        # Test when there are peers, but their machine docs don't exist:
        inst.peers = {peer_id1: 'foo', peer_id2: 'bar'}
        self.assertIsNone(inst.update_remote())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote, frozenset())
        self.assertEqual(inst.store_to_peer, {})

        # Test when just one doc exists:
        doc1 = {
            '_id': peer_id1,
            'stores': {
                store_id1: 'one',
                store_id2: 'two',
            }
        }
        db.save(doc1)
        self.assertIsNone(inst.update_remote())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote, {store_id1, store_id2})
        self.assertEqual(inst.store_to_peer, {
            store_id1: peer_id1,
            store_id2: peer_id1,
        })

        # Test when both docs exist:
        doc2 = {
            '_id': peer_id2,
            'stores': {
                store_id3: 'one',
                store_id4: 'two',
            }
        }
        db.save(doc2)
        self.assertIsNone(inst.update_remote())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote,
            {store_id1, store_id2, store_id3, store_id4}
        )
        self.assertEqual(inst.store_to_peer, {
            store_id1: peer_id1,
            store_id2: peer_id1,
            store_id3: peer_id2,
            store_id4: peer_id2,
        })

        # Finally, test when peers is empty but docs exist:
        inst.peers = {}
        self.assertIsNone(inst.update_remote())
        self.assertIsInstance(inst.remote, frozenset)
        self.assertEqual(inst.remote, frozenset())
        self.assertEqual(inst.store_to_peer, {})

        # Docs should not have been modified:
        self.assertEqual(db.get_many([peer_id1, peer_id2]), [doc1, doc2])


class DummyProcess:
    def __init__(self):
        self._calls = []

    def terminate(self):
        self._calls.append('terminate')

    def join(self):
        self._calls.append('join')


class TestTaskPool(TestCase):
    def test_init(self):
        pool = core.TaskPool()
        self.assertEqual(pool.tasks, {})
        self.assertEqual(pool.active_tasks, {})
        self.assertIsNone(pool.thread)
        self.assertIsInstance(pool.queue, queue.Queue)
        self.assertIsInstance(pool.restart_always, frozenset)
        self.assertEqual(pool.restart_always, frozenset())
        self.assertIsInstance(pool.restart_once, set)
        self.assertEqual(pool.restart_always, set())
        self.assertIs(pool.running, False)

        key1 = random_id()
        pool = core.TaskPool(key1)
        self.assertEqual(pool.restart_always, {key1})
        key2 = random_id()
        pool = core.TaskPool(key1, key2)
        self.assertEqual(pool.restart_always, {key1, key2})

    def test_start_stop_reaper(self):
        pool = core.TaskPool()

        self.assertIs(pool.start_reaper(), True)
        self.assertIsInstance(pool.thread, threading.Thread)
        self.assertIs(pool.start_reaper(), False)
        self.assertIsInstance(pool.thread, threading.Thread)

        self.assertIs(pool.stop_reaper(), True)
        self.assertIsNone(pool.thread)
        self.assertIs(pool.stop_reaper(), False)
        self.assertIsNone(pool.thread)

    def test_reaper(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self):
                self._forwards = []
                super().__init__()

            def forward_completed_task(self, task):
                assert task.process.exitcode == 0
                self._forwards.append(task)
  
        def dummy_worker(timeout):
            time.sleep(timeout)

        # one item in queue: [None]
        pool = MockedTaskPool()
        pool.queue.put(None)
        self.assertIsNone(pool.reaper(timeout=1))
        self.assertTrue(pool.queue.empty())
        self.assertEqual(pool._forwards, [])

        # Duplicate task.key:
        task1 = core.ActiveTask('foo', start_process(dummy_worker, 15), None)
        task2 = core.ActiveTask('foo', start_process(dummy_worker, 15), None)
        for task in (task1, task2):
            pool.queue.put(task)
        with self.assertRaises(ValueError) as cm:
            pool.reaper(timeout=1)
        self.assertEqual(str(cm.exception), "key 'foo' is already in task_map")
        for task in (task1, task2):
            task.process.terminate()
            task.process.join()
        self.assertTrue(pool.queue.empty())
        self.assertEqual(pool._forwards, [])

        # three items in queue: [task1, task2, None]
        task1 = core.ActiveTask('foo', start_process(dummy_worker, 5), None)
        task2 = core.ActiveTask('bar', start_process(dummy_worker, 3), None)
        for task in (task1, task2, None):
            pool.queue.put(task)
        self.assertIsNone(pool.reaper(timeout=1))
        self.assertTrue(pool.queue.empty())
        self.assertEqual(pool._forwards, [task2, task1])
        for task in (task1, task2):
            self.assertEqual(task.process.exitcode, 0)
            self.assertFalse(task.process.is_alive())

    def test_on_task_completed(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self, *restart_always):
                super().__init__(*restart_always)
                self._calls = []

            def should_restart(self, key):
                self._calls.append(('should_restart', key))
                return super().should_restart(key)

            def queue_task_for_restart(self, key):
                self._calls.append(('queue_task_for_restart', key))

        key1 = random_id()
        key2 = random_id()
        key3 = random_id()
        process1 = DummyProcess()
        process2 = DummyProcess()
        process3 = DummyProcess()
        task1 = core.ActiveTask(key1, process1, None)
        task2 = core.ActiveTask(key2, process2, None)
        task3 = core.ActiveTask(key3, process3, None)
        pool = MockedTaskPool(key1)
        self.assertEqual(pool.restart_always, {key1})
        pool.active_tasks[key1] = task1
        pool.active_tasks[key2] = task2
        pool.active_tasks[key3] = task3
        pool.tasks[key1] = 'foo'
        pool.tasks[key2] = 'bar'
        pool.tasks[key3] = 'baz'
        pool.restart_once.update([key2, key3])

        # When TaskPool.running is False, should_restart() will not be called:
        self.assertIsNone(pool.on_task_completed(task1))
        self.assertEqual(pool.active_tasks, {key2: task2, key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, [])
        self.assertEqual(process3._calls, [])

        self.assertIsNone(pool.on_task_completed(task2))
        self.assertEqual(pool.active_tasks, {key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, [])

        self.assertIsNone(pool.on_task_completed(task3))
        self.assertEqual(pool.active_tasks, {})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, ['join'])

        self.assertEqual(pool.restart_once, {key2, key3})
        self.assertEqual(pool._calls, [])

        # Now test when TaskPool.running is True:
        for process in [process1, process2, process3]:
            process._calls.clear()
        pool.active_tasks[key1] = task1
        pool.active_tasks[key2] = task2
        pool.active_tasks[key3] = task3
        pool.running = True

        self.assertIsNone(pool.on_task_completed(task1))
        self.assertEqual(pool.active_tasks, {key2: task2, key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, [])
        self.assertEqual(process3._calls, [])
        self.assertEqual(pool.restart_once, {key2, key3})
        self.assertEqual(pool._calls, [
            ('should_restart', key1),
            ('queue_task_for_restart', key1),
        ])

        pool._calls.clear()
        self.assertIsNone(pool.on_task_completed(task2))
        self.assertEqual(pool.active_tasks, {key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, [])
        self.assertEqual(pool.restart_once, {key3})
        self.assertEqual(pool._calls, [
            ('should_restart', key2),
            ('queue_task_for_restart', key2),
        ])

        pool._calls.clear()
        self.assertIsNone(pool.on_task_completed(task3))
        self.assertEqual(pool.active_tasks, {})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, ['join'])
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [
            ('should_restart', key3),
            ('queue_task_for_restart', key3),
        ])

        # Finally, test when TaskPool.should_restart() returns False:
        for process in [process1, process2, process3]:
            process._calls.clear()
        pool.active_tasks[key1] = task1
        pool.active_tasks[key2] = task2
        pool.active_tasks[key3] = task3
        pool._calls.clear()
        # key1: in restart_always, but not in tasks
        # key2: in restart_once, but not in tasks
        # key3: in tasks, but not in restart_always nor restart_once
        del pool.tasks[key1]
        del pool.tasks[key2]
        pool.restart_once.add(key2)
        self.assertEqual(pool.tasks, {key3: 'baz'})

        self.assertIsNone(pool.on_task_completed(task1))
        self.assertEqual(pool.active_tasks, {key2: task2, key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, [])
        self.assertEqual(process3._calls, [])
        self.assertEqual(pool.restart_once, {key2})
        self.assertEqual(pool._calls, [
            ('should_restart', key1),
        ])

        pool._calls.clear()
        self.assertIsNone(pool.on_task_completed(task2))
        self.assertEqual(pool.active_tasks, {key3: task3})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, [])
        self.assertEqual(pool.restart_once, {key2})
        self.assertEqual(pool._calls, [
            ('should_restart', key2),
        ])

        pool._calls.clear()
        self.assertIsNone(pool.on_task_completed(task3))
        self.assertEqual(pool.active_tasks, {})
        self.assertEqual(process1._calls, ['join'])
        self.assertEqual(process2._calls, ['join'])
        self.assertEqual(process3._calls, ['join'])
        self.assertEqual(pool.restart_once, {key2})
        self.assertEqual(pool._calls, [
            ('should_restart', key3),
        ])

    def test_should_restart(self):
        key1 = random_id()
        key2 = random_id()
        key3 = random_id()
        pool = core.TaskPool(key1)

        # Test when all keys are in tasks:
        pool.tasks.update({
            key1: 'foo',
            key2: 'bar',
            key3: 'baz',
        })
        self.assertIs(pool.should_restart(key1), True)
        self.assertIs(pool.should_restart(key2), False)
        pool.restart_once.add(key3)
        self.assertIs(pool.should_restart(key2), False)
        self.assertEqual(pool.restart_once, {key3})
        pool.restart_once.add(key2)
        self.assertIs(pool.should_restart(key2), True)
        self.assertEqual(pool.restart_once, {key3})
        self.assertIs(pool.should_restart(key3), True)
        self.assertEqual(pool.restart_once, set())
        self.assertIs(pool.should_restart(key3), False)
        self.assertEqual(pool.restart_once, set())
        self.assertIs(pool.should_restart(key1), True)

        # Test when tasks is empty:
        pool.tasks.clear()
        pool.restart_once.update([key2, key3])
        self.assertIs(pool.should_restart(key1), False)
        self.assertIs(pool.should_restart(key2), False)
        self.assertIs(pool.should_restart(key3), False)
        self.assertEqual(pool.restart_once, {key2, key3})

    def test_add_task(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self):
                super().__init__()
                self._calls = []

            def start_task(self, key):
                self._calls.append(key)

        pool = MockedTaskPool()

        def worker1(arg1, arg2):
            pass

        def worker2(arg1):
            pass

        # Test when running is False:
        key1 = random_id()
        self.assertIs(pool.add_task(key1, worker1, 'foo', 'bar'), True)
        self.assertEqual(set(pool.tasks), {key1})
        info1 = pool.tasks[key1]
        self.assertIsInstance(info1, core.TaskInfo)
        self.assertIs(info1.target, worker1)
        self.assertEqual(info1.args, ('foo', 'bar'))
        self.assertIs(pool.add_task(key1, worker1, 'foo', 'bar'), False)
        self.assertEqual(pool.tasks, {key1: info1})
        self.assertEqual(pool._calls, [])

        # Test when running is True:
        pool.running = True
        key2 = random_id()
        self.assertIs(pool.add_task(key2, worker2, 'baz'), True)
        self.assertEqual(set(pool.tasks), {key1, key2})
        info2 = pool.tasks[key2]
        self.assertIsInstance(info2, core.TaskInfo)
        self.assertIs(info2.target, worker2)
        self.assertEqual(info2.args, ('baz',))
        self.assertIs(pool.add_task(key2, worker2, 'baz'), False)
        self.assertEqual(pool.tasks, {key1: info1, key2: info2})
        self.assertEqual(pool._calls, [key2])

    def test_remove_task(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self):
                super().__init__()
                self._calls = []

            def stop_task(self, key):
                self._calls.append(key)

        pool = MockedTaskPool()
        key = random_id()

        # Key missing in tasks, should *not* call TaskPool.stop_task():
        self.assertIs(pool.remove_task(key), False)
        self.assertEqual(pool.tasks, {})
        self.assertEqual(pool.active_tasks, {})
        self.assertEqual(pool._calls, [])

        # Key in tasks, should call TaskPool.stop_task():
        pool.tasks[key] = 'foo'
        self.assertIs(pool.remove_task(key), True)
        self.assertEqual(pool.tasks, {})
        self.assertEqual(pool.active_tasks, {})
        self.assertEqual(pool._calls, [key])

    def test_start_task(self):
        pool = core.TaskPool()

        def worker(arg1, arg2):
            pass

        key = random_id()
        info = core.TaskInfo(worker, ('foo', 'bar'))
        pool.tasks[key] = info
        pool.running = True

        # Key in active_tasks:
        pool.active_tasks[key] = 'whatever'
        self.assertIs(pool.start_task(key), False)
        self.assertEqual(pool.tasks, {key: info})
        self.assertEqual(pool.active_tasks, {key: 'whatever'})
        self.assertTrue(pool.queue.empty())

        # Key *not* in active_tasks:
        pool.active_tasks.clear()
        self.assertIs(pool.start_task(key), True)
        self.assertEqual(pool.tasks, {key: info})
        self.assertEqual(set(pool.active_tasks), {key})
        result = pool.active_tasks[key]
        self.assertIsInstance(result, core.ActiveTask)
        self.assertEqual(result.key, key)
        self.assertIsInstance(result.process, multiprocessing.Process)
        in_q = pool.queue.get(timeout=1)
        self.assertIs(in_q, result)
        self.assertTrue(pool.queue.empty())

        # But should not start when TaskPool.running is False:
        pool.active_tasks.clear()
        pool.running = False
        self.assertIsNone(pool.start_task(key))
        self.assertEqual(pool.tasks, {key: info})
        self.assertEqual(pool.active_tasks, {})
        self.assertTrue(pool.queue.empty())

    def test_stop_task(self):
        pool = core.TaskPool()
        key = random_id()

        # Key *not* in active_tasks:
        self.assertIs(pool.stop_task(key), False)
        self.assertEqual(pool.active_tasks, {})

        # Key in active_tasks:
        process = DummyProcess()
        task = core.ActiveTask(key, process, None)
        pool.active_tasks[key] = task
        self.assertIs(pool.stop_task(key), True)
        self.assertEqual(pool.active_tasks, {key: task})
        self.assertIs(pool.active_tasks[key], task)
        self.assertEqual(process._calls, ['terminate'])

    def test_restart_task(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self, stop_result, *restart_always):
                super().__init__(*restart_always)
                self._calls = []
                self._stop_result = stop_result

            def stop_task(self, key):
                self._calls.append(('stop_task', key))
                return self._stop_result

            def start_task(self, key):
                self._calls.append(('start_task', key))

        key1 = random_id()
        key2 = random_id()

        # Should return None when running is False:
        pool = MockedTaskPool(True, key1)
        self.assertIs(pool._stop_result, True)
        self.assertEqual(pool.restart_always, {key1})
        self.assertIsNone(pool.restart_task(key1))
        self.assertIsNone(pool.restart_task(key2))
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [])

        # Should return 0 when key not in tasks:
        pool = MockedTaskPool(True, key1)
        pool.running = True
        self.assertIs(pool._stop_result, True)
        self.assertEqual(pool.restart_always, {key1})
        self.assertIs(pool.restart_task(key1), 0)
        self.assertIs(pool.restart_task(key2), 0)
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [])

        # First test when stop_task() returns True:
        pool = MockedTaskPool(True, key1)
        pool.running = True
        pool.tasks = {key1: 'stuff', key2: 'junk'}
        self.assertIs(pool._stop_result, True)
        self.assertEqual(pool.restart_always, {key1})
        self.assertIs(pool.restart_task(key1), True)
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [
            ('stop_task', key1),
        ])
        self.assertIs(pool.restart_task(key2), True)
        self.assertEqual(pool.restart_once, {key2})
        self.assertEqual(pool._calls, [
            ('stop_task', key1),
            ('stop_task', key2),
        ])

        # Now test when stop_task() returns False:
        pool = MockedTaskPool(False, key1)
        pool.running = True
        pool.tasks = {key1: 'stuff', key2: 'junk'}
        self.assertIs(pool._stop_result, False)
        self.assertEqual(pool.restart_always, {key1})
        self.assertIs(pool.restart_task(key1), False)
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [
            ('stop_task', key1),
            ('start_task', key1),
        ])
        self.assertIs(pool.restart_task(key2), False)
        self.assertEqual(pool.restart_once, set())
        self.assertEqual(pool._calls, [
            ('stop_task', key1),
            ('start_task', key1),
            ('stop_task', key2),
            ('start_task', key2),
        ])

    def test_start(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self):
                super().__init__()
                self._calls = []
                
            def start_reaper(self):
                self._calls.append('start_reaper')

            def start_task(self, key):
                self._calls.append(key)

        pool = MockedTaskPool()
        key1 = random_id()
        key2 = random_id()
        key3 = random_id()
        pool.tasks.update({
            key1: 'foo',
            key2: 'bar',
            key3: 'baz',
        })

        # Make sure no action is taken when running is True:
        pool.running = True
        self.assertIs(pool.start(), False)
        self.assertEqual(pool._calls, [])
        self.assertEqual(pool.tasks, {key1: 'foo', key2: 'bar', key3: 'baz'})
        self.assertIs(pool.running, True)

        # Now test when running is False:
        pool.running = False
        self.assertIs(pool.start(), True)
        self.assertEqual(pool._calls,
            ['start_reaper'] + sorted([key1, key2, key3])
        )
        self.assertEqual(pool.tasks, {key1: 'foo', key2: 'bar', key3: 'baz'})
        self.assertIs(pool.running, True)

    def test_stop(self):
        class MockedTaskPool(core.TaskPool):
            def __init__(self):
                super().__init__()
                self._calls = []

            def stop_task(self, key):
                self._calls.append(key)

        pool = MockedTaskPool()
        key1 = random_id()
        key2 = random_id()
        key3 = random_id()
        pool.active_tasks.update({
            key1: 'foo',
            key2: 'bar',
            key3: 'baz',
        })

        # Make sure no action is taken when running is False:
        self.assertIs(pool.stop(), False)
        self.assertEqual(pool._calls, [])
        self.assertEqual(pool.active_tasks, {key1: 'foo', key2: 'bar', key3: 'baz'})
        self.assertIs(pool.running, False)

        # Now test when running is True:
        pool.running = True
        self.assertIs(pool.stop(), True)
        self.assertEqual(pool._calls, sorted([key1, key2, key3]))
        self.assertEqual(pool.active_tasks, {key1: 'foo', key2: 'bar', key3: 'baz'})
        self.assertIs(pool.running, False)


class TestTaskMaster(TestCase):
    def test_init(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        self.assertIs(master.env, env)
        self.assertIs(master.ssl_config, ssl_config)
        self.assertIsInstance(master.pool, core.TaskPool)
        self.assertEqual(master.pool.tasks, {})
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_add_filestore_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        fs = TempFileStore()
        self.assertIsNone(master.add_filestore_task(fs))
        self.assertEqual(master.pool.tasks, {
            ('filestore', fs.parentdir): core.TaskInfo(
                core.filestore_worker,
                (env, fs.parentdir, fs.id),
            ),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_remove_filestore_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        fs = TempFileStore()
        key = ('filestore', fs.parentdir)
        self.assertIsNone(master.remove_filestore_task(fs))
        self.assertEqual(master.pool.tasks, {})
        master.pool.tasks[key] = core.TaskInfo(
            core.filestore_worker,
            (env, fs.parentdir, fs.id),
        )
        self.assertIsNone(master.remove_filestore_task(fs))
        self.assertEqual(master.pool.tasks, {})
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_restart_filestore_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        fs = TempFileStore()
        self.assertIsNone(master.restart_filestore_task(fs))
        self.assertEqual(master.pool.tasks, {})
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIsNone(master.add_filestore_task(fs))
        self.assertIsNone(master.restart_filestore_task(fs))
        self.assertEqual(master.pool.tasks, {
            ('filestore', fs.parentdir): core.TaskInfo(
                core.filestore_worker,
                (env, fs.parentdir, fs.id),
            ),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_add_vigilance_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        self.assertIsNone(master.add_vigilance_task())
        self.assertEqual(master.pool.tasks, {
            ('vigilance',): core.TaskInfo(
                core.vigilance_worker,
                (env, ssl_config),
            ),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_restart_vigilance_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        self.assertIsNone(master.restart_vigilance_task())
        self.assertEqual(master.pool.tasks, {})
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIsNone(master.add_vigilance_task())
        self.assertIsNone(master.restart_vigilance_task())
        self.assertEqual(master.pool.tasks, {
            ('vigilance',): core.TaskInfo(
                core.vigilance_worker,
                (env, ssl_config),
            ),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_add_downgrade_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        self.assertIsNone(master.add_downgrade_task())
        self.assertEqual(master.pool.tasks, {
            ('downgrade',): core.TaskInfo(core.downgrade_worker, (env, ssl_config)),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)

    def test_restart_downgrade_task(self):
        env = random_id()
        ssl_config = random_id()
        master = core.TaskMaster(env, ssl_config)
        self.assertIsNone(master.restart_downgrade_task())
        self.assertEqual(master.pool.tasks, {})
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIsNone(master.add_downgrade_task())
        self.assertIsNone(master.restart_downgrade_task())
        self.assertEqual(master.pool.tasks, {
            ('downgrade',): core.TaskInfo(core.downgrade_worker, (env, ssl_config)),
        })
        self.assertEqual(master.pool.active_tasks, {})
        self.assertIs(master.pool.running, False)


class TestCore(CouchTestCase):
    def create(self):
        self.machine_id = random_id(30)
        self.user_id = random_id(30)
        self.machine = {'_id': self.machine_id}
        self.user = {'_id': self.user_id}
        self.ssl_config = random_id()
        return core.Core(self.env, self.machine, self.user, self.ssl_config)

    def test_init(self):
        machine_id = random_id(30)
        user_id = random_id(30)
        machine = {'_id': machine_id}
        user = {'_id': user_id}
        ssl_config = random_id()

        # None of _local/dmedia, user, nor machine exist:
        start = time.time()
        inst = core.Core(self.env, machine, user, ssl_config)
        end = time.time()
        self.assertIs(inst.env, self.env)
        self.assertEqual(inst.env['machine_id'], machine_id)
        self.assertEqual(inst.env['user_id'], user_id)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, 'dmedia-1')
        self.assertIsInstance(inst.log_db, microfiber.Database)
        self.assertEqual(inst.log_db.name, 'log-1')
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIsInstance(inst.ms, MetaStore)
        self.assertIs(inst.ms.db, inst.db)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertEqual(inst.peers, {})
        self.assertIsInstance(inst.task_master, core.TaskMaster)
        self.assertEqual(inst.ssl_config, ssl_config)
        self.assertEqual(inst.db.get('_local/dmedia'), {
            '_id': '_local/dmedia',
            '_rev': '0-1',
            'machine_id': machine_id,
            'user_id': user_id,
        })
        self.assertIs(inst.machine, machine)
        self.assertEqual(inst.db.get(machine_id), machine)
        self.assertEqual(inst.machine['_rev'][:2], '1-')
        mtime = inst.machine['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(inst.machine, {
            '_id': machine_id,
            '_rev': inst.machine['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {},
        })
        self.assertIs(inst.user, user)
        self.assertEqual(inst.db.get(user_id), user)
        self.assertEqual(inst.user['_rev'][:2], '1-')

        # All of _local/dmedia, user, and machine exist:
        start = time.time()
        inst = core.Core(self.env, machine, user, ssl_config)
        end = time.time()
        self.assertIs(inst.env, self.env)
        self.assertEqual(inst.env['machine_id'], machine_id)
        self.assertEqual(inst.env['user_id'], user_id)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, 'dmedia-1')
        self.assertIsInstance(inst.log_db, microfiber.Database)
        self.assertEqual(inst.log_db.name, 'log-1')
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIsInstance(inst.ms, MetaStore)
        self.assertIs(inst.ms.db, inst.db)
        self.assertIsInstance(inst.stores, LocalStores)
        self.assertEqual(inst.peers, {})
        self.assertIsInstance(inst.task_master, core.TaskMaster)
        self.assertIs(inst.ssl_config, ssl_config)
        self.assertEqual(inst.db.get('_local/dmedia'), {
            '_id': '_local/dmedia',
            '_rev': '0-2',
            'machine_id': machine_id,
            'user_id': user_id,
        })
        self.assertIsNot(inst.machine, machine)
        self.assertEqual(inst.db.get(machine_id), inst.machine)
        self.assertEqual(inst.machine['_rev'][:2], '2-')
        mtime = inst.machine['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(inst.machine, {
            '_id': machine_id,
            '_rev': inst.machine['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {},
        })
        self.assertIsNot(inst.user, user)
        self.assertEqual(inst.db.get(user_id), inst.user)
        self.assertEqual(inst.user['_rev'][:2], '2-')

    def test_update_machine(self):
        inst = self.create()
        start = time.time()
        inst.update_machine()
        end = time.time()
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, {
            '_id': self.machine_id,
            '_rev': doc['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {},
        })
        self.assertEqual(doc, inst.machine)

        # Now test when there has been a conflicting change to the machine doc,
        # to make sure Database.update() is used, not Database.save():
        del doc['mtime']
        doc['stores'] = {
            random_id(): {'parentdir': '/media/foo', 'copies': 1},
        }
        doc['peers'] = {
            random_id(30): {'url': 'https://192.168.17.18'},
        }
        inst.db.save(doc)
        start = time.time()
        inst.update_machine()
        end = time.time()
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '4-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, {
            '_id': self.machine_id,
            '_rev': doc['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {},
        })
        self.assertEqual(doc, inst.machine)

    def test_add_peer(self):
        inst = self.create()
        id1 = random_id(30)
        info1 = {
            'host': 'jderose-Gazelle-Professional',
            'url': 'https://192.168.1.139:41326/',
            'user': 'jderose',
            'version': '13.05.0'
        }
        id2 = random_id(30)
        info2 = {'url': 'https://192.168.1.77:9872/'}

        # id1 is not yet a peer:
        start = time.time()
        self.assertIsNone(inst.add_peer(id1, info1))
        end = time.time()
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, {
            '_id': self.machine_id,
            '_rev': doc['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {
                id1: info1,
            }
        })
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

        # id2 is not yet a peer:
        start = time.time()
        self.assertIsNone(inst.add_peer(id2, info2))
        end = time.time()
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '3-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, {
            '_id': self.machine_id,
            '_rev': doc['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {
                id1: info1,
                id2: info2,
            }
        })
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

        # id1 is already a peer, make sure info is replaced
        new1 = {'url': random_id()}
        start = time.time()
        self.assertIsNone(inst.add_peer(id1, new1))
        end = time.time()
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '4-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, {
            '_id': self.machine_id,
            '_rev': doc['_rev'],
            'mtime': mtime,
            'stores': {},
            'peers': {
                id1: new1,
                id2: info2,
            }
        })
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

    def test_remove_peer(self):
        inst = self.create()
        id1 = random_id(30)
        id2 = random_id(30)
        info1 = {'url': random_id()}
        info2 = {'url': random_id()}
        inst.peers = {id1: info1, id2: info2}

        # Test with a peer_id that doesn't exist:
        nope = random_id(30)
        self.assertIs(inst.remove_peer(nope), False)
        self.assertEqual(inst.peers, {id1: info1, id2: info2})
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '1-')
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], {})

        # id1 is present
        start = time.time()
        self.assertIs(inst.remove_peer(id1), True)
        end = time.time()
        self.assertEqual(inst.peers, {id2: info2})
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

        # id1 is missing
        self.assertIs(inst.remove_peer(id1), False)
        self.assertEqual(inst.peers, {id2: info2})
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

        # id2 is present
        start = time.time()
        self.assertIs(inst.remove_peer(id2), True)
        end = time.time()
        self.assertEqual(inst.peers, {})
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '3-')
        mtime = doc['mtime']
        self.assertIsInstance(mtime, int)
        self.assertTrue(
            (start - 1) <= mtime <= (end + 1)
        )
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

        # id2 is missing
        self.assertIs(inst.remove_peer(id2), False)
        self.assertEqual(inst.peers, {})
        doc = inst.db.get(self.machine_id)
        self.assertEqual(doc['_rev'][:2], '3-')
        self.assertEqual(doc, inst.machine)
        self.assertEqual(doc['peers'], inst.peers)

    def test_create_filestore(self):
        inst = self.create()

        # Test when a FileStore already exists
        tmp = TempDir()
        fs = FileStore.create(tmp.dir)
        with self.assertRaises(Exception) as cm:
            inst.create_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'Already contains a FileStore: {!r}'.format(tmp.dir)
        )

        # Test when no FileStore yet exists in the parentdir:
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        self.assertIsInstance(fs, filestore.FileStore)
        self.assertEqual(fs.parentdir, tmp.dir)
        self.assertEqual(fs.copies, 1)
        self.assertIs(inst.stores.by_id(fs.id), fs)
        self.assertIs(inst.stores.by_parentdir(fs.parentdir), fs)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['stores'], {
            fs.id: {'parentdir': fs.parentdir, 'copies': 1},
        })
        self.assertEqual(inst.machine['_rev'][:2], '2-')

        # Make sure we can disconnect a store that was just created
        inst.disconnect_filestore(fs.parentdir)
        self.assertEqual(inst.db.get(self.machine_id), inst.machine)
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['_rev'][:2], '3-')

    def test_connect_filestore(self):
        tmp = TempDir()
        basedir = tmp.join(filestore.DOTNAME)
        inst = self.create()

        # Test when .dmedia/ doesn't exist
        with self.assertRaises(FileNotFoundError) as cm:
            inst.connect_filestore(tmp.dir, random_id())
        self.assertEqual(cm.exception.filename, basedir)

        # Test when FileStore has been initialized:
        fs = FileStore.create(tmp.dir)
        fs_a = inst.connect_filestore(tmp.dir)
        self.assertIsInstance(fs_a, FileStore)
        self.assertEqual(fs_a.parentdir, tmp.dir)
        self.assertEqual(fs_a.id, fs.id)
        self.assertEqual(fs_a.doc, fs.doc)
        doc = inst.db.get(fs.id)
        _rev = doc.pop('_rev')
        self.assertTrue(_rev.startswith('1-'))
        self.assertEqual(doc, fs.doc)

        # Test when store is already connected:
        with self.assertRaises(Exception) as cm:
            inst.connect_filestore(tmp.dir)
        self.assertEqual(
            str(cm.exception),
            'already have ID {!r}'.format(fs.id)
        )

        # Test when expected_id is provided and does *not* match:
        bad_id = random_id()
        with self.assertRaises(ValueError) as cm:
            inst.connect_filestore(tmp.dir, expected_id=bad_id)
        self.assertEqual(str(cm.exception),
            "doc['_id']: expected {!r}; got {!r}".format(bad_id, fs.id)
        )

        # Test when expected_id is provided and matches:
        inst = self.create()
        fs_b = inst.connect_filestore(tmp.dir, expected_id=fs.id)
        self.assertIsInstance(fs_b, FileStore)
        self.assertEqual(fs_b.parentdir, tmp.dir)
        self.assertEqual(fs_b.id, fs.id)
        self.assertEqual(fs_b.doc, fs.doc)

        # Connect another store
        tmp2 = TempDir()
        fs2 = FileStore.create(tmp2.dir)
        fs2_a = inst.connect_filestore(tmp2.dir)
        self.assertIsInstance(fs2_a, FileStore)
        self.assertEqual(fs2_a.parentdir, tmp2.dir)
        self.assertEqual(fs2_a.id, fs2.id)
        self.assertEqual(fs2_a.copies, 1)
        self.assertIs(inst.stores.by_id(fs2.id), fs2_a)
        self.assertIs(inst.stores.by_parentdir(fs2.parentdir), fs2_a)

        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs.id: {'parentdir': fs.parentdir, 'copies': 1},
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # Test when migration is needed
        tmp = TempDir()
        m = Migration(tmp.dir)
        old = m.build_v0_simulation()
        fs = inst.connect_filestore(tmp.dir)
        self.assertEqual(b32_to_db32(old['_id']), fs.id)

    def test_disconnect_filestore(self):
        inst = self.create()
        fs1 = TempFileStore()
        fs2 = TempFileStore()

        # Test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

        # Connect both, then disconnect one by one
        inst.connect_filestore(fs1.parentdir, fs1.id)
        inst.connect_filestore(fs2.parentdir, fs2.id)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs1.id: {'parentdir': fs1.parentdir, 'copies': 1},
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '3-')

        # Disconnect fs1
        inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], 
            {
                fs2.id: {'parentdir': fs2.parentdir, 'copies': 1},
            },
        )
        self.assertEqual(inst.machine['_rev'][:2], '4-')

        # Disconnect fs2
        inst.disconnect_filestore(fs2.parentdir)
        self.assertEqual(inst.machine, inst.db.get(self.machine_id))
        self.assertEqual(inst.machine['stores'], {})
        self.assertEqual(inst.machine['_rev'][:2], '5-')

        # Again test when not connected:
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs2.parentdir)
        self.assertEqual(str(cm.exception), repr(fs2.parentdir))
        with self.assertRaises(KeyError) as cm:
            inst.disconnect_filestore(fs1.parentdir)
        self.assertEqual(str(cm.exception), repr(fs1.parentdir))

    def test_resolve(self):
        inst = self.create()

        bad_id1 = random_id(25)  # Wrong length
        self.assertEqual(inst.resolve(bad_id1),
            (bad_id1, 3, '')
        )
        bad_id2 = random_id(30)[:-1] + '0'  # Invalid letter
        self.assertEqual(inst.resolve(bad_id2),
            (bad_id2, 3, '')
        )

        unknown_id = random_id(30)
        self.assertEqual(inst.resolve(unknown_id),
            (unknown_id, 2, '')
        )

        good_id = random_id(30)
        doc = {
            '_id': good_id,
            'stored': {
                random_id(): {},
                random_id(): {},
            },
        }
        inst.db.save(doc)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        doc['stored'][fs.id] = {}
        inst.db.save(doc)
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )

        filename = fs.path(good_id)
        open(filename, 'xb').close()
        self.assertEqual(inst.resolve(good_id),
            (good_id, 1, '')
        )
        open(filename, 'wb').write(b'non empty')
        self.assertEqual(inst.resolve(good_id),
            (good_id, 0, filename)
        )

    def test_resolve_many(self):
        inst = self.create()
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)

        bad_id1 = random_id(25)  # Wrong length
        bad_id2 = random_id(30)[:-1] + '0'  # Invalid letter
        unknown_id = random_id(30)
        nonlocal_id = random_id(30)
        missing_id = random_id(30)
        empty_id = random_id(30)
        empty_filename = fs.path(empty_id)
        open(empty_filename, 'xb').close()
        good_id = random_id(30)
        good_filename = fs.path(good_id)
        open(good_filename, 'xb').write(b'non empty')

        doc1 = {
            '_id': nonlocal_id,
            'stored': {
                random_id(): {},
                random_id(): {},
            }
        }
        doc2 = {
            '_id': missing_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        doc3 = {
            '_id': empty_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        doc4 = {
            '_id': good_id,
            'stored': {
                fs.id: {},
                random_id(): {},
            }
        }
        inst.db.save_many([doc1, doc2, doc3, doc4])

        ids = [bad_id1, bad_id2, unknown_id, nonlocal_id, missing_id, empty_id, good_id]
        self.assertEqual(inst.resolve_many(ids),
            [
                (bad_id1, 3, ''),
                (bad_id2, 3, ''),
                (unknown_id, 2, ''),
                (nonlocal_id, 1, ''),
                (missing_id, 1, ''),
                (empty_id, 1, ''),
                (good_id, 0, good_filename)
            ]
        )

    def test_allocate_tmp(self):
        inst = self.create()

        with self.assertRaises(Exception) as cm:        
            inst.allocate_tmp()
        self.assertEqual(str(cm.exception), 'no file-stores present')

        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        name = inst.allocate_tmp()
        self.assertEqual(path.dirname(name), fs.tmp)
        self.assertEqual(path.getsize(name), 0)

    def test_hash_and_move(self):
        inst = self.create()
        tmp = TempDir()
        fs = inst.create_filestore(tmp.dir)
        tmp_fp = fs.allocate_tmp()
        ch = write_random(tmp_fp)

        self.assertEqual(
            inst.hash_and_move(tmp_fp.name, 'render'),
            {
                'file_id': ch.id,
                'file_path': fs.path(ch.id),
            }
        )

        doc = inst.db.get(ch.id)
        rev = doc.pop('_rev')
        self.assertTrue(rev.startswith('1-'))
        att = doc.pop('_attachments')
        self.assertIsInstance(att, dict)
        self.assertEqual(set(att), set(['leaf_hashes']))
        ts = doc.pop('time')
        self.assertIsInstance(ts, float)
        self.assertLessEqual(ts, time.time())
        self.assertEqual(doc,
            {
                '_id': ch.id,
                'type': 'dmedia/file',
                'atime': int(ts),
                'bytes': ch.file_size,
                'origin': 'render',
                'stored': {
                    fs.id: {
                        'copies': 1,
                        'mtime': get_mtime(fs, ch.id),
                    },
                },
            }
        )
