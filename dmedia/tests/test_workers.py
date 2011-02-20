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
Unit tests for `dmedia.workers` module.
"""

from unittest import TestCase
from os import path
import time
import multiprocessing
import multiprocessing.queues
from multiprocessing import current_process
import threading

import couchdb

from dmedia import workers
from .helpers import raises, DummyQueue, DummyCallback
from .couch import CouchCase


class test_functions(TestCase):
    def setUp(self):
        workers._workers.clear()

    def test_register(self):
        f = workers.register

        # Test with an instance rather than class
        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            'worker: must be subclass of %r; got %r' % (workers.Worker, 17)
        )
        self.assertEqual(workers._workers, {})

        # Test with wrong subclass
        class foo(object):
            pass
        e = raises(TypeError, f, foo)
        self.assertEqual(
            str(e),
            'worker: must be subclass of %r; got %r' % (workers.Worker, foo)
        )
        self.assertEqual(workers._workers, {})

        # Test a correct Worker
        class import_files(workers.Worker):
            pass
        f(import_files)
        self.assertEqual(
            workers._workers,
            {'import_files': import_files}
        )
        orig_import_files = import_files

        # Test that another worker with same name cannot be registered
        class import_files(workers.Worker):
            pass
        e = raises(ValueError, f, import_files)
        msg = 'cannot register %r, worker with name %r already registered' % (
            import_files, 'import_files'
        )
        self.assertEqual(str(e), msg)
        self.assertEqual(
            workers._workers,
            {'import_files': orig_import_files}
        )

        # Test another correct Worker
        class render_proxy(workers.Worker):
            pass
        f(render_proxy)
        self.assertEqual(
            workers._workers,
            {'import_files': orig_import_files, 'render_proxy': render_proxy}
        )

    def test_exception_name(self):
        f = workers.exception_name

        self.assertEqual(f(ValueError), 'ValueError')
        self.assertEqual(f(ValueError('foo')), 'ValueError')

        class Custom(Exception):
            pass

        self.assertEqual(f(Custom), 'Custom')
        self.assertEqual(f(Custom('bar')), 'Custom')

    def test_dispatch(self):
        f = workers.dispatch
        pid = current_process().pid

        # Test with unknown worker name
        q = DummyQueue()
        env = {'foo': 'bar'}
        f('ImportFiles', env, q, 'the key', ('foo', 'bar'))

        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='error',
                    args=('the key', 'KeyError', "'ImportFiles'"),
                    worker='ImportFiles',
                    pid=pid,
                ),
                dict(
                    signal='terminate',
                    args=('the key',),
                    worker='ImportFiles',
                    pid=pid,
                ),
            ]
        )

        class ImportFiles(workers.Worker):
            def run(self):
                self.emit('word', *self.args)

        workers.register(ImportFiles)

        q = DummyQueue()
        env = {'foo': 'bar'}
        f('ImportFiles', env, q, 'the key', ('hello', 'world'))
        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='word',
                    args=('the key', 'hello', 'world'),
                    worker=('ImportFiles'),
                    pid=pid,
                ),
                dict(
                    signal='terminate',
                    args=('the key',),
                    worker='ImportFiles',
                    pid=pid,
                ),
            ]
        )


class test_Worker(TestCase):
    klass = workers.Worker

    def test_init(self):
        env = {'foo': 'bar'}
        q = DummyQueue()
        key = 'the key'
        args = ('foo', 'bar')
        inst = self.klass(env, q, key, args)
        self.assertTrue(inst.env is env)
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.key is key)
        self.assertTrue(inst.args is args)
        self.assertEqual(inst.pid, current_process().pid)
        self.assertEqual(inst.name, 'Worker')

    def test_emit(self):
        env = {'foo': 'bar'}
        q = DummyQueue()
        args = ('foo', 'bar')
        inst = self.klass(env, q, 'akey', args)
        pid = current_process().pid

        self.assertEqual(q.messages, [])

        inst.emit('SomeSignal')
        one = dict(
            worker='Worker',
            pid=pid,
            signal='SomeSignal',
            args=('akey',),
        )
        self.assertEqual(q.messages, [one])

        inst.emit('AnotherSignal', 'this', 'time', 'with', 'args')
        two = dict(
            worker='Worker',
            pid=pid,
            signal='AnotherSignal',
            args=('akey', 'this', 'time', 'with', 'args')
        )
        self.assertEqual(q.messages, [one, two])

        inst.emit('OneMore', 'stuff')
        three = dict(
            worker='Worker',
            pid=pid,
            signal='OneMore',
            args=('akey', 'stuff'),
        )
        self.assertEqual(q.messages, [one, two, three])

    def test_run(self):
        env = {'foo': 'bar'}
        q = DummyQueue()
        args = ('foo', 'bar')
        pid = current_process().pid

        class do_something(self.klass):
            def execute(self, one, two):
                self.emit('Hello', '%s and %s' % (one, two))

        inst = do_something(env, q, 'key', args)
        inst.run()
        self.assertEqual(q.messages[0],
            dict(
                worker='do_something',
                pid=pid,
                signal='Hello',
                args=('key', 'foo and bar'),
            )
        )

    def test_execute(self):
        env = {'foo': 'bar'}
        q = DummyQueue()
        args = ('foo', 'bar')
        inst = self.klass(env, q, 'key', args)

        e = raises(NotImplementedError, inst.execute)
        self.assertEqual(str(e), 'Worker.execute()')

        class do_something(self.klass):
            pass
        inst = do_something(env, q, 'key', args)
        e = raises(NotImplementedError, inst.execute)
        self.assertEqual(str(e), 'do_something.execute()')


class test_CouchWorker(CouchCase):
    klass = workers.CouchWorker

    def test_init(self):
        q = DummyQueue()
        key = 'a key'
        args = ('some', 'args')
        inst = self.klass(self.env, q, key, args)
        self.assertTrue(inst.env is self.env)
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.key is key)
        self.assertTrue(inst.args is args)
        self.assertTrue(isinstance(inst.server, couchdb.Server))
        self.assertTrue(isinstance(inst.db, couchdb.Database))





def infinite():
    while True:
        time.sleep(1)


def infinite_process():
    p = multiprocessing.Process(target=infinite)
    p.daemon = True
    p.start()
    assert p.is_alive()
    return p


class ExampleWorker(workers.Worker):
    def execute(self, *args):
        if self.env.get('infinite', True):
            infinite()
        else:
            time.sleep(1)


class test_Manager(TestCase):
    klass = workers.Manager

    def setUp(self):
        workers._workers.clear()
        workers.register(ExampleWorker)

    def test_init(self):
        env = {'foo': 'bar'}
        # Test with non-callable callback:
        e = raises(TypeError, self.klass, env, 'foo')
        self.assertEqual(str(e), "callback must be callable; got 'foo'")

        # Test that callback default is None:
        inst = self.klass(env)
        self.assertTrue(inst.env is env)
        self.assertTrue(inst._callback is None)

        # Test with a callable:
        def foo():
            pass
        inst = self.klass(env, callback=foo)
        self.assertTrue(inst.env is env)
        self.assertTrue(inst._callback is foo)
        self.assertTrue(inst._running is False)
        self.assertEqual(inst._workers, {})
        self.assertTrue(isinstance(inst._q, multiprocessing.queues.Queue))
        self.assertTrue(inst._thread is None)

    def test_process_message(self):
        class Example(self.klass):
            _call = None
            def on_stuff(self, arg1, arg2):
                assert self._call is None
                self._call = arg1 + arg2

        env = {'foo': 'bar'}
        inst = Example(env = {'foo': 'bar'})
        msg = dict(signal='stuff', args=('foo', 'bar'))
        inst._process_message(msg)
        self.assertEqual(inst._call, 'foobar')

        msg = dict(signal='nope', args=('foo', 'bar'))
        e = raises(AttributeError, inst._process_message, msg)
        self.assertEqual(str(e), "'Example' object has no attribute 'on_nope'")

    def test_on_terminate(self):
        env = {'foo': 'bar'}
        inst = self.klass(env)
        e = raises(KeyError, inst.on_terminate, 'foo')
        p = multiprocessing.Process(target=time.sleep, args=(1,))
        p.daemon = True
        inst._workers['foo'] = p
        p.start()
        self.assertTrue(p.is_alive() is True)
        inst.on_terminate('foo')
        self.assertTrue(p.is_alive() is False)
        self.assertEqual(inst._workers, {})

    def test_start(self):
        env = {'foo': 'bar'}
        inst = self.klass(env)

        # Test that start() returns False when already running:
        inst._running = True
        self.assertTrue(inst.start() is False)
        self.assertTrue(inst._thread is None)

        # Start the Manager:
        inst._running = False
        self.assertTrue(inst.start() is True)
        self.assertTrue(inst._running is True)
        self.assertTrue(isinstance(inst._thread, threading.Thread))
        self.assertTrue(inst._thread.daemon is True)
        self.assertTrue(inst._thread.is_alive() is True)

        # Shutdown thread:
        inst._running = False
        inst._thread.join()

    def test_kill(self):
        env = {'foo': 'bar'}
        inst = self.klass(env)

        # Test that kill() returns False when not running:
        self.assertTrue(inst.kill() is False)

        # Test with live processes:
        foo = infinite_process()
        bar = infinite_process()
        baz = infinite_process()
        inst._workers.update(dict(foo=foo, bar=bar, baz=baz))
        self.assertTrue(inst.start() is True)
        self.assertTrue(inst._thread.is_alive())

        self.assertTrue(inst.kill() is True)
        self.assertFalse(inst._thread.is_alive())
        self.assertFalse(foo.is_alive())
        self.assertFalse(bar.is_alive())
        self.assertFalse(baz.is_alive())
        self.assertEqual(inst._workers, {})

    def test_kill_job(self):
        env = {'foo': 'bar'}
        inst = self.klass(env)

        # Test that kill_job() returns False when no such job exists:
        self.assertTrue(inst.kill_job('foo') is False)

        # Test with live processes:
        foo = infinite_process()
        inst._workers['foo'] = foo
        self.assertTrue(inst.kill_job('foo') is True)
        self.assertFalse(foo.is_alive())
        self.assertEqual(inst._workers, {})

        # Again test that kill_job() returns False when no such job exists:
        self.assertTrue(inst.kill_job('foo') is False)

    def test_start_job(self):
        env = {'foo': 'bar'}
        inst = self.klass(env)

        # Test that False is returned when key already exists:
        inst._workers['foo'] = 'bar'
        self.assertTrue(inst.start_job('ExampleWorker', 'foo') is False)

        # Test creating a process with no args
        inst._workers.clear()
        self.assertTrue(inst.start_job('ExampleWorker', 'foo') is True)
        self.assertEqual(list(inst._workers), ['foo'])
        p = inst._workers['foo']
        self.assertTrue(isinstance(p, multiprocessing.Process))
        self.assertTrue(p.daemon)
        self.assertTrue(p.is_alive())
        self.assertEqual(
            p._args,
            ('ExampleWorker', inst.env, inst._q, 'foo', tuple())
        )
        self.assertEqual(p._kwargs, {})
        p.terminate()
        p.join()

        # Test creating a process *with* args
        self.assertTrue(
            inst.start_job('ExampleWorker', 'bar', 'some', 'args') is True
        )
        self.assertEqual(sorted(inst._workers), ['bar', 'foo'])
        p = inst._workers['bar']
        self.assertTrue(isinstance(p, multiprocessing.Process))
        self.assertTrue(p.daemon)
        self.assertTrue(p.is_alive())
        self.assertEqual(
            p._args,
            ('ExampleWorker', inst.env, inst._q, 'bar', ('some', 'args'))
        )
        self.assertEqual(p._kwargs, {})
        p.terminate()
        p.join()


    def test_emit(self):
        env = {'foo': 'bar'}
        # Test with no callback
        inst = self.klass(env)
        inst.emit('ImportStarted', 'foo', 'bar')

        callback = DummyCallback()
        inst = self.klass(env, callback)
        inst.emit('ImportStarted', 'foo', 'bar')
        inst.emit('NoArgs')
        inst.emit('OneArg', 'baz')
        self.assertEqual(
            callback.messages,
            [
                ('ImportStarted', ('foo', 'bar')),
                ('NoArgs', tuple()),
                ('OneArg', ('baz',)),
            ]
        )


class test_CouchManager(CouchCase):
    klass = workers.CouchManager

    def test_init(self):
        inst = self.klass(self.env)
        self.assertTrue(inst.env is self.env)
        self.assertTrue(inst._callback is None)
        self.assertTrue(isinstance(inst.server, couchdb.Server))
        self.assertTrue(isinstance(inst.db, couchdb.Database))

        def func():
            pass
        inst = self.klass(self.env, func)
        self.assertTrue(inst.env is self.env)
        self.assertTrue(inst._callback, func)
        self.assertTrue(isinstance(inst.server, couchdb.Server))
        self.assertTrue(isinstance(inst.db, couchdb.Database))
