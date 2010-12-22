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
Unit tests for `dmedialib.workers` module.
"""

from os import path
from unittest import TestCase
import multiprocessing
import multiprocessing.queues
from multiprocessing import current_process
import threading
from dmedialib import workers
from .helpers import raises, DummyQueue


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
        f('import_files', q, ('foo', 'bar'))

        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='Error',
                    args=('KeyError', "'import_files'"),
                    worker='import_files',
                    pid=pid,
                    worker_args=('foo', 'bar'),
                ),
                dict(
                    signal='_terminate',
                    args=('foo', 'bar'),
                    worker='import_files',
                    pid=pid,
                ),
            ]
        )

        class import_files(workers.Worker):
            def run(self):
                if self.dummy:
                    self.emit('Dummy', *self.args)
                else:
                    self.emit('Smarty', *self.args)

        workers.register(import_files)

        # Test that default is dummy=False
        q = DummyQueue()
        f('import_files', q, ('hello', 'world'))
        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='Smarty',
                    args=('hello', 'world'),
                    worker=('import_files'),
                    pid=pid,
                ),
                dict(
                    signal='_terminate',
                    args=('hello', 'world'),
                    worker='import_files',
                    pid=pid,
                ),
            ]
        )

        # Test with dummy=False
        q = DummyQueue()
        f('import_files', q, ('hello', 'world'), False)
        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='Smarty',
                    args=('hello', 'world'),
                    worker=('import_files'),
                    pid=pid,
                ),
                dict(
                    signal='_terminate',
                    args=('hello', 'world'),
                    worker='import_files',
                    pid=pid,
                ),
            ]
        )

        # Test with dummy=True
        q = DummyQueue()
        f('import_files', q, ('hello', 'world'), True)
        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='Dummy',
                    args=('hello', 'world'),
                    worker=('import_files'),
                    pid=pid,
                ),
                dict(
                    signal='_terminate',
                    args=('hello', 'world'),
                    worker='import_files',
                    pid=pid,
                ),
            ]
        )


class test_Worker(TestCase):
    klass = workers.Worker

    def test_init(self):
        q = DummyQueue()
        args = ('foo', 'bar')
        inst = self.klass(q, args)
        self.assertTrue(inst.q is q)
        self.assertTrue(inst.args is args)
        self.assertTrue(inst.dummy is False)
        self.assertEqual(inst.pid, current_process().pid)
        self.assertEqual(inst.name, 'Worker')

        # Test with dummy=True, dummy=False
        inst = self.klass(q, args, dummy=True)
        self.assertTrue(inst.dummy is True)
        inst = self.klass(q, args, dummy=False)
        self.assertTrue(inst.dummy is False)

    def test_emit(self):
        q = DummyQueue()
        args = ('foo', 'bar')
        inst = self.klass(q, args)
        pid = current_process().pid

        self.assertEqual(q.messages, [])

        inst.emit('SomeSignal')
        one = dict(
            worker='Worker',
            pid=pid,
            signal='SomeSignal',
            args=tuple()
        )
        self.assertEqual(q.messages, [one])

        inst.emit('AnotherSignal', 'this', 'time', 'with', 'args')
        two = dict(
            worker='Worker',
            pid=pid,
            signal='AnotherSignal',
            args=('this', 'time', 'with', 'args')
        )
        self.assertEqual(q.messages, [one, two])

        inst.emit('OneMore')
        three = dict(
            worker='Worker',
            pid=pid,
            signal='OneMore',
            args=tuple()
        )
        self.assertEqual(q.messages, [one, two, three])

    def test_run(self):
        q = DummyQueue()
        args = ('foo', 'bar')
        pid = current_process().pid

        class do_something(self.klass):
            def execute(self, one, two):
                self.emit('Hello', '%s and %s' % (one, two))

        inst = do_something(q, args)
        inst.run()
        self.assertEqual(q.messages[0],
            dict(
                worker='do_something',
                pid=pid,
                signal='Hello',
                args=('foo and bar',),
            )
        )

    def test_execute(self):
        q = DummyQueue()
        args = ('foo', 'bar')
        inst = self.klass(q, args)

        e = raises(NotImplementedError, inst.execute)
        self.assertEqual(str(e), 'Worker.execute()')

        class do_something(self.klass):
            pass
        inst = do_something(q, args)
        e = raises(NotImplementedError, inst.execute)
        self.assertEqual(str(e), 'do_something.execute()')


class test_Manager(TestCase):
    klass = workers.Manager

    def test_init(self):
        inst = self.klass()
        self.assertTrue(inst._running is False)
        self.assertEqual(inst._workers, {})
        self.assertTrue(isinstance(inst._q, multiprocessing.queues.Queue))
        self.assertTrue(inst._thread is None)

    def test_start(self):
        inst = self.klass()

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
        inst = self.klass()

        # Test that kill() returns False when not running:
        self.assertTrue(inst.kill() is False)

    def test_do(self):
        inst = self.klass()

        # Test that False is return when key already exists:
        inst._workers['foo'] = 'bar'
        self.assertTrue(inst.do('foo', 'some_stuff') is False)

        inst._workers.clear()
        self.assertTrue(inst.do('foo', 'some_stuff') is True)
