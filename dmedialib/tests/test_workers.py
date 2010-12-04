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

from unittest import TestCase
from multiprocessing import current_process
from dmedialib import workers
from .helpers import raises


class DummyQueue(object):
    def __init__(self):
        self.messages = []

    def put(self, msg):
        self.messages.append(msg)



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
                self.emit('Example', *self.args)

        workers.register(import_files)

        q = DummyQueue()
        f('import_files', q, ('hello', 'world'))

        self.assertEqual(
            q.messages,
            [
                dict(
                    signal='Example',
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
        self.assertEqual(inst.pid, current_process().pid)
        self.assertEqual(inst.name, 'Worker')

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
        inst = self.klass(q, args)

        e = raises(NotImplementedError, inst.run)
        self.assertEqual(str(e), 'Worker.run()')

        class do_something(self.klass):
            pass
        inst = do_something(q, args)
        e = raises(NotImplementedError, inst.run)
        self.assertEqual(str(e), 'do_something.run()')
