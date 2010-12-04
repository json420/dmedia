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


class DummyQueue(object):
    def __init__(self):
        self.messages = []

    def put(self, msg):
        self.messages.append(msg)


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
