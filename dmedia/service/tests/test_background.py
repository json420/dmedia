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

from dmedia.tests.base import TempDir
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

