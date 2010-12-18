#!/usr/bin/env python

# Authors:
#   David Green <david4dev@gmail.com>
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

import os
from dmedialib.importui.pidlock import PidLock
from unittest import TestCase

class test_PidLock(TestCase):
    klass = PidLock

    def test_init(self):
        pid_lock = self.klass('dmedia-test-pidlock')
        self.assertEqual(pid_lock.appname, 'dmedia-test-pidlock')
        self.assertEqual(pid_lock.pid_file, '/tmp/dmedia-test-pidlock.pid')

    def test_get(self):
        pid_lock = self.klass('dmedia-test-pidlock')
        #assume no other tests are running (they shouldn't be)
        self.assertTrue(self.get())

    def test_create(self):
        pid_lock = self.klass('dmedia-test-pidlock')
        pid = os.getpid()
        pid_lock.create()
        pid_file = open('/tmp/dmedia-test-pidlock.pid', "r")
        pidstr = pid_file.readline()
        pid_file.close()
        self.assertEqual(pid, int(pidstr))
        os.remove(pid_lock.pid_file)

    def test_release(self):
        pid_lock = self.klass('dmedia-test-pidlock')
        pid_lock.create()
        pid_lock.release()
        pid_file = open('/tmp/dmedia-test-pidlock.pid', "r")
        pidstr = pid_file.readline()
        pid_file.close()
        self.assertEqual(pidstr, '')
        os.remove(pid_lock.pid_file)


