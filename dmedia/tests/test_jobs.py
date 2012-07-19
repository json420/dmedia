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
Unit tests for `dmedia.jobs`.
"""

from unittest import TestCase
from os import path

import filestore

from dmedia.tests.base import TempDir
from dmedia import jobs




class TestTaskMaster(TestCase):
    def test_init(self):
        tmp = TempDir()

        good = tmp.makedirs('good')
        inst = jobs.TaskMaster(good)
        self.assertEqual(inst.workersdir, good)

        bad = tmp.join('good', '..', 'bad')
        with self.assertRaises(filestore.PathError) as cm:
            inst = jobs.TaskMaster(bad)
        self.assertEqual(cm.exception.untrusted, bad)
        self.assertEqual(cm.exception.abspath, tmp.join('bad'))

        nope = tmp.join('nope')
        with self.assertRaises(ValueError) as cm:
            inst = jobs.TaskMaster(nope)
        self.assertEqual(
            str(cm.exception),
            'workersdir not a directory: {!r}'.format(nope)
        )

        afile = tmp.touch('afile')
        with self.assertRaises(ValueError) as cm:
            inst = jobs.TaskMaster(nope)
        self.assertEqual(
            str(cm.exception),
            'workersdir not a directory: {!r}'.format(nope)
        )

    def test_get_worker_scripts(self):
        tmp = TempDir()
        workersdir = tmp.makedirs('workers')
        inst = jobs.TaskMaster(workersdir)

        self.assertEqual(
            inst.get_worker_script('foo'),
            tmp.join('workers', 'foo')
        )

        with self.assertRaises(jobs.PathTraversal) as cm:
            inst.get_worker_script('../foo')
        self.assertEqual(cm.exception.untrusted, tmp.dir + '/workers/../foo')
        self.assertEqual(cm.exception.abspath, tmp.dir + '/foo')
        self.assertEqual(cm.exception.workersdir, workersdir)
        
    
