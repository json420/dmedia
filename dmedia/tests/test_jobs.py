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
import time

import filestore
import microfiber

from dmedia.tests.base import TempDir
from dmedia.tests.couch import CouchCase
from dmedia.views import job_design
from dmedia import jobs


dummy_workers = path.join(path.dirname(path.abspath(__file__)), 'workers')
assert path.isdir(dummy_workers)
echo_script = path.join(dummy_workers, 'echo-script')
assert path.isfile(echo_script)


class TestPathTraversal(TestCase):
    def test_init(self):
        inst = jobs.PathTraversal('foo', 'bar', 'baz')
        self.assertEqual(inst.untrusted, 'foo')
        self.assertEqual(inst.abspath, 'bar')
        self.assertEqual(inst.workersdir, 'baz')
        self.assertEqual(str(inst), "'bar' is outside of 'baz'")


class TestTaskMaster(CouchCase):
    def test_init(self):
        tmp = TempDir()

        good = tmp.makedirs('good')
        inst = jobs.TaskMaster(good, self.env)
        self.assertEqual(inst.workersdir, good)
        self.assertIsInstance(inst.db, microfiber.Database)

        bad = tmp.join('good', '..', 'bad')
        with self.assertRaises(filestore.PathError) as cm:
            inst = jobs.TaskMaster(bad, self.env)
        self.assertEqual(cm.exception.untrusted, bad)
        self.assertEqual(cm.exception.abspath, tmp.join('bad'))

        nope = tmp.join('nope')
        with self.assertRaises(ValueError) as cm:
            inst = jobs.TaskMaster(nope, self.env)
        self.assertEqual(
            str(cm.exception),
            'workersdir not a directory: {!r}'.format(nope)
        )

        afile = tmp.touch('afile')
        with self.assertRaises(ValueError) as cm:
            inst = jobs.TaskMaster(nope, self.env)
        self.assertEqual(
            str(cm.exception),
            'workersdir not a directory: {!r}'.format(nope)
        )

    def test_iter_workers(self):
        tmp = TempDir()
        inst = jobs.TaskMaster(tmp.dir, self.env)

        self.assertEqual(list(inst.iter_workers()), [])

        # Should ignore directories
        tmp.makedirs('some-dir')
        self.assertEqual(list(inst.iter_workers()), [])

        # Add a file
        tmp.touch('foo')
        self.assertEqual(list(inst.iter_workers()), ['foo'])

        # Add another file, should sort alphabetically
        tmp.touch('bar')
        self.assertEqual(list(inst.iter_workers()), ['bar', 'foo'])

    def test_get_worker_scripts(self):
        tmp = TempDir()
        workersdir = tmp.makedirs('workers')
        inst = jobs.TaskMaster(workersdir, self.env)

        self.assertEqual(
            inst.get_worker_script('foo'),
            tmp.join('workers', 'foo')
        )

        with self.assertRaises(jobs.PathTraversal) as cm:
            inst.get_worker_script('../foo')
        self.assertEqual(cm.exception.untrusted, tmp.dir + '/workers/../foo')
        self.assertEqual(cm.exception.abspath, tmp.dir + '/foo')
        self.assertEqual(cm.exception.workersdir, workersdir)

        inst = jobs.TaskMaster(dummy_workers, self.env)
        self.assertEqual(inst.get_worker_script('echo-script'), echo_script)

    def test_run(self):
        inst = jobs.TaskMaster(dummy_workers, self.env)
        inst.db.ensure()
        inst.db.post(job_design)

        inst.run()

        id1 = microfiber.random_id()
        doc1 = {
            '_id': id1,
            'type': 'dmedia/job',
            'time': 17,
            'worker': 'echo-script',
            'status': 'waiting',
            'job': {
                'delay': 0.5,
            },
            'files': [],
        }
        inst.db.save(doc1)

        id2 = microfiber.random_id()
        doc2 = {
            '_id': id2,
            'type': 'dmedia/job',
            'time': 19,
            'worker': 'echo-script',
            'status': 'waiting',
            'job': {
                'delay': 0.5,
                'fail': True,
            },
            'files': [],
        }
        inst.db.save(doc2)

        id3 = microfiber.random_id()
        doc3 = {
            '_id': id3,
            'type': 'dmedia/job',
            'time': 21,
            'worker': 'echo-script',
            'status': 'waiting',
            'job': {
                'delay': 0.5,
            },
            'files': [],
        }
        inst.db.save(doc3)

        inst.run()
        self.assertEqual(inst.db.get(id1)['status'], 'completed')
        self.assertEqual(inst.db.get(id2)['status'], 'failed')
        self.assertEqual(inst.db.get(id3)['status'], 'completed')

    def test_run_job(self):
        inst = jobs.TaskMaster(dummy_workers, self.env)
        inst.db.ensure()

        # Test when it's all good
        job_id = microfiber.random_id()
        marker = microfiber.random_id()
        file_id = microfiber.random_id(filestore.DIGEST_BYTES)
        doc = {
            '_id': job_id,
            'worker': 'echo-script',
            'status': 'waiting',
            'job': {
                'delay': 1.25,
                'marker': marker,
            },
            'files': [file_id],
        }
        start = time.time()
        self.assertTrue(inst.run_job(doc))
        doc = inst.db.get(job_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        self.assertGreaterEqual(doc['time_start'], start)
        self.assertGreaterEqual(doc['time_end'], doc['time_start'] + 1)
        self.assertEqual(doc['status'], 'completed')
        self.assertEqual(doc['result'],
            {
                'job': {
                    'delay': 1.25,
                    'marker': marker,
                },
                'files': [file_id],
            }
        )

        # Test when the worker exists with a non-zero status
        job_id = microfiber.random_id()
        marker = microfiber.random_id()
        file_id = microfiber.random_id(filestore.DIGEST_BYTES)
        doc = {
            '_id': job_id,
            'worker': 'echo-script',
            'status': 'waiting',
            'job': {
                'delay': 1.25,
                'marker': marker,
                'fail': True,
            },
            'files': [file_id],
        }
        start = time.time()
        self.assertTrue(inst.run_job(doc))
        doc = inst.db.get(job_id)
        self.assertEqual(doc['_rev'][:2], '2-')
        self.assertGreaterEqual(doc['time_start'], start)
        self.assertGreaterEqual(doc['time_end'], doc['time_start'] + 1)
        self.assertEqual(doc['status'], 'failed')
        self.assertNotIn('result', doc)

        # Test with a naughty
        job_id = microfiber.random_id()
        marker = microfiber.random_id()
        file_id = microfiber.random_id(filestore.DIGEST_BYTES)
        doc = {
            '_id': job_id,
            'worker': '../sneaky',
            'status': 'waiting',
            'job': {
                'delay': 1.25,
                'marker': marker,
            },
            'files': [file_id],
        }
        start = time.time()
        with self.assertRaises(jobs.PathTraversal) as cm:
            self.assertTrue(inst.run_job(doc))
        self.assertEqual(cm.exception.untrusted, dummy_workers + '/../sneaky')
        self.assertEqual(cm.exception.untrusted, dummy_workers + '/../sneaky')
        doc = inst.db.get(job_id)
        self.assertEqual(doc['_rev'][:2], '1-')
        self.assertGreaterEqual(doc['time_start'], start)
        self.assertEqual(doc['status'], 'executing')
        self.assertNotIn('time_end', doc)
        self.assertNotIn('result', doc)
