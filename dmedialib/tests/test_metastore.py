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
Unit tests for `dmedialib.metastore` module.
"""

from unittest import TestCase
import socket
import platform
from helpers import CouchCase, TempDir, TempHome
from dmedialib import metastore
import couchdb
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record
from desktopcouch.local_files import Context
from desktopcouch.stop_local_couchdb import stop_couchdb
import desktopcouch
import tempfile
import os
import shutil


class test_functions(TestCase):
    def test_dc_context(self):
        f = metastore.dc_context
        tmp = TempDir()
        ctx = f(tmp.path)
        self.assertTrue(isinstance(ctx, Context))
        self.assertEqual(ctx.run_dir, tmp.join('cache'))
        self.assertEqual(ctx.db_dir, tmp.join('data'))
        self.assertEqual(ctx.config_dir, tmp.join('config'))

        # Test that it makes sure couchdir is a directory
        self.assertRaises(AssertionError, f, tmp.join('nope'))
        self.assertRaises(AssertionError, f, tmp.touch('nope'))

    def test_build_design_doc(self):
        f = metastore.build_design_doc
        views = (
            ('bytes', 'foo', '_sum'),
            ('mtime', 'bar', None),
        )
        self.assertEqual(f('file', views),
            (
                '_design/file',
                {
                    '_id': '_design/file',
                    'language': 'javascript',
                    'views': {
                        'bytes': {
                            'map': 'foo',
                            'reduce': '_sum',
                        },
                        'mtime': {
                            'map': 'bar',
                        },
                    }
                }
            )
        )

    def test_create_machine(self):
        f = metastore.create_machine
        doc = f()
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'machine_id',
                'type',
                'time',
                'hostname',
                'distribution',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/machine')
        self.assertEqual(doc['_id'], '_local/machine')
        self.assertEqual(doc['hostname'], socket.gethostname())
        self.assertEqual(doc['distribution'], platform.linux_distribution())


class test_MetaStore(CouchCase):
    klass = metastore.MetaStore

    def new(self):
        return self.klass(couchdir=self.couchdir)

    def test_init(self):
        # Test with testing ctx:
        inst = self.new()
        self.assertEqual(inst.dbname, 'dmedia')
        self.assertEqual(isinstance(inst.desktop, CouchDatabase), True)
        self.assertEqual(isinstance(inst.server, couchdb.Server), True)

        # Test when overriding dbname:
        inst = self.klass(dbname='dmedia_test', couchdir=self.couchdir)
        self.assertEqual(inst.dbname, 'dmedia_test')
        self.assertEqual(isinstance(inst.desktop, CouchDatabase), True)
        self.assertEqual(isinstance(inst.server, couchdb.Server), True)

    def test_create_machine(self):
        inst = self.new()
        self.assertFalse('_local/machine' in inst.db)
        _id = inst.create_machine()
        self.assertTrue('_local/machine' in inst.db)
        self.assertTrue(_id in inst.db)
        loc = inst.db['_local/machine']
        doc = inst.db[_id]
        self.assertEqual(set(loc), set(doc))
        self.assertEqual(loc['machine_id'], doc['machine_id'])
        self.assertEqual(loc['time'], doc['time'])

        self.assertEqual(inst._machine_id, None)
        self.assertEqual(inst.machine_id, _id)
        self.assertEqual(inst._machine_id, _id)

    def test_by_quickid(self):
        mov_chash = 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE'
        mov_qid = 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'
        thm_chash = 'F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A'
        thm_qid =  'EYCDXXCNDB6OIIX5DN74J7KEXLNCQD5M'
        inst = self.new()
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            []
        )
        inst.db.create(
            {'_id': thm_chash, 'qid': thm_qid, 'type': 'dmedia/file'}
        )
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            []
        )
        inst.db.create(
            {'_id': mov_chash, 'qid': mov_qid, 'type': 'dmedia/file'}
        )
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            [mov_chash]
        )
        self.assertEqual(
            list(inst.by_quickid(thm_qid)),
            [thm_chash]
        )
        inst.db.create(
            {'_id': 'should-not-happen', 'qid': mov_qid, 'type': 'dmedia/file'}
        )
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            [mov_chash, 'should-not-happen']
        )

    def test_total_bytes(self):
        inst = self.new()
        self.assertEqual(inst.total_bytes(), 0)
        total = 0
        for exp in xrange(20, 31):
            size = 2 ** exp + 1
            total += size
            inst.db.create({'bytes': size, 'type': 'dmedia/file'})
            self.assertEqual(inst.total_bytes(), total)

    def test_extensions(self):
        inst = self.new()
        self.assertEqual(list(inst.extensions()), [])
        for i in xrange(17):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'jpg', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'cr2', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 17),
                ('mov', 17),
            ]
        )
        for i in xrange(27):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
            inst.db.create({'ext': 'jpg', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 44),
            ]
        )
        for i in xrange(25):
            inst.db.create({'ext': 'mov', 'type': 'dmedia/file'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 69),
            ]
        )
