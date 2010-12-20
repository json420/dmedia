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
from helpers import TempDir, TempHome
from dmedialib import metastore
import couchdb
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record

from desktopcouch.stop_local_couchdb import stop_couchdb
import desktopcouch
import tempfile
import os
import shutil


class test_functions(TestCase):
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


class test_MetaStore(TestCase):
    klass = metastore.MetaStore

    def new(self):
        return self.klass(ctx=self.ctx)

    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix='dc-test.')
        cache = os.path.join(self.data_dir, 'cache')
        data = os.path.join(self.data_dir, 'data')
        config = os.path.join(self.data_dir, 'config')
        self.ctx = desktopcouch.local_files.Context(cache, data, config)

    def tearDown(self):
        stop_couchdb(ctx=self.ctx)
        shutil.rmtree(self.data_dir)

    def test_init(self):
        # Test with ctx=None:
        inst = self.klass()
        self.assertEqual(inst.dbname, 'dmedia')

        # Test with testing ctx:
        inst = self.new()
        self.assertEqual(inst.dbname, 'dmedia')
        self.assertEqual(isinstance(inst.desktop, CouchDatabase), True)
        self.assertEqual(isinstance(inst.server, couchdb.Server), True)

        # Test when overriding dbname:
        inst = self.klass(dbname='dmedia_test', ctx=self.ctx)
        self.assertEqual(inst.dbname, 'dmedia_test')
        self.assertEqual(isinstance(inst.desktop, CouchDatabase), True)
        self.assertEqual(isinstance(inst.server, couchdb.Server), True)

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
        inst.db.create({'_id': thm_chash, 'quickid': thm_qid})
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            []
        )
        inst.db.create({'_id': mov_chash, 'quickid': mov_qid})
        self.assertEqual(
            list(inst.by_quickid(mov_qid)),
            [mov_chash]
        )
        self.assertEqual(
            list(inst.by_quickid(thm_qid)),
            [thm_chash]
        )
        inst.db.create({'_id': 'should-not-happen', 'quickid': mov_qid})
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
            inst.db.create({'bytes': size})
            self.assertEqual(inst.total_bytes(), total)

    def test_extensions(self):
        inst = self.new()
        self.assertEqual(list(inst.extensions()), [])
        for i in xrange(17):
            inst.db.create({'ext': 'mov'})
            inst.db.create({'ext': 'jpg'})
            inst.db.create({'ext': 'cr2'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 17),
                ('mov', 17),
            ]
        )
        for i in xrange(27):
            inst.db.create({'ext': 'mov'})
            inst.db.create({'ext': 'jpg'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 44),
            ]
        )
        for i in xrange(25):
            inst.db.create({'ext': 'mov'})
        self.assertEqual(
            list(inst.extensions()),
            [
                ('cr2', 17),
                ('jpg', 44),
                ('mov', 69),
            ]
        )
