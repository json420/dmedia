# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Unit tests for `dmedialib.metastore` module.
"""

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

class test_MetaStore(object):
    klass = metastore.MetaStore

    def new(self):
        return self.klass(context=self.ctx)

    def setUp(self):
        self.test_data_dir = tempfile.mkdtemp()
        if not os.path.exists(self.test_data_dir):
            os.mkdir(self.test_data_dir)
        cache = os.path.join(self.test_data_dir, 'cache')
        data = os.path.join(self.test_data_dir, 'data')
        config = os.path.join(self.test_data_dir, 'config')

        self.ctx = desktopcouch.local_files.Context(cache, data, config)

    def tearDown(self):
        pid = desktopcouch.read_pidfile(self.ctx)

        stop_couchdb(pid=pid)
        shutil.rmtree(self.test_data_dir)

    def test_init(self):
        assert self.klass.type_url == 'http://example.com/dmedia'

        inst = self.klass(context=self.ctx)
        assert inst.name == 'dmedia'
        assert inst.test is False
        assert isinstance(inst.desktop, CouchDatabase)
        assert isinstance(inst.server, couchdb.Server)

        inst = self.new()
        assert inst.name == 'dmedia_test'
        assert inst.test is True
        assert isinstance(inst.desktop, CouchDatabase)
        assert isinstance(inst.server, couchdb.Server)

    def test_total_bytes(self):
        inst = self.new()
        assert inst.total_bytes() == 0
        total = 0
        for exp in xrange(20, 31):
            size = 2 ** exp + 1
            total += size
            inst.db.create({'bytes': size})
            assert inst.total_bytes() == total

    def test_extensions(self):
        inst = self.new()
        assert list(inst.extensions()) == []
        for i in xrange(17):
            inst.db.create({'ext': 'mov'})
            inst.db.create({'ext': 'jpg'})
            inst.db.create({'ext': 'cr2'})
        assert list(inst.extensions()) == [
            ('cr2', 17),
            ('jpg', 17),
            ('mov', 17),
        ]
        for i in xrange(27):
            inst.db.create({'ext': 'mov'})
            inst.db.create({'ext': 'jpg'})
        assert list(inst.extensions()) == [
            ('cr2', 17),
            ('jpg', 44),
            ('mov', 44),
        ]
        for i in xrange(25):
            inst.db.create({'ext': 'mov'})
        assert list(inst.extensions()) == [
            ('cr2', 17),
            ('jpg', 44),
            ('mov', 69),
        ]
