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
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record


class test_MetaStore(object):
    klass = metastore.MetaStore

    def test_init(self):
        inst = self.klass()
        assert isinstance(inst.db, CouchDatabase)
        assert inst.type_url == 'http://example.com/dmedia'
