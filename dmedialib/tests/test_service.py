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
from helpers import CouchCase, TempDir, random_bus
from dmedialib import service, importer


class test_DMedia(CouchCase):
    klass = service.DMedia

    def test_init(self):
        bus = random_bus()
        def kill():
            pass
        inst = self.klass(
            killfunc=kill, bus=bus, couchdir=self.couchdir, no_gui=True
        )
        self.assertTrue(inst._killfunc is kill)
        self.assertTrue(inst._bus is bus)
        self.assertTrue(inst._couchdir is self.couchdir)
        self.assertTrue(inst._no_gui)
        self.assertEqual(inst._manager, None)

        m = inst.manager
        self.assertTrue(inst._manager is m)
        self.assertTrue(isinstance(m, importer.ImportManager))
        self.assertEqual(m._callback, inst._on_signal)
        self.assertTrue(m._couchdir is self.couchdir)
