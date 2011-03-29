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
Unit tests for `dmedia.metastore` module.
"""

from unittest import TestCase

from dmedia.tests.helpers import TempDir, random_bus
from dmedia.tests.couch import CouchCase
from dmedia import importer
from dmedia.gtkui import service


class test_DMedia(CouchCase):
    klass = service.DMedia

    def test_init(self):
        bus = random_bus()
        self.env['bus'] = bus
        self.env['no_gui'] = True
        def kill():
            pass
        inst = self.klass(self.env, killfunc=kill)
        self.assertTrue(inst._killfunc is kill)
        self.assertTrue(inst._bus is bus)
        self.assertTrue(inst._dbname is self.dbname)
        self.assertTrue(inst._no_gui)
        self.assertEqual(inst._manager, None)

        m = inst.manager
        self.assertTrue(inst._manager is m)
        self.assertTrue(isinstance(m, importer.ImportManager))
        self.assertEqual(m._callback, inst._on_signal)
