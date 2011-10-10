# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.core` module.
"""

from unittest import TestCase

import microfiber
import filestore

from dmedia.local import LocalStores
from dmedia import core

from .couch import CouchCase
from .base import TempDir


class TestCore(CouchCase):
    def test_init(self):
        inst = core.Core(self.env)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertEqual(inst.db.name, 'dmedia')
        self.assertIsInstance(inst.stores, LocalStores)

    def test_init_local(self):
        inst = core.Core(self.env, bootstrap=False)
        self.assertTrue(inst.db.ensure())
        self.assertFalse(hasattr(inst, 'local'))
        self.assertIsNone(inst._init_local())
        self.assertEqual(inst.local, inst.db.get('_local/dmedia'))

