# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2011 Novacut Inc
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
Unit tests for `dmedia.util`.
"""

from unittest import TestCase
from .base import TempDir
from .couch import CouchCase

import microfiber
from microfiber import random_id

from dmedia import schema
from dmedia import util


class TestFunctions(TestCase):
    def test_isfilestore(self):
        tmp = TempDir()
        self.assertFalse(util.isfilestore(tmp.dir))
        tmp.makedirs('.dmedia')
        self.assertTrue(util.isfilestore(tmp.dir))


class TestDBFunctions(CouchCase):
    def test_get_db(self):
        db = util.get_db(self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertTrue(db.ensure())
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)

    def test_get_db2(self):
        # Test with init=True
        db = util.get_db(self.env, True)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)
        self.assertFalse(db.ensure())

    def test_get_project_db(self):
        _id = random_id()
        db_name = schema.project_db_name(_id)
        db = util.get_project_db(_id, self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, db_name)
        self.assertTrue(db.ensure())
        self.assertEqual(db.get()['db_name'], db_name)
        
        # Test with init=True
        _id = random_id()
        db_name = schema.project_db_name(_id)
        db = util.get_project_db(_id, self.env, True)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, db_name)
        self.assertEqual(db.get()['db_name'], db_name)
        self.assertFalse(db.ensure())

