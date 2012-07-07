# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010, 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.views` module.
"""

from unittest import TestCase

from microfiber import Database

from dmedia.tests.couch import CouchCase
from dmedia.util import get_db
from dmedia import views


class TestDesignValues(TestCase):
    def check_design(self, doc):
        self.assertIsInstance(doc, dict)
        self.assertTrue(set(doc).issuperset(['_id', 'views']))
        self.assertTrue(set(doc).issubset(['_id', 'views', 'filters']))

        _id = doc['_id']
        self.assertIsInstance(_id, str)
        self.assertTrue(_id.startswith('_design/'))

        views = doc['views']
        self.assertIsInstance(views, dict)
        self.assertGreater(len(views), 0)
        for (key, value) in views.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, dict)
            self.assertTrue(set(value).issuperset(['map']))
            self.assertTrue(set(value).issubset(['map', 'reduce']))
            self.assertIsInstance(value['map'], str)
            if 'reduce' in value:
                self.assertIsInstance(value['reduce'], str)

        if 'filters' not in doc:
            return

        filters = doc['filters']
        self.assertIsInstance(filters, dict)
        self.assertGreater(len(filters), 0)
        for (key, value) in filters.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, str)

    def test_core(self):
        for doc in views.core:
            self.check_design(doc)

    def test_project(self):
        for doc in views.project:
            self.check_design(doc)

        
