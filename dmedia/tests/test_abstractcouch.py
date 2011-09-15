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
Unit tests for `dmedia.abstractcouch` module.
"""

import microfiber

from dmedia import abstractcouch

from .couch import CouchCase


class test_functions(CouchCase):

    def test_get_db(self):
        s = microfiber.Server(self.env)
        s.delete(self.dbname)

        db = abstractcouch.get_db(self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, 'test_dmedia')

        # Make sure database dosen't exist:
        try:
            db.delete()
        except microfiber.NotFound:
            pass

        # Make sure database isn't created by get_db():
        db = abstractcouch.get_db(self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, 'test_dmedia')

        with self.assertRaises(microfiber.NotFound) as cm:
            db.get()

        # Create the database
        self.assertEqual(db.put(None), {'ok': True})
        self.assertEqual(db.get()['db_name'], 'test_dmedia')

