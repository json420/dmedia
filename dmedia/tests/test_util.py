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

import microfiber
from microfiber import random_id, Database, NotFound

from .base import TempDir
from .couch import CouchCase

from dmedia import schema
from dmedia import util


doc_type = """
function(doc) {
    emit(doc.type, null);
}
"""


class TestFunctions(TestCase):
    def test_isfilestore(self):
        tmp = TempDir()
        self.assertFalse(util.isfilestore(tmp.dir))
        tmp.mkdir('.dmedia')
        self.assertFalse(util.isfilestore(tmp.dir))
        tmp.touch('.dmedia', 'store.json')
        self.assertFalse(util.isfilestore(tmp.dir))
        tmp.touch('.dmedia', 'filestore.json')
        self.assertTrue(util.isfilestore(tmp.dir))


class TestCouchFunctions(CouchCase):
    def test_get_designs(self):
        db = Database('hello', self.env)
        db.put(None)
        self.assertEqual(util.get_designs(db), {})

        foo = {
            '_id': '_design/foo',
            'views': {
                'foo': {'map': doc_type},
            },
        }
        db.save(foo)
        self.assertEqual(
            util.get_designs(db),
            {
                '_design/foo': foo['_rev'],
            }
        )

        bar = {
            '_id': '_design/bar',
            'views': {
                'bar': {'map': doc_type, 'reduce': '_count'},
            },
        }
        db.save(bar)
        self.assertEqual(
            util.get_designs(db),
            {
                '_design/foo': foo['_rev'],
                '_design/bar': bar['_rev'],
            }
        )

    def test_update_design_doc(self):
        f = util.update_design_doc
        db = util.get_db(self.env)
        db.put(None)

        # Test when design doesn't exist:
        doc = {
            '_id': '_design/doc',
            'views': {
                'type': {'map': doc_type},
            },
        }
        self.assertEqual(f(db, doc), 'new')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertNotIn('_rev', doc)

        # Test when design is same:
        self.assertEqual(f(db, doc), 'same')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertNotIn('_rev', doc)

        # Test when design has changed:
        doc['views']['type'] = {'map': doc_type, 'reduce': '_count'}
        self.assertEqual(f(db, doc), 'changed')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '2-a55f77029023bffaf68c19bb618d7b7a'
        )
        self.assertNotIn('_rev', doc)

        # Again test when design is same:
        self.assertEqual(f(db, doc), 'same')
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '2-a55f77029023bffaf68c19bb618d7b7a'
        )
        self.assertNotIn('_rev', doc)

    def test_init_views(self):
        db = util.get_db(self.env)
        db.put(None)

        doc1 = {
            '_id': '_design/doc',
            'views': {
                'type': {'map': doc_type},
            },
        }
        doc2 = {
            '_id': '_design/stuff',
            'views': {
                'junk': {'map': doc_type, 'reduce': '_count'},
            },
        }
        designs = (doc1, doc2)
 
        self.assertEqual(util.init_views(db, designs),
            [
                ('new', '_design/doc'),
                ('new', '_design/stuff'),   
            ]
        )
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )

        self.assertEqual(util.init_views(db, designs),
            [
                ('same', '_design/doc'),
                ('same', '_design/stuff'),   
            ]
        )
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '1-98e283762b239249fc0cfc4159d84797'
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )

        # Test that old designs get deleted
        self.assertEqual(util.init_views(db, [doc2]),
            [
                ('same', '_design/stuff'),   
                ('deleted', '_design/doc'),
            ]
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )
        with self.assertRaises(NotFound):
            db.get('_design/doc')

        # Test restoring a deleted design:            
        self.assertEqual(util.init_views(db, designs),
            [
                ('new', '_design/doc'),
                ('same', '_design/stuff'),   
            ]
        )
        self.assertEqual(
            db.get('_design/doc')['_rev'],
            '3-52fa486f718f1f929eebd96339796904'
        )
        self.assertEqual(
            db.get('_design/stuff')['_rev'],
            '1-f2fc40529084795118edaa583a0cc89b'
        )

    def test_get_db(self):
        db = util.get_db(self.env)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertTrue(db.ensure())
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)

        # Test our assumptions about default CouchDB _revs_limit:
        self.assertEqual(db.get('_revs_limit'), 1000)

    def test_get_db2(self):
        # Test with init=True
        db = util.get_db(self.env, True)
        self.assertIsInstance(db, microfiber.Database)
        self.assertEqual(db.name, schema.DB_NAME)
        self.assertEqual(db.get()['db_name'], schema.DB_NAME)
        self.assertFalse(db.ensure())

        # Make sure that with init=True _revs_limit is set:
        self.assertEqual(db.get('_revs_limit'), 25)

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

