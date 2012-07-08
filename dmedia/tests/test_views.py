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
import time
from copy import deepcopy

from microfiber import Database, random_id
from filestore import DIGEST_BYTES

from dmedia.tests.couch import CouchCase
from dmedia import util, views


class TestDesignValues(TestCase):
    """
    Do a Python value sanity check on all design docs.

    This is a fast test to make sure all the design docs are well-formed from
    the Python perspective.  But it can't tell you if you have JavaScript syntax
    errors.  For that, there is `TestDesignsLive`.
    """

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
                self.assertIn(value['reduce'], ['_count', '_sum'])

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


class TestDesignsLive(CouchCase):
    """
    Do a sanity check on all design docs using a live CouchDB.

    This is mostly a check for JavaScript syntax errors, or other things that
    would make a design or view fail immediately.
    """

    def check_views1(self, db, doc):
        """
        Test views when database is empty.
        """
        design = doc['_id'].split('/')[1]
        for view in doc['views']:
            if 'reduce' in doc['views'][view]:
                expected = {'rows': []}
            else:
                expected = {'rows': [], 'offset': 0, 'total_rows': 0}
            self.assertEqual(db.view(design, view), expected,
                '_design/{}/_view/{}'.format(design, view)
            )

    def check_views2(self, db, doc):
        """
        Test views when database is *not* empty.
        """
        design = doc['_id'].split('/')[1]
        for view in doc['views']:
            db.view(design, view)
            if 'reduce' in doc['views'][view]:
                db.view(design, view, reduce=True)

    def check_designs(self, designs):
        db = Database('foo', self.env)
        db.put(None)
        ids = [doc['_id'] for doc in designs]
        self.assertEqual(
            util.init_views(db, designs),
            [('new', _id) for _id in ids],
        )

        # Test all designs when database is empty
        for doc in designs:
            self.check_views1(db, doc)

        # Add 100 random docs and test all designs again
        for i in range(100):
            db.post({'_id': random_id()})
        for doc in designs:
            self.check_views2(db, doc)

    def test_core(self):
        self.check_designs(views.core)

    def test_project(self):
        self.check_designs(views.project)


class TestFileDesign(CouchCase):
    """
    Test each view function in the _design/file design.
    """
    design = views.file_design

    def build_view(self, view):
        return {
            '_id': self.design['_id'],
            'views': {
                view: self.design['views'][view],   
            }
        }

    def test_stored(self):
        db = Database('foo', self.env)
        db.put(None)
        design = self.build_view('stored')
        db.save(design)

        self.assertEqual(
            db.view('file', 'stored'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        (store_id1, store_id2) = sorted(random_id() for i in range(2))
        _id = random_id(DIGEST_BYTES)
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
            'stored': {
                store_id1: None,
                store_id2: None,
            },
        }
        db.save(doc)

        self.assertEqual(
            db.view('file', 'stored'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': None},
                    {'key': store_id2, 'id': _id, 'value': None},
                ]
            },
        )

        # Make sure view func checks doc.type
        doc['type'] = 'dmedia/file2'
        db.save(doc)
        
        self.assertEqual(
            db.view('file', 'stored'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_fragile(self):
        db = Database('foo', self.env)
        db.put(None)
        design = self.build_view('fragile')
        db.save(design)

        self.assertEqual(
            db.view('file', 'fragile'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Schema-wise, doc['stored'] is supposed to be present and non-empty,
        # but lets still make sure files are reported as fragile when this
        # isn't the case.
        _id = random_id(DIGEST_BYTES)
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
            'origin': 'user',
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'fragile'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )

        # Make things work even if copies is missing
        doc['stored'] = {
            random_id(): {},
            random_id(): {},
            random_id(): {},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'fragile'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )

        # Make sure copies is being properly summed
        doc['stored'] = {
            random_id(): {'copies': 1},
            random_id(): {'copies': 0},
            random_id(): {'copies': 1},
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'fragile'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 2, 'id': _id, 'value': None},
                ],
            },
        )

        # Check when one store provides 3 copies
        doc['stored'] = {
            random_id(): {'copies': 3},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'fragile'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Check when each store provides 1 copy
        doc['stored'] = {
            random_id(): {'copies': 1},
            random_id(): {'copies': 1},
            random_id(): {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'fragile'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_reclaimable(self):
        db = Database('foo', self.env)
        db.put(None)
        design = self.build_view('reclaimable')
        db.save(design)

        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        _id = random_id(DIGEST_BYTES)
        atime = time.time()
        stores = sorted(random_id() for i in range(4))
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
            'origin': 'user',
            'atime': atime,
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc['stored'] = {}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        
        # Test when copies is missing:
        doc['stored'] = {
            stores[0]: {'copies': 3},
            stores[1]: {},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Again test when copies is missing
        doc['stored'] = {
            stores[0]: {},
            stores[1]: {},
            stores[2]: {},
            stores[3]: {},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Should only emit specific stores such that sufficient durability is
        # maintained if that copy was removed.

        # In this case, nothing can be reclaimed, even though total durability
        # is 4:
        doc['stored'] = {
            stores[0]: {'copies': 2},
            stores[1]: {'copies': 2},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # But any one of these could be reclaimed:
        doc['stored'] = {
            stores[0]: {'copies': 2},
            stores[1]: {'copies': 2},
            stores[2]: {'copies': 2},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {
                'offset': 0, 
                'total_rows': 3,
                'rows': [
                    {'key': [stores[0], atime], 'id': _id, 'value': None},
                    {'key': [stores[1], atime], 'id': _id, 'value': None},
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # And any one of these could be reclaimed:
        doc['stored'] = {
            stores[0]: {'copies': 1},
            stores[1]: {'copies': 1},
            stores[2]: {'copies': 1},
            stores[3]: {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {
                'offset': 0, 
                'total_rows': 4,
                'rows': [
                    {'key': [stores[0], atime], 'id': _id, 'value': None},
                    {'key': [stores[1], atime], 'id': _id, 'value': None},
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
                    {'key': [stores[3], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # One of these can be reclaimed:
        doc['stored'] = {
            stores[0]: {'copies': 3},
            stores[1]: {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Two of these can be reclaimed (just not at once):
        doc['stored'] = {
            stores[0]: {'copies': 1},
            stores[1]: {'copies': 2},
            stores[2]: {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {
                'offset': 0, 
                'total_rows': 2,
                'rows': [
                    {'key': [stores[0], atime], 'id': _id, 'value': None},
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Test that doc['type'] is considered
        doc['type'] = 'dmedia/file2'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test that doc['origin'] must be 'user'
        doc['type'] = 'dmedia/file'
        doc['origin'] = 'render'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_verified(self):
        db = Database('foo', self.env)
        db.put(None)
        design = self.build_view('verified')
        db.save(design)
        self.assertEqual(
            db.view('file', 'verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        id1 = random_id(DIGEST_BYTES)
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Add another doc
        id2 = random_id(DIGEST_BYTES)
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
        }
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test sort order within the same store: None < 0
        store_id = random_id()
        doc1['stored'] = {
            store_id: {},
        }
        db.save(doc1)
        doc2['stored'] = {
            store_id: {'verified': 0},
        }
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'verified'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': [store_id, None], 'id': id1, 'value': None},
                    {'key': [store_id, 0], 'id': id2, 'value': None},
                ]
            },
        )

        # Test sort order within the same store: 0 < 1234567890
        doc1['stored'] = {
            store_id: {'verified': 1234567890},
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'verified'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': [store_id, 0], 'id': id2, 'value': None},
                    {'key': [store_id, 1234567890], 'id': id1, 'value': None},
                ]
            },
        )
