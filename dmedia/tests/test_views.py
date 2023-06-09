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

from dbase32 import random_id
import microfiber
from filestore import DIGEST_BYTES

from dmedia.tests.base import random_file_id
from dmedia.tests.couch import CouchCase
from dmedia import util, views


class TestSortOrder(CouchCase):
    def test_sort_order(self):
        """
        Verify our assumptions about CouchDB view sort order.

        Lesson: it's *very* important that views like "file/downgrade-by-verified" test
        the timestamp with::

            (typeof timestamp == 'number')

        and then replace all non-numeric timestamps with null.

        The reason is strings, objects, and arrays will always sort higher than
        any number.  So when doing something like downgrading by the last
        verification timestamp, it means these non-numeric timestamps will
        never be included, no matter how far into the future we go.

        To clarify with an example, this would be downgraded after the Unix time
        hits ``1434567890``:

        >>> doc = {'stored': {'mystore': {'verified': 1434567890}}}
        >>> type(doc['stored']['mystore']['verified'])
        <class 'int'>

        However, this will *never* be downgraded, even in the amazing year one
        billion:

        >>> doc = {'stored': {'mystore': {'verified': '1434567890'}}}
        >>> type(doc['stored']['mystore']['verified'])
        <class 'str'>

        """
        db = microfiber.Database('foo', self.env)
        db.put(None)

        doc_key = """
        function(doc) {
            emit(doc.key, null);
        }
        """
        doc_design = {
            '_id': '_design/doc',
            'views': {
                'key': {'map': doc_key},
            },
        }
        db.save(doc_design)

        self.assertEqual(
            db.view('doc', 'key'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        docs = [
            {'_id': random_id(), 'key': None},
            {'_id': random_id(), 'key': False},
            {'_id': random_id(), 'key': True},
            {'_id': random_id(), 'key': -16},
            {'_id': random_id(), 'key': 0},
            {'_id': random_id(), 'key': 18},
            {'_id': random_id(), 'key': '17'},
            {'_id': random_id(), 'key': 'hello'},
            {'_id': random_id(), 'key': ['hello', 'naughty', 'nurse']},
            {'_id': random_id(), 'key': {'hello': 'world'}},
        ]
        rows = [
            {'key': doc['key'], 'id': doc['_id'], 'value': None}
            for doc in docs
        ]
        db.save_many(docs)
        self.assertEqual(
            db.view('doc', 'key'),
            {
                'offset': 0,
                'total_rows': len(rows),
                'rows': rows,
            }
        )
        rows.reverse()
        self.assertEqual(
            db.view('doc', 'key', descending=True),
            {
                'offset': 0,
                'total_rows': len(rows),
                'rows': rows,
            }
        )


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
                self.assertIn(value['reduce'], ['_count', '_sum', '_stats'])

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

    def check_views(self, db, doc):
        design = doc['_id'].split('/')[1]
        for view in doc['views']:
            db.view(design, view)
            if 'reduce' in doc['views'][view]:
                db.view(design, view, reduce=True)

    def check_designs(self, designs):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        ids = [doc['_id'] for doc in designs]
        self.assertEqual(
            util.init_views(db, designs),
            [('new', _id) for _id in ids],
        )

        # Test all designs when database is empty
        for doc in designs:
            self.check_views(db, doc)

        # Add 100 random docs and test all designs again
        for i in range(100):
            db.post({'_id': random_id()})
        for doc in designs:
            self.check_views(db, doc)

    def test_core(self):
        self.check_designs(views.core)

    def test_project(self):
        self.check_designs(views.project)


class DesignTestCase(CouchCase):
    """
    Base class for CouchDB design/view tests.
    """
    design = views.file_design  # Override this is subclasses

    def build_view(self, view):
        return {
            '_id': self.design['_id'],
            'views': {
                view: self.design['views'][view],   
            }
        }


class TestDocDesign(DesignTestCase):
    """
    Test each view function in the _design/doc design.
    """
    design = views.doc_design

    def test_type(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('type')
        db.save(design)

        self.assertEqual(
            db.view('doc', 'type'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True),
            {'rows': []}
        )

        id1 = random_id()
        doc1 = {
            '_id': id1,
            'type': 'misc',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('doc', 'type'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 'misc', 'id': id1, 'value': None}, 
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True),
            {
                'rows': [
                    {'key': None, 'value': 1}, 
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True, group=True),
            {
                'rows': [
                    {'key': 'misc', 'value': 1}, 
                ]
            }
        )

        id2 = random_id()
        doc2 = {
            '_id': id2,
            'type': 'abba',
        }
        db.save(doc2)
        self.assertEqual(
            db.view('doc', 'type'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 'abba', 'id': id2, 'value': None},
                    {'key': 'misc', 'id': id1, 'value': None}, 
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True),
            {
                'rows': [
                    {'key': None, 'value': 2}, 
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True, group=True),
            {
                'rows': [
                    {'key': 'abba', 'value': 1},
                    {'key': 'misc', 'value': 1}, 
                ]
            }
        )

        id3 = random_id()
        doc3 = {
            '_id': id3,
            'type': 'misc',
        }
        db.save(doc3)
        self.assertEqual(
            db.view('doc', 'type', reduce=True),
            {
                'rows': [
                    {'key': None, 'value': 3}, 
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'type', reduce=True, group=True),
            {
                'rows': [
                    {'key': 'abba', 'value': 1},
                    {'key': 'misc', 'value': 2}, 
                ]
            }
        )

    def test_time(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('time')
        db.save(design)

        self.assertEqual(
            db.view('doc', 'time'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        id1 = random_id()
        doc1 = {
            '_id': id1,
            'time': 21,
        }
        db.save(doc1)
        self.assertEqual(
            db.view('doc', 'time'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 21, 'id': id1, 'value': None},
                ]
            }
        )

        id2 = random_id()
        doc2 = {
            '_id': id2,
            'time': 19,
        }
        db.save(doc2)
        self.assertEqual(
            db.view('doc', 'time'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 19, 'id': id2, 'value': None},
                    {'key': 21, 'id': id1, 'value': None},
                ]
            }
        )
        self.assertEqual(
            db.view('doc', 'time', descending=True),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 21, 'id': id1, 'value': None},
                    {'key': 19, 'id': id2, 'value': None},
                ]
            }
        )

    def test_conflicts(self):
        server = microfiber.Server(self.env)
        A = server.database('a', True)
        B = server.database('b', True)

        # Only save design in B:
        design = self.build_view('conflicts')
        B.save(design)

        # B is empty:
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Build base revision in A:
        _id = random_id()
        doc_A = {'_id': _id}
        rev1 = A.save(doc_A)['rev']

        # Replicate from A to B:
        self.assertEqual(
            B.post({'docs': [doc_A], 'new_edits': False}, '_bulk_docs'),
            []
        )
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev1})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Create 2nd and 3rd revision in B:
        doc_B = doc_A.copy()
        m_B2 = random_id()
        doc_B['m'] = m_B2
        rev2B = B.save(doc_B)['rev']
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev2B, 'm': m_B2})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        m_B3 = random_id()
        doc_B['m'] = m_B3
        rev3B = B.save(doc_B)['rev']
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev3B, 'm': m_B3})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Create a conflicting 2nd revision in A:
        m_A2 = random_id()
        doc_A['m'] = m_A2
        rev2A = A.save(doc_A)['rev']

        # Replicate from A to B:
        self.assertEqual(
            B.post({'docs': [doc_A], 'new_edits': False}, '_bulk_docs'),
            []
        )
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev3B, 'm': m_B3})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'id': _id, 'key': 1, 'value': [rev2A]}
                ]
            }
        )

        # Create 4th revision in B:
        m_B4 = random_id()
        doc_B['m'] = m_B4
        rev4B = B.save(doc_B)['rev']
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev4B, 'm': m_B4})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'id': _id, 'key': 1, 'value': [rev2A]}
                ]
            }
        )

        # Create a conflicting 3rd revision in A:
        m_A3 = random_id()
        doc_A['m'] = m_A3
        rev3A = A.save(doc_A)['rev']

        # Replicate from A to B:
        self.assertEqual(
            B.post({'docs': [doc_A], 'new_edits': False}, '_bulk_docs'),
            []
        )
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev4B, 'm': m_B4})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'id': _id, 'key': 2, 'value': [rev3A, rev2A]}
                ]
            }
        )

        # Delete conflicts in B:
        docs = [
            {'_id': _id, '_rev': rev3A, '_deleted': True},
            {'_id': _id, '_rev': rev2A, '_deleted': True},
        ]
        B.post({'docs': docs, 'all_or_nothing': True}, '_bulk_docs')
        self.assertEqual(B.get(_id), {'_id': _id, '_rev': rev4B, 'm': m_B4})
        self.assertEqual(
            B.view('doc', 'conflicts'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )


class TestFileDesign(DesignTestCase):
    """
    Test each view function in the _design/file design.
    """
    design = views.file_design

    def test_stored(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('stored')
        db.save(design)

        self.assertEqual(
            db.view('file', 'stored'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True),
            {'rows': []}
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

        # Test when doc.bytes does not exist:
        self.assertEqual(
            db.view('file', 'stored'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': 0},
                    {'key': store_id2, 'id': _id, 'value': 0},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': store_id1,
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                    {
                        'key': store_id2,
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )

        # Test when doc.bytes is not a number:
        doc['bytes'] = 'foo'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'stored'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': 0},
                    {'key': store_id2, 'id': _id, 'value': 0},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': store_id1,
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                    {
                        'key': store_id2,
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )

        # Test when doc.bytes is an integer:
        doc['bytes'] = 42
        db.save(doc)
        self.assertEqual(
            db.view('file', 'stored'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': 42},
                    {'key': store_id2, 'id': _id, 'value': 42},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 84,
                            'sumsqr': 3528,
                            'min': 42,
                            'max': 42,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': store_id1,
                        'value': {
                            'count': 1,
                            'sum': 42,
                            'sumsqr': 1764,
                            'min': 42,
                            'max': 42,
                        },
                    },
                    {
                        'key': store_id2,
                        'value': {
                            'count': 1,
                            'sum': 42,
                            'sumsqr': 1764,
                            'min': 42,
                            'max': 42,
                        },
                    },
                ],
            }
        )

        # Make sure view func checks doc.type
        doc['type'] = 'dmedia/file2'
        db.save(doc)

        self.assertEqual(
            db.view('file', 'stored'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        self.assertEqual(
            db.view('file', 'stored', reduce=True),
            {'rows': []}
        )

    def test_nonzero(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('nonzero')
        db.save(design)

        self.assertEqual(
            db.view('file', 'nonzero'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when doc.stored doesn't exist
        _id = random_file_id()
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when doc.stored is empty
        doc['stored'] = {}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when copies is missing:
        (store_id1, store_id2) = sorted(random_id() for i in range(2))
        doc['stored'] = {
            store_id1: {},
            store_id2: {},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': None},
                    {'key': store_id2, 'id': _id, 'value': None},
                ],
            }
        )

        # Test that compare is done with !== 0:
        doc['stored'] = {
            store_id1: {'copies': '0'},
            store_id2: {'copies': False},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': None},
                    {'key': store_id2, 'id': _id, 'value': None},
                ],
            }
        )

        # Test when copies === 0:
        doc['stored'] = {
            store_id1: {'copies': 0},
            store_id2: {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test with typical values:
        doc['stored'] = {
            store_id1: {'copies': 2},
            store_id2: {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': _id, 'value': None},
                    {'key': store_id2, 'id': _id, 'value': None},
                ],
            }
        )

        # Make sure doc.type is considered
        doc['type'] = 'dmedia/files'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'nonzero'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_copies(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('copies')
        db.save(design)

        self.assertEqual(
            db.view('file', 'copies'),
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
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
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
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )

        # Make things work even if copies isn't a number
        doc['stored'] = {
            random_id(): {'copies': 'foo'},
            random_id(): {'copies': 'bar'},
            random_id(): {'copies': 'baz'},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
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
            random_id(): {'copies': -1},
            random_id(): {'copies': 1},
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 2, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
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
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 3, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
            {'rows': [], 'offset': 0, 'total_rows': 1},
        )

        # Check when each store provides 1 copy
        doc['stored'] = {
            random_id(): {'copies': 1},
            random_id(): {'copies': 1},
            random_id(): {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'copies'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 3, 'id': _id, 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'copies', endkey=2),
            {'rows': [], 'offset': 0, 'total_rows': 1},
        )

    def test_rank(self):
        ####################################################################
        # 1st, test sort order and other properties on a collection of docs:
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('rank')
        db.save(design)

        # Test when empty
        self.assertEqual(
            db.view('file', 'rank'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Create rank=(0 through 6) test data:
        ids = tuple(random_file_id() for i in range(7))
        stores = tuple(random_id() for i in range(3))
        docs = [
            {
                '_id': ids[0],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {},
            },
            {
                '_id': ids[1],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 0},
                },
            },
            {
                '_id': ids[2],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                },
            },
            {
                '_id': ids[3],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 0},
                },
            },
            {
                '_id': ids[4],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                },
            },
            {
                '_id': ids[5],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 0},
                },
            },
            {
                '_id': ids[6],
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 1},
                },
            },
        ]
        db.save_many(docs)

        # Test sorting, overall properties:
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 7,
                'rows': [
                    {'key': 0, 'id': ids[0], 'value': None},
                    {'key': 1, 'id': ids[1], 'value': None},
                    {'key': 2, 'id': ids[2], 'value': None},
                    {'key': 3, 'id': ids[3], 'value': None},
                    {'key': 4, 'id': ids[4], 'value': None},
                    {'key': 5, 'id': ids[5], 'value': None},
                    {'key': 6, 'id': ids[6], 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('file', 'rank', descending=True),
            {
                'offset': 0, 
                'total_rows': 7,
                'rows': [
                    {'key': 6, 'id': ids[6], 'value': None},
                    {'key': 5, 'id': ids[5], 'value': None},
                    {'key': 4, 'id': ids[4], 'value': None},
                    {'key': 3, 'id': ids[3], 'value': None},
                    {'key': 2, 'id': ids[2], 'value': None},
                    {'key': 1, 'id': ids[1], 'value': None},
                    {'key': 0, 'id': ids[0], 'value': None},
                ],
            },
        )

        ###################################################
        # 2nd, drill down on the minutia with a single doc:
        db = microfiber.Database('bar', self.env)
        db.put(None)
        design = self.build_view('rank')
        db.save(design)

        # Test when empty
        self.assertEqual(
            db.view('file', 'rank'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # rank should be 0 when doc['stored'] is missing, empty, or wrong type:
        _id = random_file_id()
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
            'origin': 'user',
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = {}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = ['hello', 'world']
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = 'helloworld'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = None
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = 17
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 0, 'id': _id, 'value': None},
                ],
            },
        )

        # Test with one location, broken doc['stored'][STORE_id]:
        doc['stored'] = {random_id(): 'blah blah broken'}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 1, 'id': _id, 'value': None},
                ],
            },
        )

        # Test with one location, broken doc['stored'][STORE_id]['copies']:
        doc['stored'] = {random_id(): {'copies': '2'}}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 1, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = {random_id(): {'copies': 'foo bar'}}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 1, 'id': _id, 'value': None},
                ],
            },
        )
        doc['stored'] = {random_id(): {'copies': -3}}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 1, 'id': _id, 'value': None},
                ],
            },
        )

        # Test with locations=1, durability=2:
        doc['stored'] = {random_id(): {'copies': 2}}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 3, 'id': _id, 'value': None},
                ],
            },
        )

        # Test with locations=2, durability=0:
        doc['stored'] = {
            random_id(): {'copies': 0},
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 2, 'id': _id, 'value': None},
                ],
            },
        )

        # Test with locations=2, durability=1:
        doc['stored'] = {
            random_id(): {'copies': 1},
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 3, 'id': _id, 'value': None},
                ],
            },
        )

        # Test that locations is clamped at 3:
        doc['stored'] = {
            random_id(): {'copies': 0},
            random_id(): {'copies': 0},
            random_id(): {'copies': 0},
            random_id(): {'copies': 0},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 3, 'id': _id, 'value': None},
                ],
            },
        )

        # Test that durability is clamped at 3:
        doc['stored'] = {random_id(): {'copies': 17}}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 4, 'id': _id, 'value': None},
                ],
            },
        )

        # Make sure doc['type'] is checked:
        doc['type'] = 'dmedia/guile'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure doc['orign'] is checked:
        doc['type'] = 'dmedia/file'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': 4, 'id': _id, 'value': None},
                ],
            },
        )
        doc['origin'] = 'render'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'rank'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        ######################################################################
        # 3rd, test assumptions about how "startkey_docid" and "key" interact:
        db = microfiber.Database('baz', self.env)
        db.put(None)
        design = self.build_view('rank')
        db.save(design)

        # Create rank=(0 through 6) test data:
        ranks = tuple(
            tuple(random_file_id() for j in range(17 + i))
            for i in range(7)
        )
        sorted_ranks = tuple(
            tuple(sorted(rank_n)) for rank_n in ranks
        )
        flattened = []
        for (n, rank_n) in enumerate(sorted_ranks):
            flattened.extend((n, _id) for _id in rank_n)
        flattened = tuple(flattened)
        self.assertEqual(len(flattened), 140)

        stores = tuple(random_id() for i in range(3))
        docs = []
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {},
            }
            for _id in ranks[0]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 0},
                },
            }
            for _id in ranks[1]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                },
            }
            for _id in ranks[2]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 0},
                },
            }
            for _id in ranks[3]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                },
            }
            for _id in ranks[4]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 0},
                },
            }
            for _id in ranks[5]
        )
        docs.extend(
            {
                '_id': _id,
                'type': 'dmedia/file',
                'origin': 'user',
                'stored': {
                    stores[0]: {'copies': 1},
                    stores[1]: {'copies': 1},
                    stores[2]: {'copies': 1},
                },
            }
            for _id in ranks[6]
        )
        self.assertEqual(len(docs), 140)
        db.save_many(docs)

        # Test that sorting is being done by (key, _id):
        self.assertEqual(
            db.view('file', 'rank'),
            {
                'offset': 0, 
                'total_rows': 140,
                'rows': [
                    {'key': n, 'id': ID, 'value': None}
                    for (n, ID) in flattened
                ],
            },
        )

        # Test that sorting is being done by _id within a single key, then test
        # that we can use "startkey_docid" as expected:
        offset = 0
        for (n, rank_n) in enumerate(sorted_ranks):
            self.assertEqual(
                db.view('file', 'rank', key=n),
                {
                    'offset': offset, 
                    'total_rows': 140,
                    'rows': [
                        {'key': n, 'id': ID, 'value': None}
                        for ID in rank_n
                    ],
                },
            )
            for i in range(len(rank_n)):
                self.assertEqual(
                    db.view('file', 'rank', key=n, startkey_docid=rank_n[i]),
                    {
                        'offset': offset + i, 
                        'total_rows': 140,
                        'rows': [
                            {'key': n, 'id': ID, 'value': None}
                            for ID in rank_n[i:]
                        ],
                    },
                )
            offset += len(rank_n)

    def test_store_reclaimable(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('store-reclaimable')
        db.save(design)

        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        _id = random_id(DIGEST_BYTES)
        atime = int(time.time())
        stores = sorted(random_id() for i in range(4))
        doc = {
            '_id': _id,
            'type': 'dmedia/file',
            'origin': 'user',
            'atime': atime,
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc['stored'] = {}
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when copies is missing in something reclaimable:
        doc['stored'] = {
            stores[0]: {'copies': 3},
            stores[1]: {},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Test when copies is a junk value in something reclaimable:
        doc['stored'] = {
            stores[0]: {'copies': 3},
            stores[2]: {'copies': ['hello', 'world']},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Test when copies is negative in something reclaimable:
        doc['stored'] = {
            stores[2]: {'copies': 3},
            stores[3]: {'copies': -1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[3], atime], 'id': _id, 'value': None},
                ],
            },
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
            db.view('file', 'store-reclaimable'),
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
            db.view('file', 'store-reclaimable'),
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
            db.view('file', 'store-reclaimable'),
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

        # Pin one copy, make sure it's not reclaimable
        doc['stored'] = {
            stores[0]: {'copies': 2},
            stores[1]: {'copies': 2, 'pinned': True},
            stores[2]: {'copies': 2},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 2,
                'rows': [
                    {'key': [stores[0], atime], 'id': _id, 'value': None},
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Pin all copies, make sure none are reclaimable
        doc['stored'] = {
            stores[0]: {'copies': 2, 'pinned': True},
            stores[1]: {'copies': 2, 'pinned': True},
            stores[2]: {'copies': 2, 'pinned': True},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Any one of these could be reclaimed:
        doc['stored'] = {
            stores[0]: {'copies': 1},
            stores[1]: {'copies': 1},
            stores[2]: {'copies': 1},
            stores[3]: {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
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

        # Two of these can be reclaimed (just not at once):
        doc['stored'] = {
            stores[0]: {'copies': 1},
            stores[1]: {'copies': 2},
            stores[2]: {'copies': 1},
        }
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 2,
                'rows': [
                    {'key': [stores[0], atime], 'id': _id, 'value': None},
                    {'key': [stores[2], atime], 'id': _id, 'value': None},
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
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], atime], 'id': _id, 'value': None},
                ],
            },
        )

        # Test that non-numeric atime values get replaced with None
        doc['atime'] = '1234567890'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], None], 'id': _id, 'value': None},
                ],
            },
        )
        doc['atime'] = ['what', 'tangled', 'web']
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], None], 'id': _id, 'value': None},
                ],
            },
        )

        # But negative atimes values should be passed through:
        doc['atime'] = -1234567890
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {
                'offset': 0, 
                'total_rows': 1,
                'rows': [
                    {'key': [stores[1], -1234567890], 'id': _id, 'value': None},
                ],
            },
        )

        # Test that doc['type'] is considered
        doc['type'] = 'dmedia/file2'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test that doc['origin'] must be 'user'
        doc['type'] = 'dmedia/file'
        doc['origin'] = 'render'
        db.save(doc)
        self.assertEqual(
            db.view('file', 'store-reclaimable'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_downgrade_by_mtime(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('downgrade-by-mtime')
        db.save(design)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        id1 = random_file_id()
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when there are 2 stores
        store_id1 = random_id()
        store_id2 = random_id()
        doc1['stored'] = {
            store_id1: {
                'copies': 1,
                'mtime': 1001,
            },
            store_id2: {
                'copies': 1,
                'mtime': 1003,
            },
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 1001, 'id': id1, 'value': store_id1},
                    {'key': 1003, 'id': id1, 'value': store_id2},
                ]
            }
        )

        # Test that stores are excluded when copies === 0
        doc1['stored'][store_id1]['copies'] = 0
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 1003, 'id': id1, 'value': store_id2},
                ]
            }
        )
        doc1['stored'][store_id2]['copies'] = 0
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Add another doc
        id2 = random_file_id()
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
            'stored': {
                store_id1: {
                    'copies': 2,
                    'mtime': 1002,
                },
                store_id2: {
                    'copies': 19,
                    'mtime': 1004,
                },
            }
        }
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 1002, 'id': id2, 'value': store_id1},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure it's filtering with !== 0
        doc1['stored'][store_id1]['copies'] = '0'
        doc1['stored'][store_id2]['copies'] = False
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': 1001, 'id': id1, 'value': store_id1},
                    {'key': 1002, 'id': id2, 'value': store_id1},
                    {'key': 1003, 'id': id1, 'value': store_id2},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure non-numeric mtime can't mess up sort order:
        doc2['stored'][store_id1]['mtime'] = '1005'
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': None, 'id': id2, 'value': store_id1},
                    {'key': 1001, 'id': id1, 'value': store_id1},
                    {'key': 1003, 'id': id1, 'value': store_id2},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure verified can't be a number
        doc1['stored'][store_id1]['verified'] = 123
        doc2['stored'][store_id1]['verified'] = 456
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 1003, 'id': id1, 'value': store_id2},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure doc.type is being checked
        doc1['type'] = 'dmedia/foo'
        doc2['type'] = 'dmedia/bar'
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'downgrade-by-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_downgrade_by_verified(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('downgrade-by-verified')
        db.save(design)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        id1 = random_file_id()
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when there are 2 stores
        store_id1 = random_id()
        store_id2 = random_id()
        doc1['stored'] = {
            store_id1: {
                'copies': 1,
                'verified': 1001,
            },
            store_id2: {
                'copies': 1,
                'verified': 1003,
            },
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 1001, 'id': id1, 'value': store_id1},
                    {'key': 1003, 'id': id1, 'value': store_id2},
                ]
            }
        )

        # Test that stores are excluded when copies === 0
        doc1['stored'][store_id1]['copies'] = 0
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 1003, 'id': id1, 'value': store_id2},
                ]
            }
        )
        doc1['stored'][store_id2]['copies'] = 0
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Add another doc
        id2 = random_file_id()
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
            'stored': {
                store_id1: {
                    'copies': 2,
                    'verified': 1002,
                },
                store_id2: {
                    'copies': 19,
                    'verified': 1004,
                },
            }
        }
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 1002, 'id': id2, 'value': store_id1},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure it's filtering with !== 0
        doc1['stored'][store_id1]['copies'] = '0'
        doc1['stored'][store_id2]['copies'] = False
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': 1001, 'id': id1, 'value': store_id1},
                    {'key': 1002, 'id': id2, 'value': store_id1},
                    {'key': 1003, 'id': id1, 'value': store_id2},
                    {'key': 1004, 'id': id2, 'value': store_id2},
                ]
            }
        )

        # Make sure doc.type is being checked
        doc1['type'] = 'dmedia/foo'
        doc2['type'] = 'dmedia/bar'
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'downgrade-by-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_store_downgraded(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('store-downgraded')
        db.save(design)
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        
        (id1, id2) = sorted(random_file_id() for i in range(2))
        (store_id1, store_id2, store_id3) = sorted(random_id() for i in range(3))

        # Make sure things are well behaved even when doc['stored'] is missed:
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
        }
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
        }
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        doc2['stored'] = {}
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when across 3 stores:
        doc1['stored'] = {
            store_id1: {
                'copies': 0,
                'mtime': 1001,
            },
            store_id2: {
                'copies': 0,
                'mtime': 1002,
            },
        }
        doc2['stored'] = {
            store_id1: {
                'copies': 0,
                'mtime': 1003,
            },
            store_id3: {
                'copies': 0,
                'mtime': 1004,
            },
        }
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': store_id1, 'id': id1, 'value': None},
                    {'key': store_id1, 'id': id2, 'value': None},
                    {'key': store_id2, 'id': id1, 'value': None},
                    {'key': store_id3, 'id': id2, 'value': None},
                ]
            }
        )

        # Filter down to just what's in store_id1:
        self.assertEqual(
            db.view('file', 'store-downgraded', key=store_id1),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': store_id1, 'id': id1, 'value': None},
                    {'key': store_id1, 'id': id2, 'value': None},
                ]
            }
        )

        # Filter down to just what's in store_id2:
        self.assertEqual(
            db.view('file', 'store-downgraded', key=store_id2),
            {
                'offset': 2,
                'total_rows': 4,
                'rows': [
                    {'key': store_id2, 'id': id1, 'value': None},
                ]
            }
        )

        # Filter down to just what's in store_id3:
        self.assertEqual(
            db.view('file', 'store-downgraded', key=store_id3),
            {
                'offset': 3,
                'total_rows': 4,
                'rows': [
                    {'key': store_id3, 'id': id2, 'value': None},
                ]
            }
        )

        # Make sure it's filtering with (typeof verified != 'number'):
        doc1['stored'][store_id1]['verified'] = 1234567890
        doc1['stored'][store_id2]['verified'] = 17
        doc2['stored'][store_id1]['verified'] = 18
        doc2['stored'][store_id3]['verified'] = 19
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # As above, now make sure a numeric 'verified' excludes rows:
        doc1['stored'][store_id1]['verified'] = '17'
        doc1['stored'][store_id2]['verified'] = True
        doc2['stored'][store_id1]['verified'] = {'hello': 'world'}
        doc2['stored'][store_id3]['verified'] = ['hello', 'naughty', 'nurse']
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': store_id1, 'id': id1, 'value': None},
                    {'key': store_id1, 'id': id2, 'value': None},
                    {'key': store_id2, 'id': id1, 'value': None},
                    {'key': store_id3, 'id': id2, 'value': None},
                ]
            }
        )

        # Make sure it's filtering with (copies === 0):
        doc1['stored'][store_id1]['copies'] = '0'
        doc1['stored'][store_id2]['copies'] = False
        doc2['stored'][store_id1]['copies'] = 1
        doc2['stored'][store_id3]['copies'] = -1
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Change a few back to {'copies': 0}
        doc1['stored'][store_id1]['copies'] = 0
        doc2['stored'][store_id3]['copies'] = 0
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': store_id1, 'id': id1, 'value': None},
                    {'key': store_id3, 'id': id2, 'value': None},
                ]
            }
        )

        # Make sure doc.type is being checked
        doc1['type'] = 'dmedia/foo'
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': store_id3, 'id': id2, 'value': None},
                ]
            }
        )
        doc2['type'] = 'dmedia/bar'
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'store-downgraded'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_store_mtime(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('store-mtime')
        db.save(design)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Make sure things are well behaved even when doc['stored'] is missed:
        id1 = random_file_id()
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test when there are 2 stores
        (store_id1, store_id2) = sorted(random_id() for i in range(2))
        doc1['stored'] = {
            store_id1: {
                'copies': 1,
                'mtime': 1001,
            },
            store_id2: {
                'copies': 1,
                'mtime': 1003,
            },
        }
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )

        # Add another doc
        id2 = random_file_id()
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
            'stored': {
                store_id1: {
                    'copies': 2,
                    'mtime': 1002,
                },
                store_id2: {
                    'copies': 19,
                    'mtime': 1004,
                },
            }
        }
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                    {'key': [store_id1, 1002], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                    {'key': [store_id2, 1004], 'id': id2, 'value': None},
                ]
            }
        )

        # Filter down to just what's in store_id1:
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id1, None],
                endkey=[store_id1, 1002],
            ),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                    {'key': [store_id1, 1002], 'id': id2, 'value': None},
                ]
            }
        )
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id1, None],
                endkey=[store_id1, 1001],
            ),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                ]
            }
        )

        # Filter down to just what's in store_id2:
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id2, None],
                endkey=[store_id2, 1004],
            ),
            {
                'offset': 2,  # Easy unit test gotcha!
                'total_rows': 4,
                'rows': [
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                    {'key': [store_id2, 1004], 'id': id2, 'value': None},
                ]
            }
        )
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id2, None],
                endkey=[store_id2, 1003],
            ),
            {
                'offset': 2,  # Easy unit test gotcha!
                'total_rows': 4,
                'rows': [
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )

        # Make sure it's filtering with (typeof verified != 'number'):
        doc1['stored'][store_id1]['verified'] = '17'
        doc1['stored'][store_id2]['verified'] = True
        doc2['stored'][store_id1]['verified'] = {'hello': 'world'}
        doc2['stored'][store_id2]['verified'] = ['hello', 'naughty', 'nurse']
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                    {'key': [store_id1, 1002], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                    {'key': [store_id2, 1004], 'id': id2, 'value': None},
                ]
            }
        )

        # Make sure bad mtimes values don't do anything crazy to the sort order:
        doc2['stored'][store_id1]['mtime'] = '1000'
        doc2['stored'][store_id2]['mtime'] = 'weird effect on sort order'
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, None], 'id': id2, 'value': None},
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                    {'key': [store_id2, None], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id1, None],
                endkey=[store_id1, 1001],
            ),
            {
                'offset': 0,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id1, None], 'id': id2, 'value': None},
                    {'key': [store_id1, 1001], 'id': id1, 'value': None},
                ]
            }
        )
        self.assertEqual(
            db.view('file', 'store-mtime',
                startkey=[store_id2, None],
                endkey=[store_id2, 1003],
            ),
            {
                'offset': 2,
                'total_rows': 4,
                'rows': [
                    {'key': [store_id2, None], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )

        # Make sure a numeric 'verified' excludes rows:
        doc1['stored'][store_id1]['verified'] = 123
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 3,
                'rows': [
                    {'key': [store_id1, None], 'id': id2, 'value': None},
                    {'key': [store_id2, None], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )

        # Make rows are filtered with copies !== 0:
        doc2['stored'][store_id2]['copies'] = 0
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': [store_id1, None], 'id': id2, 'value': None},
                    {'key': [store_id2, 1003], 'id': id1, 'value': None},
                ]
            }
        )

        # Make sure doc.type is being checked
        doc1['type'] = 'dmedia/foo'
        doc2['type'] = 'dmedia/bar'
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-mtime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

    def test_store_verified(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('store-verified')
        db.save(design)
        self.assertEqual(
            db.view('file', 'store-verified'),
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
            db.view('file', 'store-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # And when doc['stored'] is empty:
        doc1['stored'] = {}
        db.save(doc1)
        self.assertEqual(
            db.view('file', 'store-verified'),
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
            db.view('file', 'store-verified'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        # Test sort order within the same store: 0 < 1234567890
        store_id = random_id()
        doc1['stored'] = {
            store_id: {'verified': 1234567890},
        }
        doc2['stored'] = {
            store_id: {'verified': 0},
        }
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-verified'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': [store_id, 0], 'id': id2, 'value': None},
                    {'key': [store_id, 1234567890], 'id': id1, 'value': None},
                ]
            },
        )

        # Test with non-numeric 'verified':
        doc2['stored'][store_id]['verified'] = '17'
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'store-verified'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': [store_id, 1234567890], 'id': id1, 'value': None},
                ]
            },
        )
        doc1['stored'][store_id]['verified'] = ['holly', 'crap']
        doc2['stored'][store_id]['verified'] = 3469
        db.save_many([doc1, doc2])
        self.assertEqual(
            db.view('file', 'store-verified'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': [store_id, 3469], 'id': id2, 'value': None},
                ]
            },
        )

    def test_origin(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('origin')
        db.save(design)

        self.assertEqual(
            db.view('file', 'origin'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True),
            {'rows': []}
        )

        id1 = random_id(DIGEST_BYTES)
        id2 = random_id(DIGEST_BYTES)
        doc1 = {
            '_id': id1,
            'type': 'dmedia/file',
            'origin': 'user',
        }
        doc2 = {
            '_id': id2,
            'type': 'dmedia/file',
            'origin': 'proxy',
        }
        db.save(doc1)
        db.save(doc2)

        # Test when doc.bytes does not exist:
        self.assertEqual(
            db.view('file', 'origin'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 'proxy', 'id': id2, 'value': 0},
                    {'key': 'user', 'id': id1, 'value': 0},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': 'proxy',
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                    {
                        'key': 'user',
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )

        # Test when doc.bytes is not a number:
        doc1['bytes'] = 'foo'
        doc2['bytes'] = ['bar']
        db.save(doc1)
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'origin'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 'proxy', 'id': id2, 'value': 0},
                    {'key': 'user', 'id': id1, 'value': 0},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': 'proxy',
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                    {
                        'key': 'user',
                        'value': {
                            'count': 1,
                            'sum': 0,
                            'sumsqr': 0,
                            'min': 0,
                            'max': 0,
                        },
                    },
                ],
            }
        )

        # Test when doc.bytes is an integer:
        doc1['bytes'] = 17
        doc2['bytes'] = 18
        db.save(doc1)
        db.save(doc2)
        self.assertEqual(
            db.view('file', 'origin'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 'proxy', 'id': id2, 'value': 18},
                    {'key': 'user', 'id': id1, 'value': 17},
                ]
            },
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True),
            {
                'rows': [
                    {
                        'key': None,
                        'value': {
                            'count': 2,
                            'sum': 35,
                            'sumsqr': 613,
                            'min': 17,
                            'max': 18,
                        },
                    },
                ],
            }
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True, group=True),
            {
                'rows': [
                    {
                        'key': 'proxy',
                        'value': {
                            'count': 1,
                            'sum': 18,
                            'sumsqr': 324,
                            'min': 18,
                            'max': 18,
                        },
                    },
                    {
                        'key': 'user',
                        'value': {
                            'count': 1,
                            'sum': 17,
                            'sumsqr': 289,
                            'min': 17,
                            'max': 17,
                        },
                    },
                ],
            }
        )

        # Make sure view func checks doc.type
        doc1['type'] = 'dmedia/file2'
        doc2['type'] = 'dmedia/file2'
        db.save(doc1)
        db.save(doc2)

        self.assertEqual(
            db.view('file', 'origin'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
        self.assertEqual(
            db.view('file', 'origin', reduce=True),
            {'rows': []}
        )


class TestStoreDesign(DesignTestCase):
    """
    Test each view function in the _design/store design.
    """
    design = views.store_design

    def test_atime(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('atime')
        db.save(design)

        self.assertEqual(
            db.view('store', 'atime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        docs = []
        for i in range(9):
            doc = {
                '_id': random_id(),
                'type': 'dmedia/store',
                'atime': 100 + i
            }
            docs.append(doc)
        db.save_many(docs)
        self.assertEqual(
            db.view('store', 'atime'),
            {
                'offset': 0,
                'total_rows': 9,
                'rows': [
                    {'key': D['atime'], 'id': D['_id'], 'value': None}
                    for D in docs
                ],
            },
        )

        # Test our assumputions about endkey
        self.assertEqual(
            db.view('store', 'atime', endkey=99),
            {'offset': 0, 'total_rows': 9, 'rows': []},
        )
        self.assertEqual(
            db.view('store', 'atime', endkey=100),
            {
                'offset': 0,
                'total_rows': 9,
                'rows': [
                    {'key': 100, 'id': docs[0]['_id'], 'value': None},
                ],
            },
        )
        self.assertEqual(
            db.view('store', 'atime', endkey=102),
            {
                'offset': 0,
                'total_rows': 9,
                'rows': [
                    {'key': 100, 'id': docs[0]['_id'], 'value': None},
                    {'key': 101, 'id': docs[1]['_id'], 'value': None},
                    {'key': 102, 'id': docs[2]['_id'], 'value': None},
                ],
            },
        )

        # Test when atime and time are missing
        doc = docs[-1]
        del doc['atime']
        db.save(doc)
        self.assertEqual(
            db.view('store', 'atime', endkey=102),
            {
                'offset': 0,
                'total_rows': 9,
                'rows': [
                    {'key': None, 'id': doc['_id'], 'value': None},
                    {'key': 100, 'id': docs[0]['_id'], 'value': None},
                    {'key': 101, 'id': docs[1]['_id'], 'value': None},
                    {'key': 102, 'id': docs[2]['_id'], 'value': None},
                ],
            },
        )

        # Test when atime is missing, but time is present
        doc = docs[-1]
        doc['time'] = 50.5
        db.save(doc)
        self.assertEqual(
            db.view('store', 'atime', endkey=102),
            {
                'offset': 0,
                'total_rows': 9,
                'rows': [
                    {'key': 50.5, 'id': doc['_id'], 'value': None},
                    {'key': 100, 'id': docs[0]['_id'], 'value': None},
                    {'key': 101, 'id': docs[1]['_id'], 'value': None},
                    {'key': 102, 'id': docs[2]['_id'], 'value': None},
                ],
            },
        )

        # Make sure doc.type is being checked
        for doc in docs:
            doc['type'] = 'dmedia/other'
        db.save_many(docs)
        self.assertEqual(
            db.view('store', 'atime'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )


class TestJobDesign(DesignTestCase):
    """
    Test each view function in the _design/job design.
    """
    design = views.job_design

    def test_waiting(self):
        db = microfiber.Database('foo', self.env)
        db.put(None)
        design = self.build_view('waiting')
        db.save(design)

        self.assertEqual(
            db.view('job', 'waiting'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )

        id1 = random_id()
        doc1 = {
            '_id': id1,
            'time': 17,
            'type': 'dmedia/job',
            'status': 'waiting',
        }
        db.save(doc1)
        self.assertEqual(
            db.view('job', 'waiting'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 17, 'id': id1, 'value': None},
                ]
            },
        )

        # Add another doc, make sure the sort order is correct
        id2 = random_id()
        doc2 = {
            '_id': id2,
            'time': 19,
            'type': 'dmedia/job',
            'status': 'waiting',
        }
        db.save(doc2)
        self.assertEqual(
            db.view('job', 'waiting'),
            {
                'offset': 0,
                'total_rows': 2,
                'rows': [
                    {'key': 17, 'id': id1, 'value': None},
                    {'key': 19, 'id': id2, 'value': None},
                ]
            },
        )

        # Make sure doc['status'] is considered
        doc1['status'] = 'executing'
        db.save(doc1)
        self.assertEqual(
            db.view('job', 'waiting'),
            {
                'offset': 0,
                'total_rows': 1,
                'rows': [
                    {'key': 19, 'id': id2, 'value': None},
                ]
            },
        )

        # Make sure doc['type'] is considered
        doc2['type'] = 'dmedia/jobs'
        db.save(doc2)
        self.assertEqual(
            db.view('job', 'waiting'),
            {'rows': [], 'offset': 0, 'total_rows': 0},
        )
