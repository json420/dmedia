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

from dmedia import views

from microfiber import Database

from .couch import CouchCase


class test_functions(TestCase):
    def test_build_design_doc(self):
        f = views.build_design_doc
        views_ = (
            ('bytes', 'foo', '_sum'),
            ('mtime', 'bar', None),
        )
        self.assertEqual(f('file', views_),
            {
                '_id': '_design/file',
                'language': 'javascript',
                'views': {
                    'bytes': {
                        'map': 'foo',
                        'reduce': '_sum',
                    },
                    'mtime': {
                        'map': 'bar',
                    },
                }
            }
        )


class TestCouchFunctions(CouchCase):
    def test_update_design_doc(self):
        f = views.update_design_doc
        db = Database('dmedia', self.env)
        db.put(None)

        # Test when design doesn't exist:
        doc = views.build_design_doc('file',
            [('stored', views.file_stored, '_sum')]
        )
        self.assertEqual(f(db, doc), 'new')
        self.assertTrue(db.get('_design/file')['_rev'].startswith('1-'))

        # Test when design is same:
        doc = views.build_design_doc('file',
            [('stored', views.file_stored, '_sum')]
        )
        self.assertEqual(f(db, doc), 'same')
        self.assertTrue(db.get('_design/file')['_rev'].startswith('1-'))

        # Test when design is changed:
        doc = views.build_design_doc('file',
            [('stored', views.file_bytes, '_sum')]
        )
        self.assertEqual(f(db, doc), 'changed')
        self.assertTrue(db.get('_design/file')['_rev'].startswith('2-'))

        # Again test when design is same:
        doc = views.build_design_doc('file',
            [('stored', views.file_bytes, '_sum')]
        )
        self.assertEqual(f(db, doc), 'same')
        self.assertTrue(db.get('_design/file')['_rev'].startswith('2-'))

    def test_init_views(self):
        db = Database('dmedia', self.env)
        db.put(None)

        views.init_views(db)
        for (name, views_) in views.designs:
            doc = views.build_design_doc(name, views_)
            saved = db.get(doc['_id'])
            doc['_rev'] = saved['_rev']
            self.assertEqual(saved, doc)
