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

from unittest import TestCase

import couchdb
from couchdb import ResourceNotFound
from desktopcouch.application.platform import find_port
from desktopcouch.application.local_files import get_oauth_tokens
from desktopcouch.records.http import OAuthSession

from dmedia import abstractcouch

from .helpers import raises


def dc_env(dbname='test_dmedia'):
    """
    Create desktopcouch environment.
    """
    port = find_port()
    return {
        'dbname': dbname,
        'port': port,
        'url': 'http://localhost:%d/' % port,
        'oauth': get_oauth_tokens(),
    }


class test_functions(TestCase):
    def tearDown(self):
        if abstractcouch.OAuthSession is None:
            abstractcouch.OAuthSession = OAuthSession

    def test_get_server(self):
        f = abstractcouch.get_server

        # Test with only url
        s = f({'url': 'http://localhost:5984/'})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:5984/'>")

        # Test with desktopcouch
        env = dc_env()
        s = f(env)
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(
            repr(s),
            "<Server 'http://localhost:%d/'>" % find_port()
        )

        # Test when OAuthSession is not imported, oauth is provided
        abstractcouch.OAuthSession = None
        e = raises(ValueError, f, env)
        self.assertEqual(
            str(e),
            "provided env['oauth'] but OAuthSession not available: %r" % (env,)
        )

        # Test when OAuthSession is not imported, oauth not provided
        s = f({'url': 'http://localhost:666/'})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:666/'>")

    def test_get_db(self):
        f = abstractcouch.get_db
        env = dc_env('test_dmedia')
        server = abstractcouch.get_server(env)

        # Make sure database doesn't exist:
        try:
            server.delete('test_dmedia')
        except ResourceNotFound:
            pass

        # Test when db does not exist, server not provided
        self.assertNotIn('test_dmedia', server)
        db = f(env)
        self.assertTrue(isinstance(db, couchdb.Database))
        self.assertEqual(repr(db), "<Database 'test_dmedia'>")
        self.assertEqual(db.info()['db_name'], 'test_dmedia')
        self.assertIn('test_dmedia', server)

        # Test when db exists, server not provided
        db = f(env)
        self.assertTrue(isinstance(db, couchdb.Database))
        self.assertEqual(repr(db), "<Database 'test_dmedia'>")
        self.assertEqual(db.info()['db_name'], 'test_dmedia')
        self.assertIn('test_dmedia', server)

        # Test when db does not exist, server *is* provided
        server.delete('test_dmedia')
        self.assertNotIn('test_dmedia', server)
        db = f(env, server=server)
        self.assertTrue(isinstance(db, couchdb.Database))
        self.assertEqual(repr(db), "<Database 'test_dmedia'>")
        self.assertEqual(db.info()['db_name'], 'test_dmedia')
        self.assertIn('test_dmedia', server)

        # Test when db exists, server *is* provided
        db = f(env, server=server)
        self.assertTrue(isinstance(db, couchdb.Database))
        self.assertEqual(repr(db), "<Database 'test_dmedia'>")
        self.assertEqual(db.info()['db_name'], 'test_dmedia')
        self.assertIn('test_dmedia', server)
