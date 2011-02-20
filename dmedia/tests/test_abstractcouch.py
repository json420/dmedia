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
from desktopcouch.application.platform import find_port
from desktopcouch.application.local_files import get_oauth_tokens
from desktopcouch.records.http import OAuthSession

from dmedia import abstractcouch

from .helpers import raises

class test_functions(TestCase):
    def setUp(self):
        abstractcouch.OAuthSession = OAuthSession

    def test_get_couchdb_server(self):
        f = abstractcouch.get_couchdb_server

        # Test with empty env
        s = f({})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:5984/'>")

        # Test with only url
        s = f({'url': 'http://localhost:5984/'})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:5984/'>")

        # Test with desktopcouch
        env = {
            'url': 'http://localhost:%d/' % find_port(),
            'oauth': get_oauth_tokens(),
        }
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
        s = f({})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:5984/'>")
        s = f({'url': 'http://localhost:5984/'})
        self.assertTrue(isinstance(s, couchdb.Server))
        self.assertEqual(repr(s), "<Server 'http://localhost:5984/'>")
