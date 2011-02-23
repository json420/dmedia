# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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
Base class for CouchDB tests.
"""

from unittest import TestCase

import couchdb

from dmedia.abstractcouch import get_env, get_couchdb_server
from dmedia.util import random_id
from .helpers import TempHome


class CouchCase(TestCase):
    """
    Base class for tests that talk to CouchDB.

    So that a user's production data doesn't get hosed, all tests are run in the
    ``"dmedia_test"`` database.

    FIXME: This isn't the best solution, but some changes in desktopcouch in
    Natty make it difficult for 3rd party apps to use dc-test idioms:

        https://bugs.launchpad.net/desktopcouch/+bug/694909
    """

    def setUp(self):
        self.home = TempHome()
        self.dbname = 'dmedia_test'
        self.env = get_env(self.dbname)
        server = get_couchdb_server(self.env)
        try:
            del server[self.dbname]
        except couchdb.ResourceNotFound:
            pass
        self.machine_id = random_id()
        self.env['machine_id'] = self.machine_id

    def tearDown(self):
        self.home = None
        self.dbname = None
        self.env = None
