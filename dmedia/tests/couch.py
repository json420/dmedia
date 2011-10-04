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

import usercouch.misc
from microfiber import random_id


class CouchCase(usercouch.misc.CouchTestCase):
    """
    Base class for tests that talk to CouchDB.

    So that a user's production data doesn't get hosed, a CouchDB instance is
    is created for each test case, using temporary files, and destroyed at the
    end of each test case.
    """

    def setUp(self):
        super().setUp()
        self.machine_id = random_id()
        self.env['machine_id'] = self.machine_id

