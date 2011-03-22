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
Test JavaScript included in dmedia.
"""

from dmedia.js import JSTestCase
from dmedia.ui import datafile

class TestCouch(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('test_couch.js'),
    )

    def test_stuff(self):
        """
        Easy example test.

        When you call self.run_js(), it will execute the py.test_stuff()
        JavaScript function defined in the dmedia/data/test_couch.js.
        """
        self.run_js()

    def test_junk(self):
        """
        Same as above
        """
        self.run_js()

    def test_init(self):
        self.run_js()

    def test_path(self):
        self.run_js()

    def test_request(self):
        self.run_js()
