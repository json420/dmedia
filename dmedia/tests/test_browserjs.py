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
Test the browser.js JavaScript.
"""

from dmedia.js import JSTestCase
from dmedia.ui import datafile

class TestBrowser(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('browser.js'),
        datafile('test_browser.js'),
    )

    def test_dollar(self):
        """
        Test the $() JavaScript function.
        """
        self.run_js()

    def test_dollar_el(self):
        """
        Test the $el() JavaScript function.
        """
        self.run_js()

    def test_init(self):
        self.run_js()
