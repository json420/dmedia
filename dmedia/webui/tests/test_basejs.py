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
Test the base.js JavaScript.
"""

from dmedia.webui.js import JSTestCase
from dmedia.webui.util import datafile

class TestFunctions(JSTestCase):
    js_files = (
        datafile('base.js'),
        datafile('test_base.js'),
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

    def test_dollar_replace(self):
        """
        Test the $replace() JavaScript function.
        """
        self.run_js()

    def test_dollar_hide(self):
        """
        Test the $hide() JavaScript function.
        """
        self.run_js()

    def test_dollar_show(self):
        """
        Test the $show() JavaScript function.
        """
        self.run_js()

    def test_minsec(self):
        self.run_js()

    def test_todata(self):
        self.run_js()