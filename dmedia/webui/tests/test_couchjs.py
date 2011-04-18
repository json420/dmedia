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
Test the couch.js JavaScript.
"""

from dmedia.webui.js import JSTestCase
from dmedia.webui.util import datafile


class TestCouchRequest(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('test_couch.js'),
    )

    def test_request(self):
        self.run_js()

    def test_async_request(self):
        self.run_js()


class TestCouchBase(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('test_couch.js'),
    )

    def test_init(self):
        self.run_js()

    def test_path(self):
        self.run_js()

    def test_post(self):
        self.run_js()

    def test_put(self):
        self.run_js()

    def test_get(self):
        self.run_js()

    def test_delete(self):
        self.run_js()

    def test_request(self):
        self.run_js()


class TestServer(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('test_couch.js'),
    )

    def test_database(self):
        self.run_js()


class TestDatabase(JSTestCase):
    js_files = (
        datafile('couch.js'),
        datafile('test_couch.js'),
    )

    def test_save(self):
        self.run_js()

    def test_bulksave(self):
        self.run_js()

    def test_view(self):
        self.run_js()
