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
Unit tests for `dmedia.ui` module.
"""

from unittest import TestCase
from os import path
from base64 import b64encode
from dmedia import ui, datadir


class test_functions(TestCase):

    def test_datafile(self):
        f = ui.datafile
        self.assertEqual(
            f('foo.xml'),
            path.join(datadir, 'foo.xml')
        )

    def test_datafile_comment(self):
        f = ui.datafile_comment
        self.assertEqual(
            f('foo.xml'),
            '/* ' + path.join(datadir, 'foo.xml') + ' */\n'
        )

    def test_load_datafile(self):
        f = ui.load_datafile
        filename = path.join(datadir, 'dmedia.js')
        self.assertEqual(
            f('dmedia.js'),
            open(filename, 'r').read()
        )

    def test_inline_datafile(self):
        f = ui.inline_datafile
        filename = path.join(datadir, 'dmedia.js')
        comment = '/* ' + filename + ' */\n'
        self.assertEqual(
            f('dmedia.js'),
            comment + open(filename, 'rb').read()
        )

    def test_encode_datafile(self):
        f = ui.encode_datafile
        filename = path.join(datadir, 'style.css')
        self.assertEqual(
            f('style.css'),
            b64encode(open(filename, 'rb').read())
        )
