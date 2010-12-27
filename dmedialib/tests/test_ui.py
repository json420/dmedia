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
Unit tests for `dmedialib.ui` module.
"""

from unittest import TestCase
from os import path
from genshi.template import MarkupTemplate
from dmedialib import ui, datadir


class test_functions(TestCase):

    def test_datafile(self):
        f = ui.datafile
        self.assertEqual(
            f('foo.xml'),
            path.join(datadir, 'foo.xml')
        )


    def test_load_datafile(self):
        f = ui.load_datafile
        mootools = path.join(datadir, 'mootools.js')
        self.assertEqual(
            f('mootools.js'),
            open(mootools, 'r').read()
        )


    def test_load_template(self):
        f = ui.load_template
        xml = path.join(datadir, 'toplevel.xml')
        t = f('toplevel.xml')
        self.assertTrue(isinstance(t, MarkupTemplate))
        self.assertEqual(t.filename, xml)


    def test_render_template(self):
        f = ui.render_template
        t = ui.load_template('toplevel.xml')
        s = f(t)
        self.assertTrue(isinstance(s, str))
        self.assertTrue(s.startswith('<!DOCTYPE html PUBLIC'))


class test_Page(TestCase):
    klass = ui.Page

    def test_init(self):
        inst = self.klass()
