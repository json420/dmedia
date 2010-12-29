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
from base64 import b64encode
from genshi.template import MarkupTemplate
from dmedialib import ui, datadir


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
        mootools = path.join(datadir, 'mootools.js')
        self.assertEqual(
            f('mootools.js'),
            open(mootools, 'r').read()
        )

    def test_inline_datafile(self):
        f = ui.inline_datafile
        filename = path.join(datadir, 'dmedia.js')
        comment = '/* ' + filename + ' */\n'
        self.assertEqual(
            f('dmedia.js'),
            comment + open(filename, 'rb').read()
        )

    def test_inline_data(self):
        f = ui.inline_data
        self.assertEqual(f([]), '')
        self.assertEqual(
            f(['dmedia.js']),
            ui.inline_datafile('dmedia.js')
        )
        self.assertEqual(
            f(['mootools.js', 'dmedia.js']),
            '\n\n'.join([
                ui.inline_datafile('mootools.js'),
                ui.inline_datafile('dmedia.js')
            ])
        )

    def test_encode_datafile(self):
        f = ui.encode_datafile
        mootools = path.join(datadir, 'mootools.js')
        self.assertEqual(
            f('mootools.js'),
            b64encode(open(mootools, 'rb').read())
        )

    def test_iter_datafiles(self):
        f = ui.iter_datafiles
        self.assertEqual(
            list(f()),
            [
                ('alt.css', 'text/css'),
                ('browser.js', 'application/javascript'),
                ('dmedia.css', 'text/css'),
                ('dmedia.js', 'application/javascript'),
                ('mootools.js', 'application/javascript'),
                ('search.png', 'image/png'),
                ('stars.png', 'image/png'),
                ('style.css', 'text/css'),
            ]
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
        self.assertTrue(isinstance(inst.toplevel_t, MarkupTemplate))
        self.assertEqual(inst.body_t, None)

        class Example(self.klass):
            body = 'body.xml'

        inst = Example()
        self.assertTrue(isinstance(inst.toplevel_t, MarkupTemplate))
        self.assertTrue(isinstance(inst.body_t, MarkupTemplate))
