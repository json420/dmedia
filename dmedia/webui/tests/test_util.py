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
Unit tests for the `dmedia.webui.util` module.
"""

from unittest import TestCase
from os import path

from genshi.template import MarkupTemplate, TemplateSyntaxError

from dmedia.tests.helpers import TempDir
from dmedia.webui import util


good_t = """
<div xmlns:py="http://genshi.edgewall.org/">
</div>
"""

bad_t = """
<div xmlns:py="http://genshi.edgewall.org/">
<p>
</div>
"""


class TestFunctions(TestCase):
    def test_render_var(self):
        f = util.render_var
        obj = dict(foo=True, bar=False, baz=None)
        self.assertEqual(
            f('junk', obj),
            'var junk = {"bar": false, "baz": null, "foo": true};'
        )
        self.assertEqual(
            f('stuff.junk', obj),
            'stuff.junk = {"bar": false, "baz": null, "foo": true};'
        )

    def test_datafile(self):
        f = util.datafile
        tmp = TempDir()

        # Test when file doesn't exist:
        self.assertRaises(IOError, f, 'nope.xml', tmp.path)
        self.assertRaises(IOError, f, 'nope.xml')

        # Test that default parent is util.DATADIR:
        self.assertEqual(
            f('top.xml'),
            open(path.join(util.DATADIR, 'top.xml'), 'rb').read()
        )

        # Test when parent is provided:
        tmp.write(good_t, 'good.xml')
        self.assertEqual(f('good.xml', tmp.path), good_t)

    def test_template(self):
        f = util.template
        tmp = TempDir()

        # Test when file doesn't exist:
        self.assertRaises(IOError, f, 'nope.xml', tmp.path)

        # Test with invalid XML:
        tmp.write(bad_t, 'bad.xml')
        self.assertRaises(TemplateSyntaxError, f, 'bad.xml', tmp.path)

        # Test with good template:
        good = tmp.write(good_t, 'good.xml')
        t = f('good.xml', tmp.path)
        self.assertIsInstance(t, MarkupTemplate)
        self.assertEqual(t.filepath, good)

        # Test with data/top.xml:
        t = f('top.xml')
        self.assertIsInstance(t, MarkupTemplate)
        self.assertEqual(t.filepath, path.join(util.DATADIR, 'top.xml'))
