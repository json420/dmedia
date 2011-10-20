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
Unit tests for `dmedia.gtk.util` module.
"""

from unittest import TestCase
from dmedia.constants import TYPE_ERROR, CALLABLE_ERROR
from dmedia.gtk import util


class test_Timer(TestCase):
    klass = util.Timer

    def test_init(self):
        callback = lambda *args, **kw: 'hello'

        # Test with wrong seconds type
        with self.assertRaises(TypeError) as cm:
            inst = util.Timer('1.0', callback)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('seconds', (float, int), str, '1.0')
        )

        # Test with seconds <= 0:
        for s in [0, 0.0, -1, -0.5]:
            with self.assertRaises(ValueError) as cm:
                inst = util.Timer(s, callback)
                self.assertEqual(
                    str(cm.exception),
                    'seconds: must be > 0; got {!r}'.format(s)
                )

        # Test non-callable callback
        for c in ['call me', 18, object()]:
            with self.assertRaises(TypeError) as cm:
                inst = util.Timer(1, c)
                self.assertEqual(
                    str(cm.exception),
                    CALLABLE_ERROR.format('callback', type(c), c)
                )

        # Test with correct values:
        inst = self.klass(1, callback)
        assert inst.seconds == 1
        assert inst.callback is callback

        inst = self.klass(0.75, callback)
        assert inst.seconds == 0.75
        assert inst.callback is callback

    def test_start(self):
        inst = self.klass(1, lambda: 'hello')
        self.assertTrue(inst.start())
        self.assertFalse(inst.start())
        self.assertFalse(inst.start())

    def test_stop(self):
        inst = self.klass(1, lambda: 'hello')
        assert inst.stop() is False
        self.assertTrue(inst.start())
        self.assertTrue(inst.stop())
        self.assertFalse(inst.stop())
