# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2012 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Unit tests for the `dmedia.misc` module.
"""

from unittest import TestCase
import sys

from dmedia import misc


class Foo:
    baz = 'no callable here, move along'

    def bar(self, person):
        return 'hello ' + person

    def multi(self, *args):
        return '+'.join(args)


class TestWeakMethod(TestCase):
    def test_init(self):
        foo = Foo()
        callback = misc.WeakMethod(foo, 'bar')
        self.assertEqual(callback.method_name, 'bar')
        self.assertEqual(sys.getrefcount(foo), 2)
        self.assertEqual(callback.proxy.bar, foo.bar)

        # Test when attribute isn't callable
        with self.assertRaises(TypeError) as cm:
            callback = misc.WeakMethod(foo, 'baz')
        self.assertEqual(
            str(cm.exception),
            "'baz' attribute is not callable"
        )

        # Test when attribute doesn't exist
        with self.assertRaises(TypeError) as cm:
            callback = misc.WeakMethod(foo, 'baz')
        self.assertEqual(
            str(cm.exception),
            "'baz' attribute is not callable"
        )

    def test_call(self):
        foo = Foo()
        callback = misc.WeakMethod(foo, 'bar')
        self.assertEqual(callback('naughty nurse'), 'hello naughty nurse')

        callback = misc.WeakMethod(foo, 'multi')
        self.assertEqual(callback('hello'), 'hello')
        self.assertEqual(
            callback('hello', 'naughty', 'nurse'),
            'hello+naughty+nurse'
        )
