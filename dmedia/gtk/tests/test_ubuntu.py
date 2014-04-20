# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
Unit tests for the `dmedia.gtk.ubuntu` module.
"""

from unittest import TestCase

from gi.repository import Notify

from dmedia.gtk import ubuntu


class Generic(object):
    def __init__(self, name, callback):
        self.name = name
        self.callback = callback

    def __call__(self, *args, **kw):
        self.callback(self.name, args, kw)


class Adapter(object):
    def __init__(self, *args, **kw):
        self._args = args
        self._kw = kw
        self._calls = []

    def _generic(self, name, args, kw):
        self._calls.append((name, args, kw))

    def __getattr__(self, name):
        attr = Generic(name, self._generic)
        setattr(self, name, attr)
        return attr


class Adapter2(object):
    new = Adapter


inst = Adapter()
inst.update('foo', 'bar', 'baz')
assert inst._calls == [
    ('update', ('foo', 'bar', 'baz'), {}),
]
assert isinstance(inst.update, Generic)


class TestFunctions(TestCase):
    def test_notify_started(self):
        basedirs = ['/media/EOS_DIGITAL']
        (summary, body) = ubuntu.notify_started(basedirs)
        self.assertEqual(summary, 'Importing files from 1 card:')
        self.assertEqual(body, '/media/EOS_DIGITAL')

        basedirs = ['/media/EOS_DIGITAL', '/media/H4n']
        (summary, body) = ubuntu.notify_started(basedirs)
        self.assertEqual(summary, 'Importing files from 2 cards:')
        self.assertEqual(body, '\n'.join(basedirs))

        basedirs = ['/media/EOS_DIGITAL', '/media/H4n', '/media/stuff']
        (summary, body) = ubuntu.notify_started(basedirs)
        self.assertEqual(summary, 'Importing files from 3 cards:')
        self.assertEqual(body, '\n'.join(basedirs))


class test_NotifyManger(TestCase):
    klass = ubuntu.NotifyManager

    def test_init(self):
        inst = self.klass()
        self.assertIs(inst._klass, Notify.Notification)

        inst = self.klass(klass=None)
        self.assertIs(inst._klass, Notify.Notification)

        k = 'whatever'
        inst = self.klass(klass=k)
        self.assertIs(inst._klass, k)

    def test_on_closed(self):
        inst = self.klass()
        n = 'a Notification instance'
        inst._current = n
        inst._on_closed(n)
        self.assertIsNone(inst._current)

    def test_isvisible(self):
        inst = self.klass()
        self.assertFalse(inst.isvisible())
        inst._current = 'foo'
        self.assertTrue(inst.isvisible())
        inst._current = False
        self.assertTrue(inst.isvisible())
        inst._current = None
        self.assertFalse(inst.isvisible())

    def test_notify(self):
        inst = self.klass(Adapter2)
        self.assertTrue(inst._current is None)
        inst.notify('foo', 'bar', 'baz')
        self.assertTrue(isinstance(inst._current, Adapter))
        self.assertEqual(
            inst._current._args,
            ('foo', 'bar', 'baz')
        )
        self.assertEqual(inst._current._kw, {})
        self.assertEqual(
            inst._current._calls,
            [
                ('connect', ('closed', inst._on_closed), {}),
                ('show', tuple(), {}),
            ]
        )
        with self.assertRaises(AssertionError):
            inst.notify('foo', 'bar', 'baz')

    def test_update(self):
        inst = self.klass()

        with self.assertRaises(AssertionError):
            inst.update('foo', 'bar', 'baz')

        current = Adapter()
        inst._current = current
        inst.update('foo', 'bar', 'baz')
        self.assertEqual(
            current._calls,
            [
                ('update', ('foo', 'bar', 'baz'), {}),
                ('show', tuple(), {}),
            ]
        )

    def test_replace(self):
        # Test with no current notification
        inst = self.klass(Adapter2)
        self.assertTrue(inst._current is None)
        inst.replace('foo', 'bar', 'baz')
        self.assertTrue(isinstance(inst._current, Adapter))
        self.assertEqual(
            inst._current._args,
            ('foo', 'bar', 'baz')
        )
        self.assertEqual(inst._current._kw, {})
        self.assertEqual(
            inst._current._calls,
            [
                ('connect', ('closed', inst._on_closed), {}),
                ('show', tuple(), {}),
            ]
        )

        # Test with a visible current notification
        current = Adapter()
        inst = self.klass()
        inst._current = current
        inst.replace('foo', 'bar', 'baz')
        self.assertEqual(
            current._calls,
            [
                ('update', ('foo', 'bar', 'baz'), {}),
                ('show', tuple(), {}),
            ]
        )

