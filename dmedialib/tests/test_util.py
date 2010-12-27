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
Unit tests for `dmedialib.util` module.
"""

from unittest import TestCase
from base64 import b32encode, b32decode
from .helpers import raises
from dmedialib.constants import TYPE_ERROR, CALLABLE_ERROR
from dmedialib import util


class test_functions(TestCase):
    def test_random_id(self):
        f = util.random_id
        _id = f()
        self.assertEqual(len(_id), 24)
        binary = b32decode(_id)
        self.assertEqual(len(binary), 15)
        self.assertEqual(b32encode(binary), _id)

    def test_units_base10(self):
        f = util.units_base10

        # Test with negative number:
        e = raises(ValueError, f, -17)
        self.assertEqual(str(e), 'size must be greater than zero; got -17')

        # Test with size >= 1 EB
        big = 10 ** 18
        e = raises(ValueError, f, big)
        self.assertEqual(
            str(e),
            'size must be smaller than 10**18; got %r' % big
        )

        # Test with None
        self.assertTrue(f(None) is None)

        # Test with 0:
        self.assertEqual(f(0), '0 bytes')

        # Test with 1:
        self.assertEqual(f(1), '1 byte')

        # Bunch of tests
        self.assertEqual(f(17), '17 bytes')
        self.assertEqual(f(314), '314 bytes')

        self.assertEqual(f(1000), '1 kB')
        self.assertEqual(f(3140), '3.14 kB')
        self.assertEqual(f(31400), '31.4 kB')
        self.assertEqual(f(314000), '314 kB')

        self.assertEqual(f(10 ** 6), '1 MB')
        self.assertEqual(f(3140000), '3.14 MB')
        self.assertEqual(f(31400000), '31.4 MB')
        self.assertEqual(f(314000000), '314 MB')

        self.assertEqual(f(10 ** 9), '1 GB')
        self.assertEqual(f(3140000000), '3.14 GB')
        self.assertEqual(f(31400000000), '31.4 GB')
        self.assertEqual(f(314000000000), '314 GB')

        self.assertEqual(f(10 ** 12), '1 TB')
        self.assertEqual(f(3140000000000), '3.14 TB')
        self.assertEqual(f(31400000000000), '31.4 TB')
        self.assertEqual(f(314000000000000), '314 TB')

        self.assertEqual(f(10 ** 15), '1 PB')
        self.assertEqual(f(3140000000000000), '3.14 PB')
        self.assertEqual(f(31400000000000000), '31.4 PB')
        self.assertEqual(f(314000000000000000), '314 PB')
        self.assertEqual(f(999 * 10 ** 15), '999 PB')

    def test_import_started(self):
        f = util.import_started

        base = '/media/EOS_DIGITAL'
        self.assertEqual(
            f([base]),
            ('Searching for new files...', base)
        )

        bases = ['/media/EOS_DIGITAL', '/media/ANOTHER_CARD']
        self.assertEqual(
            f(bases),
            ('Searching on 2 cards...', '\n'.join(bases))
        )

        bases = ['/media/EOS_DIGITAL', '/media/ANOTHER_CARD', '/media/ONE_MORE']
        self.assertEqual(
            f(bases),
            ('Searching on 3 cards...', '\n'.join(bases))
        )

    def test_batch_import_finished(self):
        f = util.batch_import_finished

        # Test that value error is raised for imported or skipped < 0
        e = raises(ValueError, f, dict(imported=-17))
        self.assertEqual(
            str(e),
            "stats['imported'] must be >= 0; got -17"
        )
        e = raises(ValueError, f, dict(skipped=-18))
        self.assertEqual(
            str(e),
            "stats['skipped'] must be >= 0; got -18"
        )

        # Test with empty dictionary
        self.assertEqual(
            f({}),
            ('No files found', None)
        )

        # Test with all 0 values:
        self.assertEqual(
            f(dict(imported=0, imported_bytes=0, skipped=0, skipped_bytes=0)),
            ('No files found', None)
        )

        # Test with 1 imported
        self.assertEqual(
            f(dict(imported=1)),
            ('Added 1 new file, 0 bytes', None)
        )
        self.assertEqual(
            f(dict(imported=1, imported_bytes=29481537)),
            ('Added 1 new file, 29.5 MB', None)
        )

        # Test with 2 imported
        self.assertEqual(
            f(dict(imported=2)),
            ('Added 2 new files, 0 bytes', None)
        )
        self.assertEqual(
            f(dict(imported=2, imported_bytes=392012353)),
            ('Added 2 new files, 392 MB', None)
        )

        # Test with 1 skipped
        self.assertEqual(
            f(dict(skipped=1)),
            ('No new files found', 'Skipped 1 duplicate, 0 bytes')
        )
        self.assertEqual(
            f(dict(skipped=1, skipped_bytes=29481537)),
            ('No new files found', 'Skipped 1 duplicate, 29.5 MB')
        )

        # Test with 2 skipped
        self.assertEqual(
            f(dict(skipped=2)),
            ('No new files found', 'Skipped 2 duplicates, 0 bytes')
        )
        self.assertEqual(
            f(dict(skipped=2, skipped_bytes=392012353)),
            ('No new files found', 'Skipped 2 duplicates, 392 MB'),
        )

        # Test with imported and skipped:
        self.assertEqual(
            f(dict(imported=7, skipped=1)),
            ('Added 7 new files, 0 bytes', 'Skipped 1 duplicate, 0 bytes')
        )
        stats = dict(
            imported=1,
            imported_bytes=29481537,
            skipped=6,
            skipped_bytes=392012353,
        )
        self.assertEqual(
            f(stats),
            ('Added 1 new file, 29.5 MB', 'Skipped 6 duplicates, 392 MB')
        )


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


inst = Adapter()
inst.update('foo', 'bar', 'baz')
assert inst._calls == [
    ('update', ('foo', 'bar', 'baz'), {}),
]
assert isinstance(inst.update, Generic)


class test_NotifyManger(TestCase):
    klass = util.NotifyManager

    def test_init(self):
        inst = self.klass()
        self.assertTrue(inst._klass is util.Notification)

        inst = self.klass(klass=None)
        self.assertTrue(inst._klass is util.Notification)

        k = 'whatever'
        inst = self.klass(klass=k)
        self.assertTrue(inst._klass is k)

    def test_on_closed(self):
        inst = self.klass()
        n = 'a Notification instance'
        inst._current = n
        inst._on_closed(n)
        self.assertTrue(inst._current is None)

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
        inst = self.klass(Adapter)
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

        e = raises(AssertionError, inst.notify, 'foo', 'bar', 'baz')

    def test_update(self):
        inst = self.klass()

        e = raises(AssertionError, inst.update, 'foo', 'bar', 'baz')

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
        inst = self.klass(Adapter)
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


class test_Timer(TestCase):
    klass = util.Timer

    def test_init(self):
        callback = lambda *args, **kw: 'hello'

        # Test with wrong seconds type
        for s in ['1.0', u'2']:
            e = raises(TypeError, self.klass, s, callback)
            assert str(e) == TYPE_ERROR % ('seconds', (float, int), type(s), s)

        # Test with seconds <= 0:
        for s in [0, 0.0, -1, -0.5]:
            e = raises(ValueError, self.klass, s, callback)
            assert str(e) == 'seconds: must be > 0; got %r' % s

        # Test non-callable callback
        for c in ['call me', 18, object()]:
            e = raises(TypeError, self.klass, 1, c)
            assert str(e) == CALLABLE_ERROR % ('callback', type(c), c)

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
