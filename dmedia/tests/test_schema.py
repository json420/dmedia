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
Unit tests for `dmedia.schema` module.
"""

from unittest import TestCase
from base64 import b32encode
from .helpers import raises
from dmedia.constants import TYPE_ERROR
from dmedia import schema


class test_functions(TestCase):
    def test_isbase32(self):
        f = schema.isbase32

        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('_id', basestring, int, 17)
        )
        e = raises(TypeError, f, True, key='import_id')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('import_id', basestring, bool, True)
        )

        bad = 'MZzG2ZDSOQVSW2TEMVZG643F'
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            'Non-base32 digit found'
        )

        for n in xrange(5, 26):
            b32 = b32encode('a' * n)
            if n % 5 == 0:
                self.assertTrue(f(b32), b32)
            else:
                e = raises(ValueError, f, b32, key='foo')
                self.assertEqual(
                    str(e),
                    'len(b32decode(foo)) not multiple of 5: %r' % b32
                )

        self.assertEqual(
            f('MZZG2ZDSOQVSW2TEMVZG643F'), 'MZZG2ZDSOQVSW2TEMVZG643F'
        )

    def test_istime(self):
        f = schema.istime

        # Test with wrong type
        bad = '123456789'
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time', (int, float), str, bad)
        )
        bad = u'123456789.18'
        e = raises(TypeError, f, bad, key='time_end')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time_end', (int, float), unicode, bad)
        )

        # Test with negative value
        bad = -1234567890
        e = raises(ValueError, f, bad, key='mtime')
        self.assertEqual(
            str(e),
            'mtime must be >= 0; got %r' % bad
        )
        bad = -1234567890.18
        e = raises(ValueError, f, bad, key='foo')
        self.assertEqual(
            str(e),
            'foo must be >= 0; got %r' % bad
        )

        # Test with good values
        self.assertEqual(f(1234567890), 1234567890)
        self.assertEqual(f(1234567890.18), 1234567890.18)
        self.assertEqual(f(0), 0)
        self.assertEqual(f(0.0), 0.0)

    def test_isdmedia(self):
        f = schema.isdmedia

        bad = [
            ('_id', 'MZZG2ZDSOQVSW2TEMVZG643F'),
            ('type', 'dmedia/foo'),
            ('time', 1234567890),
        ]
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('doc', dict, list, bad)
        )

        good = {
            '_id': 'MZZG2ZDSOQVSW2TEMVZG643F',
            'type': 'dmedia/foo',
            'time': 1234567890,
            'foo': 'bar',
        }
        self.assertEqual(f(dict(good)), good)
        for key in ['_id', 'type', 'time']:
            bad = dict(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing required keys: %r' % [key]
            )
        for keys in (['_id', 'type'], ['_id', 'time'], ['time', 'type']):
            bad = dict(good)
            for key in keys:
                del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing required keys: %r' % keys
            )
        bad = {'foo': 'bar'}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'doc missing required keys: %r' % ['_id', 'time', 'type']
        )
