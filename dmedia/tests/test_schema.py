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
from copy import deepcopy
from .helpers import raises
from dmedia.constants import TYPE_ERROR
from dmedia import schema


class test_functions(TestCase):
    def test_check_base32(self):
        f = schema.check_base32

        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('_id', basestring, int, 17)
        )
        e = raises(TypeError, f, True, label='import_id')
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
                self.assertEqual(f(b32), None)
            else:
                e = raises(ValueError, f, b32, label='foo')
                self.assertEqual(
                    str(e),
                    'len(b32decode(foo)) not multiple of 5: %r' % b32
                )

        self.assertEqual(f('MZZG2ZDSOQVSW2TEMVZG643F'), None)

    def test_check_type(self):
        f = schema.check_type

        # Test with wrong type
        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['type']", basestring, int, 17)
        )

        # Test with wrong case
        e = raises(ValueError, f, 'Dmedia/Foo')
        self.assertEqual(
            str(e),
             "doc['type'] must be lowercase; got 'Dmedia/Foo'"
        )

        # Test with wrong prefix
        e = raises(ValueError, f, 'foo/bar')
        self.assertEqual(
            str(e),
             "doc['type'] must start with 'dmedia/'; got 'foo/bar'"
        )

        # Test with multiple slashes
        e = raises(ValueError, f, 'dmedia/foo/bar')
        self.assertEqual(
            str(e),
             "doc['type'] must contain only one '/'; got 'dmedia/foo/bar'"
        )

        # Test with good values
        self.assertEqual(f('dmedia/foo'), None)
        self.assertEqual(f('dmedia/machine'), None)

    def test_check_time(self):
        f = schema.check_time

        # Test with wrong type
        bad = '123456789'
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time', (int, float), str, bad)
        )
        bad = u'123456789.18'
        e = raises(TypeError, f, bad, label='time_end')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time_end', (int, float), unicode, bad)
        )

        # Test with negative value
        bad = -1234567890
        e = raises(ValueError, f, bad, label='mtime')
        self.assertEqual(
            str(e),
            'mtime must be >= 0; got %r' % bad
        )
        bad = -1234567890.18
        e = raises(ValueError, f, bad, label='foo')
        self.assertEqual(
            str(e),
            'foo must be >= 0; got %r' % bad
        )

        # Test with good values
        self.assertEqual(f(1234567890), None)
        self.assertEqual(f(1234567890.18), None)
        self.assertEqual(f(0), None)
        self.assertEqual(f(0.0), None)

    def test_check_dmedia(self):
        f = schema.check_dmedia

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
        g = deepcopy(good)
        self.assertEqual(f(g), None)
        for key in ['_id', 'type', 'time']:
            bad = deepcopy(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % [key]
            )
        for keys in (['_id', 'type'], ['_id', 'time'], ['time', 'type']):
            bad = deepcopy(good)
            for key in keys:
                del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % keys
            )
        bad = {'foo': 'bar'}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'doc missing keys: %r' % ['_id', 'time', 'type']
        )

    def test_check_stored(self):
        f = schema.check_stored

        good = {
            'MZZG2ZDSOQVSW2TEMVZG643F': {
                'copies': 2,
                'time': 1234567890,
            },
            'NZXXMYLDOV2F6ZTUO5PWM5DX': {
                'copies': 1,
                'time': 1234666890,
            },
        }

        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # Test with wrong type:
        bad = [
            (
                'MZZG2ZDSOQVSW2TEMVZG643F',
                {
                    'copies': 2,
                    'time': 1234567890,
                }
            )
        ]
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('stored', dict, list, bad)
        )

        # Test with empty value:
        e = raises(ValueError, f, {})
        self.assertEqual(str(e), "stored cannot be empty")

        # Test with bad key
        bad = deepcopy(good)
        bad['MFQWCYLBMFQWCYI='] =  {'copies': 2, 'time': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "len(b32decode(<key in stored>)) not multiple of 5: 'MFQWCYLBMFQWCYI='"
        )

        # Test with wrong value Type
        bad = deepcopy(good)
        v = (2, 1234567890)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = v
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("stored['OVRHK3TUOUQCWIDMNFXGC4TP']", dict, tuple, v)
        )

        # Test with misisng value keys
        bad = deepcopy(good)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = {'number': 2, 'time': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "stored['OVRHK3TUOUQCWIDMNFXGC4TP'] missing keys: ['copies']"
        )
        bad = deepcopy(good)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = {'number': 2, 'added': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "stored['OVRHK3TUOUQCWIDMNFXGC4TP'] missing keys: ['copies', 'time']"
        )


    def test_check_dmedia_file(self):
        f = schema.check_dmedia_file

        # Test with good doc:
        good = {
            '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
            'type': 'dmedia/file',
            'time': 1234567890,
            'bytes': 20202333,
            'stored': {
                'MZZG2ZDSOQVSW2TEMVZG643F': {
                    'copies': 2,
                    'time': 1234567890,
                },
            },
        }
        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # Test with wrong record type:
        bad = deepcopy(good)
        bad['type'] = 'dmedia/files'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "doc['type'] must be 'dmedia/file'; got 'dmedia/files'"
        )

        # Test with missing attributes:
        for key in ['bytes', 'stored']:
            bad = deepcopy(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % [key]
            )

        # Test with bytes wrong type:
        bad = deepcopy(good)
        bad['bytes'] *= 1.0
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['bytes']", int, float, bad['bytes'])
        )

        # Test with bytes < 1:
        bad = deepcopy(good)
        bad['bytes'] = 0
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "doc['bytes'] must be > 0; got 0"
        )
        bad = deepcopy(good)
        bad['bytes'] = -1
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "doc['bytes'] must be > 0; got -1"
        )

        # Test with bytes=1
        g = deepcopy(good)
        g['bytes'] = 1
        self.assertEqual(f(g), None)
