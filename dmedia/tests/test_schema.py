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
from .helpers import raises
from dmedia.constants import TYPE_ERROR
from dmedia import schema


class test_functions(TestCase):
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
            'time': '1234567890',
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
