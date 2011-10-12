# dmedia: dmedia hashing protocol and file layout
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
Unit tests for `dmedia.units`.
"""

from unittest import TestCase

from dmedia import units


class TestFunctions(TestCase):
    def test_bytes2(self):

        # Test with negative number:
        with self.assertRaises(ValueError) as cm:
            units.bytes10(-17)
        self.assertEqual(
            str(cm.exception),
            'size must be >= 0; got -17'
        )

        # Test with None
        self.assertIsNone(units.bytes10(None))

        # Test with 0:
        self.assertEqual(units.bytes10(0), '0 bytes')

        # Test with 1:
        self.assertEqual(units.bytes10(1), '1 byte')

        # Bunch of values
        self.assertEqual(units.bytes10(2), '2 bytes')
        self.assertEqual(units.bytes10(17), '17 bytes')
        self.assertEqual(units.bytes10(314), '314 bytes')

        self.assertEqual(units.bytes10(1000), '1 kB')
        self.assertEqual(units.bytes10(3140), '3.14 kB')
        self.assertEqual(units.bytes10(31400), '31.4 kB')
        self.assertEqual(units.bytes10(314000), '314 kB')

        self.assertEqual(units.bytes10(10 ** 6), '1 MB')
        self.assertEqual(units.bytes10(3140000), '3.14 MB')
        self.assertEqual(units.bytes10(31400000), '31.4 MB')
        self.assertEqual(units.bytes10(314000000), '314 MB')

        self.assertEqual(units.bytes10(10 ** 9), '1 GB')
        self.assertEqual(units.bytes10(3140000000), '3.14 GB')
        self.assertEqual(units.bytes10(31400000000), '31.4 GB')
        self.assertEqual(units.bytes10(314000000000), '314 GB')

        self.assertEqual(units.bytes10(10 ** 12), '1 TB')
        self.assertEqual(units.bytes10(3140000000000), '3.14 TB')
        self.assertEqual(units.bytes10(31400000000000), '31.4 TB')
        self.assertEqual(units.bytes10(314000000000000), '314 TB')

        self.assertEqual(units.bytes10(10 ** 15), '1 PB')
        self.assertEqual(units.bytes10(3140000000000000), '3.14 PB')
        self.assertEqual(units.bytes10(31400000000000000), '31.4 PB')
        self.assertEqual(units.bytes10(314000000000000000), '314 PB')
        self.assertEqual(units.bytes10(999 * 10 ** 15), '999 PB')

        big = 1000 ** len(units.BYTES10)
        self.assertEqual(units.bytes10(big), '1e+03 YB')
