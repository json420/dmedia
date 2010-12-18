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
from .helpers import raises
from dmedialib import util


class test_functions(TestCase):
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

    def test_import_finished(self):
        f = util.import_finished

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
