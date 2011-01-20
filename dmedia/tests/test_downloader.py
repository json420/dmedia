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
Unit tests for `dmedia.downloader` module.
"""

from unittest import TestCase
from .helpers import raises
from dmedia import downloader


class test_functions(TestCase):
    def test_bytes_range(self):
        f = downloader.bytes_range
        self.assertEqual(f(0, 500), 'bytes=0-499')
        self.assertEqual(f(500, 1000), 'bytes=500-999')
        self.assertEqual(f(-500), 'bytes=-500')
        self.assertEqual(f(9500), 'bytes=9500-')

    def test_range_request(self):
        f = downloader.range_request

        e = raises(ValueError, f, -2, 1024, 3001)
        self.assertEqual(str(e), 'i must be >=0; got -2')
        e = raises(ValueError, f, 0, 500, 3001)
        self.assertEqual(str(e), 'leaf_size must be >=1024; got 500')
        e = raises(ValueError, f, 0, 1024, 0)
        self.assertEqual(str(e), 'file_size must be >=1; got 0')

        self.assertEqual(f(0, 1024, 3001), 'bytes=0-1023')
        self.assertEqual(f(1, 1024, 3001), 'bytes=1024-2047')
        self.assertEqual(f(2, 1024, 3001), 'bytes=2048-3000')

        e = raises(ValueError, f, 3, 1024, 3001)
        self.assertEqual(
            str(e),
            'past end of file: i=3, leaf_size=1024, file_size=3001'
        )
