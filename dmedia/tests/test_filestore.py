# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com)
#
# dmedia: distributed media library
# Copyright (C) 2010, 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for external filestore, dmedia style.
"""

from .base import SampleFilesTestCase

import filestore


class TestFunctions(SampleFilesTestCase):
    def test_hash_fp(self):
        src_fp = open(self.mov, 'rb')
        self.assertEqual(filestore.hash_fp(src_fp), self.mov_ch)

        src_fp = open(self.thm, 'rb')
        self.assertEqual(filestore.hash_fp(src_fp), self.thm_ch)
