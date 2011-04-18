# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedia.transfers` module.
"""

from unittest import TestCase
from os import urandom
from base64 import b32encode


from dmedia import transfers


def random_id(blocks=3):
    return b32encode(urandom(5 * blocks))


class TestFunctions(TestCase):
    def test_download_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.download_key(file_id, store_id),
            ('download', file_id)
        )

    def test_upload_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.upload_key(file_id, store_id),
            ('upload', file_id, store_id)
        )
