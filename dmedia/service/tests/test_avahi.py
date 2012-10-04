# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2012 Novacut Inc
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
Unit tests for `dmedia.service.avahi`.
"""

from unittest import TestCase
from copy import deepcopy

from microfiber import random_id
from usercouch import random_oauth

from dmedia.service import avahi


class TestAvahi(TestCase):
    def test_init(self):
        _id = random_id()
        inst = avahi.Avahi(_id, 42)
        self.assertEqual(inst.id, _id)
        self.assertEqual(inst.port, 42)
        self.assertIsNone(inst.group)

    def test_add_peer(self):
        inst = avahi.Avahi('the id', 42)
        with self.assertRaises(NotImplementedError) as cm:
            inst.add_peer('key', 'url')
        self.assertEqual(str(cm.exception), 'Avahi.add_peer()')

    def test_remove_peer(self):
        inst = avahi.Avahi('the id', 42)
        with self.assertRaises(NotImplementedError) as cm:
            inst.remove_peer('key')
        self.assertEqual(str(cm.exception), 'Avahi.remove_peer()')

