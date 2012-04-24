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
Unit tests for `dmedia.service.replicator`.
"""

from unittest import TestCase
from copy import deepcopy

from microfiber import random_id
from usercouch import random_oauth

from dmedia.service import replicator


class TestFunctions(TestCase):
    def test_get_body(self):
        self.assertEqual(
            replicator.get_body('foo', 'bar'),
            {
                'source': 'foo',
                'target': 'bar',
                'continuous': True,
            }
        )

    def test_get_peer(self):
        url = 'http://192.168.20.118:60492/'
        dbname = 'novacut-0' + random_id().lower()
        oauth = random_oauth()
        self.assertEqual(
            replicator.get_peer(url, dbname, deepcopy(oauth)),
            {
                'url': url + dbname,
                'auth': {
                    'oauth': oauth,
                }
            }
        ) 
        
        
