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

from random import SystemRandom
from copy import deepcopy

import microfiber

from dmedia.tests.couch import CouchCase
from dmedia.service import avahi
from dmedia.util import get_db


random = SystemRandom()


def random_port():
    return random.randint(1001, 50000)


class TestAvahi(CouchCase):
    def test_init(self):
        db = get_db(self.env)
        self.assertTrue(db.ensure())
        port = random_port()
        ssl_config = 'the SSL config'
        inst = avahi.Avahi(self.env, port, ssl_config)
        self.assertIsNone(inst.group)
        self.assertIs(inst.machine_id, self.machine_id)
        self.assertIs(inst.user_id, self.user_id)
        self.assertIs(inst.port, port)
        self.assertIs(inst.ssl_config, ssl_config)
        self.assertIsInstance(inst.db, microfiber.Database)
        self.assertIsInstance(inst.server, microfiber.Server)
        self.assertIs(inst.db.ctx, inst.server.ctx)
        self.assertEqual(inst.replications, {})
        self.assertEqual(inst.peers,
            {
                '_id': '_local/peers',
                '_rev': '0-1',
                'peers': {},
            }
        )
        self.assertEqual(db.get('_local/peers'), inst.peers)

        peers = deepcopy(inst.peers)
        peers['peers'] = {'foo': 'bar'}
        db.save(peers)
        inst = avahi.Avahi(self.env, port, ssl_config)
        self.assertEqual(inst.peers,
            {
                '_id': '_local/peers',
                '_rev': '0-3',
                'peers': {},
            }
        )
        inst = avahi.Avahi(self.env, port, ssl_config)
        self.assertEqual(inst.peers,
            {
                '_id': '_local/peers',
                '_rev': '0-3',
                'peers': {},
            }
        )

        inst.__del__()

