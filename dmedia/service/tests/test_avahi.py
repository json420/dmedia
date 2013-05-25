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
import ssl

import microfiber
from dbase32 import random_id

from dmedia.tests.couch import CouchCase
from dmedia.identity import TempPKI
from dmedia.core import Core
from dmedia.service import avahi


random = SystemRandom()


def random_port():
    return random.randint(1001, 50000)


class TestAvahi(CouchCase):
    def test_init(self):
        pki = TempPKI(client_pki=True)
        ssl_config = pki.get_client_config()
        core = Core(self.env, ssl_config)
        port = random_port()
        inst = avahi.Avahi(core, port)
        self.assertIs(inst.core, core)
        self.assertEqual(inst.port, port)
        self.assertEqual(inst.machine_id, self.machine_id)
        self.assertEqual(inst.user_id, self.user_id)
        self.assertIs(inst.server, core.server)
        self.assertIsInstance(inst.ssl_context, ssl.SSLContext)
        self.assertEqual(inst.replications, {})

        inst.__del__()

