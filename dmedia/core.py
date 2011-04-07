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
Core dmedia entry-point/API - start here!

For background, please see:

    https://bugs.launchpad.net/dmedia/+bug/753260
"""

import time
import socket
import platform

from couchdb import ResourceNotFound

from .abstractcouch import get_env, get_dmedia_db
from .schema import random_id


class DMedia(object):
    def __init__(self, dbname=None, env=None):
        self.env = (get_env(dbname) if env is None else env)
        self.db = get_dmedia_db(self.env)

    def init_machine(self):
        """
        If needed, create the 'dmedia/machine' record for this computer.
        """
        try:
            loc = self.db['_local/machine']
        except ResourceNotFound:
            loc = {
                '_id': '_local/machine',
                'machine_id': random_id(),
            }
            self.db.save(loc)
        machine_id = loc['machine_id']
        try:
            machine = self.db[machine_id]
        except ResourceNotFound:
            machine = {
                '_id': machine_id,
                'type': 'dmedia/machine',
                'time': time.time(),
                'hostname': socket.gethostname(),
                'distribution': platform.linux_distribution(),
            }
            self.db.save(machine)
        return machine
