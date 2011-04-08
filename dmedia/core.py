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


Security note on /dmedia/_local/filestores
==========================================

When `DMedia.init_filestores()` is called, it creates `FileStore` instances
based solely on information in the non-replicated /dmedia/_local/filestores
document... despite the fact that the exact same information is also available
in the corresponding 'dmedia/store' documents.

When it comes to deciding what files dmedia will read and write, it's prudent to
assume that replicated documents are untrustworthy.

The dangerous approach would be to use a view to get all the 'dmedia/store'
documents with a machine_id that matches this machine_id, and initialize those `FileStore`.  But the problem is that if any attacker gained control of just one
of your replicating peers or services, they could insert arbitrary
'dmedia/store' documents, and have dmedia happily initialize `FileStore` at
those mount points.  And that would be "a bad thing".

So although the corresponding 'dmedia/store' records are created (if they don't
already exists), they are completely ignored when it comes to deciding what
filestores and mount points are configured.
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

    def bootstrap(self):
        self.machine = self.init_machine()
        self.machine_id = self.machine['_id']
        self.env['machine_id'] = self.machine_id

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

    def init_filestores(self):
        try:
            loc = self.db['_local/filestores']
        except ResourceNotFound:
            loc = {
                '_id': '_local/filestores',
                'fixed': {},
            }
            self.db.save(loc)
