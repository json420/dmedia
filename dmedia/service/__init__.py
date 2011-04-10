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
Core dmedia DBus service at org.freedesktop.DMedia.
"""

import logging
import json
import time

import dbus
import dbus.service

from dmedia import __version__
from dmedia.constants import IFACE
from dmedia.core import Core


log = logging.getLogger()


class DMedia(dbus.service.Object):
    def __init__(self, dbname, no_dc, bus, killfunc, start=None):
        self._bus = bus
        self._dbname = dbname
        self._killfunc = killfunc
        log.info('Starting dmedia core service on %r', bus)
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self._busname = dbus.service.BusName(bus, self._conn)
        self._core = Core(dbname, no_dc)
        self._core.bootstrap()
        self._env_s = json.dumps(self._core.env)
        if start is not None:
            log.info('Started in %.3f', time.time() - start)

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def Version(self):
        """
        Return dmedia version.
        """
        return __version__

    @dbus.service.method(IFACE, in_signature='', out_signature='s')
    def GetEnv(self):
        """
        Return dmedia version.
        """
        return self._env_s

    @dbus.service.method(IFACE, in_signature='', out_signature='')
    def Kill(self):
        """
        Kill the dmedia service process.
        """
        log.info('Killing dmedia core service on %r', self._bus)
        self._killfunc()
