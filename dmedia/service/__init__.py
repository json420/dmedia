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

import dbus
import dbus.service

from dmedia.constants import IFACE


log = logging.getLogger()


class DMedia(dbus.service.Object):
    def __init__(self, bus, killfunc=None):
        self._bus = bus
        self._killfunc = killfunc
        log.info('Starting dmedia core service on %r', bus)
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self._busname = dbus.service.BusName(bus, self._conn)

    @dbus.service.method(IFACE, in_signature='', out_signature='')
    def Kill(self):
        """
        Kill the dmedia service process.
        """
        log.info('Killing dmedia core service on %r', self._bus)
        if callable(self._killfunc):
            log.info('Calling killfunc()')
            self._killfunc()
