# Copyright (C) 2011 Novacut Inc
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
Simple dbus helper to make PyGI a bit nicer.
"""

from gi.repository import Gio


class DBus:
    def __init__(self, conn):
        self.conn = conn

    def get(self, bus, path, iface=None):
        if iface is None:
            iface = bus
        return Gio.DBusProxy.new_sync(
            self.conn, 0, None, bus, path, iface, None
        )

    def get_async(self, callback, bus, path, iface=None):
        if iface is None:
            iface = bus
        Gio.DBusProxy.new(
            self.conn, 0, None, bus, path, iface, None, callback, None
        )


session = DBus(Gio.bus_get_sync(Gio.BusType.SESSION, None))
system = DBus(Gio.bus_get_sync(Gio.BusType.SYSTEM, None))

