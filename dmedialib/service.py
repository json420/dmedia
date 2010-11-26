# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Makes dmedia functionality avaible over D-Bus.
"""

import dbus
import dbus.service


BUS_NAME = 'org.freedesktop.DMedia'
OBJECT_PATH = '/org/freedesktop/DMedia'


class DMedia(dbus.service.Object):
    def __init__(self, killfunc=None):
        self.killfunc = killfunc
        self.conn = dbus.SessionBus()
        super(DMedia, self).__init__(self.conn, object_path='/')
        self.bus_name = dbus.service.BusName(BUS_NAME, self.conn)

    @dbus.service.method(BUS_NAME, in_signature='', out_signature='s')
    def test(self):
        return 'okay'

    @dbus.service.method(BUS_NAME, in_signature='', out_signature='')
    def kill(self):
        if callable(self.killfunc):
            self.killfunc()
