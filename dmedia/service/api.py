# dmedia: distributed media library
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Python convenience API for talking to dmedia components over DBus.
"""

import json

from . import dbus


class DMedia:
    """
    Talk to "org.freedesktop.DMedia".
    """

    def __init__(self, bus='org.freedesktop.DMedia'):
        self.bus = bus
        self._proxy = None

    @property
    def proxy(self):
        if self._proxy is None:
            self._proxy = dbus.session.get(self.bus, '/')
        return self._proxy

    def Version(self):
        return self.proxy.Version()

    def Kill(self):
        self.proxy.Kill()
        self._proxy = None

    def GetEnv(self):
        return json.loads(self.proxy.GetEnv())

    def AddFileStore(self, parentdir):
        return self.proxy.AddFileStore('(s)', parentdir)
        
