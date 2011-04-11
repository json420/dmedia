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
Python convenience API for talking to dmedia components over DBus.
"""

import dbus

from dmedia.constants import BUS
from dmedia.abstractcouch import load_env


class DMedia(object):
    """
    Talk to "org.freedesktop.DMedia".
    """
    def __init__(self, bus=BUS):
        self.bus = bus
        self.conn = dbus.SessionBus()
        self._proxy = None

    @property
    def proxy(self):
        if self._proxy is None:
            self._proxy = self.conn.get_object(self.bus, '/')
        return self._proxy

    def version(self):
        return self.proxy.Version()

    def kill(self):
        self.proxy.Kill()
        self._proxy = None

    def get_env(self, env_s=None):
        if not env_s:
            env_s = self.proxy.GetEnv()
        return load_env(env_s)

    def get_auth_url(self):
        return self.proxy.GetAuthURL()

    def has_app(self):
        return self.proxy.HasApp()


class DMediaImporter(object):
    """
    Talk to "org.freedesktop.DMediaImporter".
    """
