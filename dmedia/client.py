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
Python convenience wrapper for talking to dmedia components over DBus.
"""

import json

import dbus

from dmedia.constants import BUS


class DMedia(object):
    """
    Talk to "org.freedesktop.DMedia".
    """
    def __init__(self, bus=BUS):
        self.conn = dbus.SessionBus()
        self.proxy = self.conn.get_object(BUS, '/')

    def get_env(self, env_s=None):
        if not env_s:
            env_s = self.proxy.GetEnv()
        env = json.loads(env_s)
        # FIXME: hack to work-around for Python oauth not working with unicode,
        # which is what we get when the env is retrieved over D-Bus as JSON
        if 'oauth' in env:
            env['oauth'] = dict(
                (k.encode('ascii'), v.encode('ascii'))
                for (k, v) in env['oauth'].iteritems()
            )
        return env
