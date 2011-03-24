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
`dmedia` - distributed media library

WARNING: the dmedia content-hash and schema are *not* yet stable, may change
wildly and without warning!

The `dmedia` API will go through significant changes in the next few months,
so keep your hardhats on!  A good place to start is the `FileStore` class in the
`filestore` module, which also probably has the most stable API of any of the
current code.
"""

__version__ = '0.5.0'

import os
from os import path


try:
    import gi
    gi.require_version('Gtk', '2.0')
    gi.require_version('WebKit', '1.0')
    from gi.repository import Gtk
except ImportError:
    pass



packagedir = path.dirname(path.abspath(__file__))
assert path.isdir(packagedir)
datadir = path.join(packagedir, 'data')
assert path.isdir(datadir)


def get_env(dbname=None):
    """
    Get desktopcouch runtime info in most the lightweight way possible.

    Here "lightweight" doesn't necessarily mean "fast", but with as few imports
    as possible to keep the dmedia memory footprint small.
    """
    import dbus
    DC = 'org.desktopcouch.CouchDB'
    conn = dbus.SessionBus()
    proxy = conn.get_object(DC, '/')
    getPort = proxy.get_dbus_method('getPort', dbus_interface=DC)
    port = getPort()
    url = 'http://localhost:%d/' % port

    import gnomekeyring
    data = gnomekeyring.find_items_sync(
        gnomekeyring.ITEM_GENERIC_SECRET,
        {'desktopcouch': 'oauth'}
    )
    oauth = dict(zip(
        ('consumer_key', 'consumer_secret', 'token', 'token_secret'),
        data[0].secret.split(':')
    ))

    return dict(port=port, url=url, oauth=oauth, dbname=dbname)
