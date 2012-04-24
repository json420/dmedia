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
Advertise Dmedia HTTP server over Avahi, discover other peers.
"""

import logging

from microfiber import Database, NotFound
import dbus


PEERS = '_local/peers'
log = logging.getLogger()
system = dbus.SystemBus()


class Avahi:
    def __init__(self, env, port):
        self.group = None
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        self.db = Database('dmedia-0', env)
        self.machine_id = env['machine_id']
        self.port = port

    def __del__(self):
        self.free()

    def run(self):
        try:
            self.peers = self.db.get(PEERS)
            if self.peers.get('peers') != {}:
                self.peers['peers'] = {}
                self.db.save(self.peers)
        except NotFound:
            self.peers = {'_id': PEERS, 'peers': {}}
            self.db.save(self.peers)
        self.group = system.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info('Avahi: advertising %r on port %r', self.machine_id, self.port)
        self.group.AddService(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.machine_id,
            '_dmedia._tcp',
            '',  # Domain, default to .local
            '',  # Host, default to localhost
            self.port,  # Port
            b'',  # TXT record
            dbus_interface='org.freedesktop.Avahi.EntryGroup'
        )
        self.group.Commit(dbus_interface='org.freedesktop.Avahi.EntryGroup')
        browser_path = self.avahi.ServiceBrowserNew(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            '_dmedia._tcp',
            'local',
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = system.get_object('org.freedesktop.Avahi', browser_path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def free(self):
        if self.group is not None:
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')

    def on_ItemNew(self, interface, protocol, name, _type, domain, flags):
        if name == self.machine_id:  # Ignore what we publish ourselves
            return
        (ip, port) = self.avahi.ResolveService(
            interface, protocol, name, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server'
        )[7:9]
        url = 'http://{}:{}/'.format(ip, port)
        log.info('Avahi: new peer %s at %s', name, url)
        self.peers['peers'][name] = url
        self.db.save(self.peers)

    def on_ItemRemove(self, interface, protocol, name, _type, domain, flags):
        log.info('Avahi: removing peer %s', name)
        try:
            del self.peers['peers'][name]
            self.db.save(self.peers)
        except KeyError:
            pass
