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
    group = None

    def __init__(self, env, port):
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
#        browser_path = self.avahi.ServiceBrowserNew('(iissu)',
#            -1,  # Interface
#            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
#            '_dmedia._tcp',
#            'local',
#            0  # Flags
#        )
#        self.browser = system.get(
#            'org.freedesktop.Avahi',
#            browser_path,
#            'org.freedesktop.Avahi.ServiceBrowser'
#        )
        #self.browser.connect('g-signal', self.on_g_signal)

    def free(self):
        if self.group is not None:
            self.group.Reset()
 
    def on_g_signal(self, proxy, sender, signal, params):
        if signal == 'ItemNew':
            (interface, protocol, name, _type, domain, flags) = params.unpack()
            if name != self.machine_id:  # Ignore what we publish ourselves
                (ip, port) = self.avahi.ResolveService('(iisssiu)',
                    interface, protocol, name, _type, domain, -1, 0
                )[7:9]
                url = 'http://{}:{}/'.format(ip, port)
                log.info('Avahi: new peer %r at %r', name, url)
                self.peers['peers'][name] = url
                self.db.save(self.peers)
        elif signal == 'ItemRemove':
            (interface, protocol, name, _type, domain, flags) = params.unpack()
            log.info('Avahi: removing peer %r', name)
            try:
                del self.peers['peers'][name]
                self.db.save(self.peers)
            except KeyError:
                pass
