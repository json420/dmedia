# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
Advertise CouchDB HTTP server over Avahi, discover other peers in same library.
"""

import logging
import json

from microfiber import Server, Database, PreconditionFailed
import dbus


log = logging.getLogger()
system = dbus.SystemBus()

SERVICE = '_usercouch._tcp'


def get_body(source, target):
    return {
        'source': source,
        'target': target,
        'continuous': True,
    }


def get_peer(url, dbname, oauth):
    return {
        'url': url + dbname,
        'auth': {
            'oauth': oauth,
        },
    }


class Replicator:
    def __init__(self, env, library_id):
        self.group = None
        self.server = Server(env)
        self.library_id = library_id
        self.base_id = self.library_id + '-'
        self.machine_id = env['machine_id']
        self.id = self.base_id + self.machine_id
        self.port = env['port']
        self.oauth = env['oauth']
        self.peers = {}

    def __del__(self):
        self.free()

    def run(self):
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        self.group = system.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info('Replicator: advertising %r on port %r', self.id, self.port)
        self.group.AddService(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.id,
            SERVICE,
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
            SERVICE,
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
        if name == self.id:  # Ignore what we publish ourselves
            return
        if not name.startswith(self.base_id):  # Ignore other libraries
            return
        (ip, port) = self.avahi.ResolveService(
            interface, protocol, name, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server'
        )[7:9]
        url = 'http://{}:{}/'.format(ip, port)
        log.info('Replicator: new peer %s at %s', name, url)
        self.peers[name] = url
        self.replicate(url, 'foo') 

    def on_ItemRemove(self, interface, protocol, name, _type, domain, flags):
        log.info('Replicator: removing peer %s', name)
        try:
            del self.peers[name]
        except KeyError:
            pass
        print(json.dumps(self.peers, sort_keys=True, indent=4))

    def replicate(self, url, dbname):
        peer = get_peer(url, dbname, self.oauth)
        to = get_body(dbname, peer)
        fro = get_body(peer, dbname)
        for obj in (to, fro):
            print(json.dumps(obj, sort_keys=True, indent=4))
            self.server.post(obj, '_replicate')
        
        
        
        
