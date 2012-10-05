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
Browse for Dmedia peer offerings, publish the same.

Existing machines constantly listen for _dmedia-offer._tcp.

New machine publishes _dmedia-offer._tcp, and listens for _dmedia-accept._tcp.

Existing machine prompts user, and if they accept, machine publishes
_dmedia-accept._tcp, which initiates peering process.
"""

import logging

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject


GObject.threads_init()
DBusGMainLoop(set_as_default=True)
log = logging.getLogger()


class Peer:
    def __init__(self, _id, add_callback, remove_callback):
        self.id = _id
        self.add_callback = add_callback
        self.remove_callback = remove_callback
        self.group = None
        self.bus = dbus.SystemBus()
        self.avahi = self.bus.get_object('org.freedesktop.Avahi', '/')

    def __del__(self):
        self.unpublish()

    def publish(self, service, port):
        self.group = self.bus.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info(
            'Avahi(%s): publishing %s on port %s', service, self.id, port
        )
        self.group.AddService(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.id,
            service,
            '',  # Domain, default to .local
            '',  # Host, default to localhost
            port,  # Port
            b'',  # TXT record
            dbus_interface='org.freedesktop.Avahi.EntryGroup'
        )
        self.group.Commit(dbus_interface='org.freedesktop.Avahi.EntryGroup')

    def unpublish(self):
        if self.group is not None:
            log.info('Avahi(%s): unpublishing %s', self.pservice, self.id)
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None

    def browse(self, service):
        log.info('Avahi(%s): browsing...', service)
        self.service = service
        path = self.avahi.ServiceBrowserNew(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            service,
            'local',
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = self.bus.get_object('org.freedesktop.Avahi', path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def on_ItemNew(self, interface, protocol, key, _type, domain, flags):
        self.avahi.ResolveService(
            interface, protocol, key, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        key = args[2]
        (ip, port) = args[7:9]
        url = 'https://{}:{}/'.format(ip, port)
        log.info('Avahi(%s): new peer %s at %s', self.service, key, url)
        self.add_callback(key, url)

    def on_error(self, exception):
        log.error('%s: error calling ResolveService(): %r',
            self.service, exception
        )

    def on_ItemRemove(self, interface, protocol, key, _type, domain, flags):
        log.info('Avahi(%s): peer removed: %s', self.service, key)
        self.remove_callback(key)

