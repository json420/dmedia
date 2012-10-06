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
from collections import namedtuple
import ssl

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject
from microfiber import _start_thread, random_id, CouchBase, dumps


GObject.threads_init()
DBusGMainLoop(set_as_default=True)
log = logging.getLogger()

Remote = namedtuple('Remote', 'id ip port')


class Peer(GObject.GObject):
    __gsignals__ = {
        'peer_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # remote_id, url
            [GObject.TYPE_PYOBJECT, GObject.TYPE_PYOBJECT]
        ),
        'peer_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # remote_id, url
            [GObject.TYPE_PYOBJECT, GObject.TYPE_PYOBJECT]
        ),
    }

    def __init__(self, _id, pki):
        super().__init__()
        self.id = _id
        self.pki = pki
        self.remote_id = None
        self.remote = None
        self.group = None
        self.bus = dbus.SystemBus()
        self.avahi = self.bus.get_object('org.freedesktop.Avahi', '/')

    def __del__(self):
        self.unpublish()

    def abort(self, remote_id):
        GObject.timeout_add(10 * 1000, self.on_abort_timeout, remote_id)

    def on_abort_timeout(self, remote_id):
        if remote_id is None or remote_id != self.remote_id:
            log.error('abort for wrong remote_id, not aborting')
            return
        log.info('aborting session for %s', remote_id)
        self.remote_id = None
        self.remote = None

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
        log.info('browsing for %r', service)
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

    def on_ItemNew(self, interface, protocol, remote_id, _type, domain, flags):
        if self.remote_id is not None:
            log.warning('possible attack from %s', remote_id)
            return
        assert self.remote_id is None
        log.info('starting peering session with %s', remote_id)
        self.remote_id = remote_id
        self.avahi.ResolveService(
            interface, protocol, remote_id, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        remote_id = args[2]
        assert remote_id == self.remote_id
        assert self.remote is None
        (ip, port) = args[7:9]
        log.info('%s is at %s, port %s', remote_id, ip, port)
        self.remote = Remote(str(remote_id), str(ip), int(port))
        _start_thread(self.check_thread, self.remote)

    def on_error(self, error):
        log.error('%s error calling ResolveService(): %r', self.remote_id, error)
        self.abort(self.remote_id)

    def on_ItemRemove(self, interface, protocol, remote_id, _type, domain, flags):
        log.info('Peer removed: %s', remote_id)
        self.abort(self.remote_id)

    def check_thread(self, remote):
        try:
            address = (remote.ip, remote.port)
            pem = ssl.get_server_certificate(address, ssl.PROTOCOL_TLSv1)
        except Exception as e:
            log.exception('Could not retrieve cert for %r', remote)
            return self.abort(remote.id)
        log.info('Retrieved cert for %r', remote)
        try:
            ca_file = self.pki.write_ca(remote.id, pem.encode('ascii'))
        except Exception as e:
            log.exception('Could not verify cert for %r', remote)
            return self.abort(remote.id)
        log.info('Verified cert for %r', remote)
        try:
            env = {
                'url': 'https://{}:{}/'.format(remote.ip, remote.port),
                'ssl': {
                    'ca_file': ca_file,
                    'check_hostname': False,
                }
            }
            client = CouchBase(env)
            client.get()
        except Exception as e:
            log.exception('GET / failed for %r', remote)
        log.info('GET / succeeded for %r', remote)
        GObject.idle_add(self.on_check_complete, remote, env['url'])

    def on_check_complete(self, remote, url):
        assert remote.id == self.remote_id
        assert remote is self.remote
        log.info('Cert check complete for %r', remote)

            
            
            

