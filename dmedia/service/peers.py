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
import socket

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject
from microfiber import _start_thread, random_id, CouchBase, dumps, build_ssl_context


GObject.threads_init()
DBusGMainLoop(set_as_default=True)
log = logging.getLogger()

Remote = namedtuple('Remote', 'id ip port')
Info = namedtuple('Info', 'name host url id')


def get_service(verb):
    """
    Get Avahi service name for appropriate direction.

    For example, for an offer:

    >>> get_service('offer')
    '_dmedia-offer._tcp'

    And for an accept:

    >>> get_service('accept')
    '_dmedia-accept._tcp'

    """
    assert verb in ('offer', 'accept')
    return '_dmedia-{}._tcp'.format(verb)


class Peer(GObject.GObject):
    __gsignals__ = {
        'offer': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
        'accept': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [GObject.TYPE_PYOBJECT]
        ),
    }

    def __init__(self, pki, client_mode=False):
        super().__init__()
        self.pki = pki
        self.client_mode = client_mode
        self.id = (pki.machine.id if client_mode else pki.user.id)
        self.cert_file = pki.verify_ca(self.id)
        self.key_file = pki.verify_key(self.id)
        self.remote_id = None
        self.remote = None
        self.group = None
        self.bus = dbus.SystemBus()
        self.avahi = self.bus.get_object('org.freedesktop.Avahi', '/')

    def __del__(self):
        self.unpublish()

    def get_server_config(self):
        """
        Get the initial server SSL config.
        """
        config = {
            'key_file': self.key_file,
            'cert_file': self.cert_file,
        }
        if not self.client_mode:
            config['ca_file'] = self.pki.verify_ca(self.remote_id)
        return config

    def abort(self, remote_id):
        if remote_id is None or remote_id != self.remote_id:
            log.error('abort for wrong remote_id, not aborting')
            return
        log.info('aborting session for %s', remote_id)
        GObject.timeout_add(10 * 1000, self.on_timeout, remote_id)

    def on_timeout(self, remote_id):
        if remote_id is None or remote_id != self.remote_id:
            log.error('timeout for wrong remote_id, not resetting')
            return
        log.info('rate-limiting timeout reached, reseting from %s', remote_id)
        self.remote_id = None
        self.remote = None

    def publish(self, port):
        verb = ('offer' if self.client_mode else 'accept')
        service = get_service(verb)
        self.group = self.bus.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info(
            'Publishing %s for %r on port %s', self.id, service, port
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
            log.info('Un-publishing %s', self.id)
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None

    def browse(self):
        verb = ('accept' if self.client_mode else 'offer')
        service = get_service(verb)
        log.info('Browsing for %r', service)
        path = self.avahi.ServiceBrowserNew(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            service,
            '', # Domain, default to .local
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
        log.error('%s: error calling ResolveService(): %r', self.remote_id, error)
        self.abort(self.remote_id)

    def on_ItemRemove(self, interface, protocol, remote_id, _type, domain, flags):
        log.info('Peer removed: %s', remote_id)
        if remote_id == self.remote_id:
            self.abort(self.remote_id)

    def check_thread(self, remote):
        try:
            address = (remote.ip, remote.port)
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            ctx.options |= ssl.OP_NO_COMPRESSION
            if self.client_mode:
                # The server will only let its cert be retrieved by the client
                # bound to the peering session
                ctx.load_cert_chain(self.cert_file, self.key_file)
            sock = ctx.wrap_socket(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            )
            sock.connect(address)
            pem = ssl.DER_cert_to_PEM_cert(sock.getpeercert(True))
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
            url = 'https://{}:{}/'.format(remote.ip, remote.port)
            ssl_config = {
                'ca_file': ca_file,
                'check_hostname': False,
            }
            if self.client_mode:
                ssl_config.update({
                    'key_file': self.key_file,
                    'cert_file': self.cert_file,
                })
            env = {
                'url': url,
                'ssl': ssl_config,
            }
            client = CouchBase(env)
            d = client.get()
            info = Info(d['user'], d['host'], url, remote.id)
        except Exception as e:
            log.exception('GET / failed for %r', remote)
            return self.abort(remote.id)
        log.info('GET / succeeded for %r', remote)
        log.info('%r', info)
        GObject.idle_add(self.on_check_complete, remote, info)

    def on_check_complete(self, remote, info):
        assert remote.id == self.remote_id
        assert remote is self.remote
        log.info('Cert checked-out for %r', remote)
        signal = ('accept' if self.client_mode else 'offer')
        log.info('Firing %r signal for %r', signal, info)
        self.emit(signal, info)
