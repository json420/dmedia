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

Peer = namedtuple('Peer', 'id ip port')
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


class AvahiPeer(GObject.GObject):
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
        self.group = None
        self.pki = pki
        self.client_mode = client_mode
        self.id = (pki.machine.id if client_mode else pki.user.id)
        self.cert_file = pki.verify_ca(self.id)
        self.key_file = pki.verify_key(self.id)
        self.peer_id = None
        self.peer = None
        self.info = None
        self.__active = False
        self.bus = dbus.SystemBus()
        self.avahi = self.bus.get_object('org.freedesktop.Avahi', '/')

    def __del__(self):
        self.unpublish()

    def activate(self):
        assert self.__active is False
        assert self.peer_id is not None
        assert self.peer.id == self.peer_id
        assert self.info.id == self.peer_id
        assert self.info.url == 'https://{}:{}/'.format(
            self.peer.ip, self.peer.port
        )
        log.info('Activating session with %r', self.peer)
        self.__active = True

    def accept(self, port):
        assert self.client_mode is False
        self.activate()
        self.publish(port)

    def get_server_config(self):
        """
        Get the initial server SSL config.
        """
        config = {
            'key_file': self.key_file,
            'cert_file': self.cert_file,
        }
        if not self.client_mode:
            config['ca_file'] = self.pki.verify_ca(self.peer_id)
        return config

    def abort(self, peer_id):
        if self.__active:
            log.error('Session has been activated, cannot abort')
            return
        if peer_id is None or peer_id != self.peer_id:
            log.error('Abort for wrong peer_id, not aborting')
            return
        log.info('Aborting session for %s', peer_id)
        GObject.timeout_add(10 * 1000, self.on_timeout, peer_id)

    def on_timeout(self, peer_id):
        if peer_id is None or peer_id != self.peer_id:
            log.error('Timeout for wrong peer_id, not resetting')
            return
        log.info('Rate-limiting timeout reached, reseting from %s', peer_id)
        self.info = None
        self.peer = None
        self.peer_id = None

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

    def on_ItemNew(self, interface, protocol, peer_id, _type, domain, flags):
        if self.peer_id is not None:
            log.warning('Possible attack from %s', peer_id)
            return
        assert self.peer_id is None
        log.info('Peer added: %s', peer_id)
        self.peer_id = peer_id
        self.avahi.ResolveService(
            interface, protocol, peer_id, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        peer_id = args[2]
        assert peer_id == self.peer_id
        assert self.peer is None
        (ip, port) = args[7:9]
        log.info('%s is at %s, port %s', peer_id, ip, port)
        self.peer = Peer(str(peer_id), str(ip), int(port))
        _start_thread(self.cert_thread, self.peer)

    def on_error(self, error):
        log.error('%s: error calling ResolveService(): %r', self.peer_id, error)
        self.abort(self.peer_id)

    def on_ItemRemove(self, interface, protocol, peer_id, _type, domain, flags):
        log.info('Peer removed: %s', peer_id)
        if peer_id == self.peer_id:
            self.abort(self.peer_id)

    def cert_thread(self, peer):
        # 1 Retrieve the peer certificate:
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            ctx.options |= ssl.OP_NO_COMPRESSION
            if self.client_mode:
                # The server will only let its cert be retrieved by the client
                # bound to the peering session
                ctx.load_cert_chain(self.cert_file, self.key_file)
            sock = ctx.wrap_socket(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            )
            sock.connect((peer.ip, peer.port))
            pem = ssl.DER_cert_to_PEM_cert(sock.getpeercert(True))
        except Exception as e:
            log.exception('Could not retrieve cert for %r', peer)
            return self.abort(peer.id)
        log.info('Retrieved cert for %r', peer)

        # 2 Make sure peer cert has correct intrinsic CN, etc:
        try:
            ca_file = self.pki.write_ca(peer.id, pem.encode('ascii'))
        except Exception as e:
            log.exception('Could not verify cert for %r', peer)
            return self.abort(peer.id)
        log.info('Verified cert for %r', peer)

        # 3 Make get request to verify peer has private key:
        try:
            url = 'https://{}:{}/'.format(peer.ip, peer.port)
            ssl_config = {
                'ca_file': ca_file,
                'check_hostname': False,
            }
            if self.client_mode:
                ssl_config.update({
                    'key_file': self.key_file,
                    'cert_file': self.cert_file,
                })
            client = CouchBase({'url': url, 'ssl': ssl_config})
            d = client.get()
            info = Info(d['user'], d['host'], url, peer.id)
        except Exception as e:
            log.exception('GET / failed for %r', peer)
            return self.abort(peer.id)
        log.info('GET / succeeded with %r', info)
        GObject.idle_add(self.on_cert_complete, peer, info)

    def on_cert_complete(self, peer, info):
        assert peer.id == self.peer_id
        assert peer is self.peer
        assert self.info is None
        self.info = info
        log.info('Cert checked-out for %r', peer)
        signal = ('accept' if self.client_mode else 'offer')
        log.info('Firing %r signal for %r', signal, info)
        self.emit(signal, info)
