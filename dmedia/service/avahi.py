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
Advertise Dmedia HTTP services over Avahi, discover other peers.
"""

import logging
import json
import time
from collections import namedtuple

from filestore import _start_thread
from microfiber import Server, Database, NotFound
import dbus
from gi.repository import GObject

from dmedia import util, views

log = logging.getLogger()
Peer = namedtuple('Peer', 'env names')
PEERS = '_local/peers'


class Avahi:
    """
    Base class to capture the messy Avahi DBus details.
    """

    service = '_example._tcp'

    def __init__(self, _id, port):
        self.group = None
        self.id = _id
        self.port = port

    def __del__(self):
        self.free()

    def run(self):
        system = dbus.SystemBus()
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        self.group = system.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info(
            'Avahi(%s): advertising %s on port %s', self.service, self.id, self.port
        )
        self.group.AddService(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.id,
            self.service,
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
            self.service,
            'local',
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = system.get_object('org.freedesktop.Avahi', browser_path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def free(self):
        if self.group is not None:
            log.info(
                'Avahi(%s): freeing %s on port %s', self.service, self.id, self.port
            )
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None
            del self.browser
            del self.avahi

    def on_ItemNew(self, interface, protocol, key, _type, domain, flags):
        # Always Ignore what we publish ourselves:
        if key == self.id:
            return
        # Subclasses can add finer-graned ignore behaviour:
        if self.ignore_peer(interface, protocol, key, _type, domain, flags):
            return
        self.avahi.ResolveService(
            interface, protocol, key, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        key = args[2]
        (ip, port) = args[7:9]
        url = 'http://{}:{}/'.format(ip, port)
        log.info('Avahi(%s): new peer %s at %s', self.service, key, url)
        self.add_peer(key, url)

    def on_error(self, exception):
        log.error('%s: error calling ResolveService(): %r', self.service, exception)

    def on_ItemRemove(self, interface, protocol, key, _type, domain, flags):
        log.info('Avahi(%s): peer removed: %s', self.service, key)
        self.remove_peer(key)

    def ignore_peer(self, interface, protocol, key, _type, domain, flags):
        return False

    def add_peer(self, key, url):
        raise NotImplementedError(
            '{}.add_peer()'.format(self.__class__.__name__)
        )

    def remove_peer(self, key):
        raise NotImplementedError(
            '{}.remove_peer()'.format(self.__class__.__name__)
        )


class FileServer(Avahi):
    """
    Advertise HTTP file server server over Avahi, discover other peers.
    """
    service = '_dmedia._tcp'

    def __init__(self, env, port):
        self.db = Database('dmedia-0', env)
        _id = env['machine_id']
        super().__init__(_id, port)

    def run(self):
        try:
            self.peers = self.db.get(PEERS)
            if self.peers.get('peers') != {}:
                self.peers['peers'] = {}
                self.db.save(self.peers)
        except NotFound:
            self.peers = {'_id': PEERS, 'peers': {}}
            self.db.save(self.peers)
        super().run()

    def add_peer(self, key, url):
        self.peers['peers'][key] = url
        self.db.save(self.peers)

    def remove_peer(self, key):
        try:
            del self.peers['peers'][key]
            self.db.save(self.peers)
        except KeyError:
            pass


class Replicator(Avahi):
    """
    Advertise CouchDB over Avahi, discover other peers in same library.
    """

    service = '_usercouch._tcp'

    def __init__(self, env, config):
        self.env = env
        self.server = Server(env)
        self.peers = {}
        self.base_id = config['library_id'] + '-'
        self.tokens = config.get('tokens')
        _id = self.base_id + env['machine_id']
        port = env['port']
        super().__init__(_id, port)

    def run(self):
        super().run()
        # Every 15 seconds we check for database created since the replicator
        # started
        self.timeout_id = GObject.timeout_add(15000, self.on_timeout)

    def on_timeout(self):
        if not self.peers:
            return True  # Repeat timeout call
        current = set(self.get_names())
        for (key, peer) in self.peers.items():
            new = current - set(peer.names)
            if new:
                log.info('New databases: %r', sorted(new))
                tmp = Peer(peer.env, tuple(new))
                peer.names.extend(new)
                _start_thread(self.replication_worker, None, tmp)
        return True  # Repeat timeout call

    def ignore_peer(self, interface, protocol, key, _type, domain, flags):
        # Ignore peers in other libraries:
        return not key.startswith(self.base_id)

    def add_peer(self, key, url):
        env = {'url': url, 'oauth': self.tokens}
        cancel = self.peers.pop(key, None)
        start = Peer(env, list(self.get_names()))
        self.peers[key] = start
        _start_thread(self.replication_worker, cancel, start)

    def remove_peer(self, key):
        cancel = self.peers.pop(key, None)
        if cancel:
            _start_thread(self.replication_worker, cancel, None)

    def get_names(self):
        for name in self.server.get('_all_dbs'):
            if name.startswith('dmedia-0') or name.startswith('novacut-0'):
                yield name

    def replication_worker(self, cancel, start):
        if cancel:
            for name in cancel.names:
                self.replicate(name, cancel.env, cancel=True)
        if start:
            for name in start.names:
                #time.sleep(0.25)
                self.replicate(name, start.env)
        log.info('replication_worker() done')

    def replicate(self, name, env, cancel=False):
        """
        Start or cancel push replication of database *name* to peer at *url*.

        Security note: we only do push replication because pull replication
        would allow unauthorized peers to write to our databases via their
        changes feed.  For both push and pull, there is currently no privacy
        whatsoever... everything is in cleartext and uses oauth 1.0a. But push
        replication is the only way to at least prevent malicious data
        corruption.
        """
        kw = {
            'continuous': True,
            'create_target': True,
            'filter': 'doc/normal',
        }
        if cancel:
            kw['cancel'] = True
            log.info('Canceling push of %s to %s', name, env['url'])
        else:
            log.info('Starting push of %s to %s', name, env['url'])
            db = self.server.database(name)
            util.update_design_doc(db, views.doc_design)
        try:
            self.server.push(name, name, env, **kw)
        except Exception as e:
            if cancel:
                log.exception('Error canceling push of %s to %s', name, env['url'])
            else:
                log.exception('Error starting push of %s to %s', name, env['url'])
