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
from microfiber import Context, Server, Database, NotFound, CouchBase, dumps
import dbus
from gi.repository import GObject

from dmedia.parallel import start_thread
from dmedia import util, views

log = logging.getLogger()
PROTO = 0  # Protocol -1 = both, 0 = IPv4, 1 = IPv6
Peer = namedtuple('Peer', 'env names')
PEERS = '_local/peers'


def make_url(ip, port):
    if PROTO == 0:
        return 'https://{}:{}/'.format(ip, port)
    elif PROTO == 1:
        return 'https://[{}]:{}/'.format(ip, port)
    raise Exception('bad PROTO')


class Avahi:

    service = '_dmedia._tcp'

    def __init__(self, env, port, ssl_config):
        self.group = None
        self.machine_id = env['machine_id']
        self.user_id = env['user_id']
        self.port = port
        self.ssl_config = ssl_config
        ctx = Context(env)
        self.db = Database('dmedia-0', ctx=ctx)
        self.server = Server(ctx=ctx)

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
        system = dbus.SystemBus()
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        self.group = system.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info('Avahi: advertising %s on port %s', self.machine_id, self.port)
        self.group.AddService(
            -1,  # Interface
            PROTO,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.machine_id,
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
            PROTO,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            self.service,
            '', # Domain, default to .local
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = system.get_object('org.freedesktop.Avahi', browser_path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def free(self):
        if self.group is not None:
            log.info('Avahi: freeing %s on port %s', self.machine_id, self.port)
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None
            del self.browser
            del self.avahi

    def on_ItemRemove(self, interface, protocol, key, _type, domain, flags):
        log.info('Avahi: peer removed: %s', key)
        GObject.idle_add(self.remove_peer, key)

    def on_ItemNew(self, interface, protocol, key, _type, domain, flags):
        # Ignore what we publish ourselves:
        if key == self.machine_id:
            return
        self.avahi.ResolveService(
            # 2nd to last arg is Protocol, again for some reason
            interface, protocol, key, _type, domain, PROTO, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        key = args[2]
        (ip, port) = args[7:9]
        url = make_url(ip, port)
        log.info('Avahi: new peer %s at %s', key, url)
        start_thread(self.info_thread, key, url)

    def on_error(self, exception):
        log.error('Avahi: error calling ResolveService(): %r', exception)

    def info_thread(self, key, url):
        try:
            env = {'url': url, 'ssl': self.ssl_config}
            client = CouchBase(env)
            info = client.get()
            assert info.pop('user_id') == self.user_id
            assert info.pop('machine_id') == key
            info['url'] = url
            log.info('Avahi: got peer info: %s', dumps(info, pretty=True))
            GObject.idle_add(self.add_peer, key, info)
        except Exception:
            log.exception('Avahi: could not get info for %s', url)

    def add_peer(self, key, info):
        self.peers['peers'][key] = info
        self.db.save(self.peers)

    def remove_peer(self, key):
        try:
            del self.peers['peers'][key]
            self.db.save(self.peers)
        except KeyError:
            pass
        



class FileServer(Avahi):
    """
    Advertise HTTP file server server over Avahi, discover other peers.
    """
    service = '_dmedia._tcp'

    def __init__(self, env, port):
        super().__init__(env['machine_id'], port)
        ctx = Context(env)
        self.db = Database('dmedia-0', ctx=ctx)
        self.server = Server(ctx=ctx)
        self.replications = {}

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
        self.timeout_id = GObject.timeout_add(15000, self.on_timeout)

    def add_peer(self, key, url):
        self.peers['peers'][key] = url
        self.db.save(self.peers)
        self.add_replication_peer(key, url)

    def remove_peer(self, key):
        try:
            del self.peers['peers'][key]
            self.db.save(self.peers)
        except KeyError:
            pass
        self.remove_replication_peer(key)

    def on_timeout(self):
        if not self.replications:
            return True  # Repeat timeout call
        current = set(self.get_names())
        for (key, peer) in self.replications.items():
            new = current - set(peer.names)
            if new:
                log.info('New databases: %r', sorted(new))
                tmp = Peer(peer.env, tuple(new))
                peer.names.extend(new)
                _start_thread(self.replication_worker, None, tmp)
        return True  # Repeat timeout call

    def add_replication_peer(self, key, url):
        env = {'url': url + 'couch/'}
        cancel = self.replications.pop(key, None)
        start = Peer(env, list(self.get_names()))
        self.replications[key] = start
        _start_thread(self.replication_worker, cancel, start)

    def remove_replication_peer(self, key):
        cancel = self.replications.pop(key, None)
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
    
