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
import time
from collections import namedtuple

from filestore import _start_thread
from microfiber import Server, Database, PreconditionFailed
import dbus


log = logging.getLogger()
system = dbus.SystemBus()
Peer = namedtuple('Peer', 'env names')
SERVICE = '_usercouch._tcp'


def get_body(source, target, cancel=False):
    body = {
        'source': source,
        'target': target,
        'continuous': True,
    }
    if cancel:
        body['cancel'] = True
    return body


def get_peer(env, dbname):
    return {
        'url': env['url'] + dbname,
        'auth': {
            'oauth': env['oauth'],
        },
    }


class Replicator:
    def __init__(self, env, config):
        self.group = None
        self.env = env
        self.server = Server(env)
        self.library_id = config['library_id']
        self.base_id = self.library_id + '-'
        self.port = env['port']
        self.tokens = config['tokens']
        self.peers = {}

    def __del__(self):
        self.free()

    def run(self):
        self.machine_id = self.env['machine_id']
        self.id = self.base_id + self.machine_id
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

    def on_ItemNew(self, interface, protocol, key, _type, domain, flags):
        if key == self.id:  # Ignore what we publish ourselves
            return
        if not key.startswith(self.base_id):  # Ignore other libraries
            return
        (ip, port) = self.avahi.ResolveService(
            interface, protocol, key, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server'
        )[7:9]
        url = 'http://{}:{}/'.format(ip, port)
        log.info('Replicator: new peer %s at %s', key, url)
        env = {'url': url, 'oauth': self.tokens}
        cancel = self.peers.pop(key, None)
        start = Peer(env, list(self.get_names()))
        self.peers[key] = start
        _start_thread(self.replication_worker, cancel, start)

    def on_ItemRemove(self, interface, protocol, key, _type, domain, flags):
        log.info('Replicator: peer removed %s', key)
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
                self.replicate(cancel.env, name, cancel=True)
        if start:
            remote = Server(start.env)
            for name in start.names:
                if name != 'dmedia-0':
                    # Create remote DB if needed
                    try:
                        remote.put(None, name)
                        log.info('Created %s in %r', name, remote)
                    except PreconditionFailed:
                        pass
                    except Exception as e:
                        log.exception('Error creating %s in %r', name, remote)
                time.sleep(0.25)
                self.replicate(start.env, name)
        log.info('replication_worker() done')

    def replicate(self, env, name, cancel=False):
        """
        Start or cancel push replication of database *name* to peer at *url*.

        Security note: we only do push replication because pull replication
        would allow unauthorized peers to write to our databases via their
        changes feed.  For both push and pull, there is currently no privacy
        whatsoever... everything is in cleartext and uses oauth 1.0a. But push
        replication is the only way to at least prevent malicious data
        corruption.
        """
        if cancel:
            log.info('Canceling push of %s to %s', name, env['url'])
        else:
            log.info('Starting push of %s to %s', name, env['url'])
        peer = get_peer(env, name)
        push = get_body(name, peer, cancel)
        try:
            self.server.post(push, '_replicate')
        except Exception as e:
            if cancel:
                log.exception('Error canceling push of %s to %s', name, env['url'])
            else:
                log.exception('Error starting push of %s to %s', name, env['url'])
            

        
        
        
        