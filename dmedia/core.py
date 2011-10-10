# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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

"""
Core dmedia entry-point/API - start here!

For background, please see:

    https://bugs.launchpad.net/dmedia/+bug/753260


Security note on /dmedia/_local/dmedia
======================================

When `DMedia.init_filestores()` is called, it creates `FileStore` instances
based solely on information in the non-replicated /dmedia/_local/dmedia
document... despite the fact that the exact same information is also available
in the corresponding 'dmedia/store' documents.

When it comes to deciding what files dmedia will read and write, it's prudent to
assume that replicated documents are untrustworthy.

The dangerous approach would be to use a view to get all the 'dmedia/store'
documents with a machine_id that matches this machine_id, and initialize those `FileStore`.  But the problem is that if an attacker gained control of just one
of your replicating peers or services, they could insert arbitrary
'dmedia/store' documents, and have dmedia happily initialize `FileStore` at
those mount points.  And that would be "a bad thing".

So although the corresponding 'dmedia/store' records are created (if they don't
already exists), they are completely ignored when it comes to deciding what
filestores and mount points are configured.
"""

import logging
from copy import deepcopy
import os
from os import path
import json
import time
import stat

from microfiber import Database, NotFound, Conflict
from filestore import FileStore, DOTNAME

from .transfers import TransferManager
from dmedia import schema
from dmedia.local import LocalStores
from .schema import random_id, create_machine, create_store
from .views import init_views


LOCAL_ID = '_local/dmedia'


log = logging.getLogger()


def start_file_server(env, queue):
    from wsgiref.simple_server import make_server, demo_app
    httpd = make_server('', 0, demo_app)
    (ip, port) = httpd.socket.getsockname()
    queue.put(port)
    httpd.serve_forever()


class Core2:
    def __init__(self, env):
        self.env = env
        self.db = Database('dmedia', env)
        self.stores = LocalStores()

    def bootstrap(self):
        self.db.ensure()
        self.init_local()

    def init_local(self):
        try:
            self.local = self.db.get(LOCAL_ID)
        except NotFound:
            machine = create_machine()
            self.local = {
                '_id': LOCAL_ID,
                'machine_id': machine['_id'],
                'stores': {},
            }
            self.db.save(self.local)
            self.db.save(machine)
        self.machine_id = self.local['machine_id']
        self.env['machine_id'] = self.machine_id

    def init_stores(self):
        if not self.local['stores']:
            return
        dirs = sorted(self.local['stores'])
        for parentdir in dirs:
            try:
                fs = self.init_filestore(parentdir)
                self.stores.add(fs)
                self.local['stores'][parentdir] = {
                    'id': fs.id,
                    'copies': fs.copies,
                }
            except Exception:
                del self.local['stores'][parentdir]
        self.db.save(self.local)

    def init_filestore(self, parentdir, copies=1):
        fs = FileStore(parentdir)
        f = path.join(fs.basedir, 'store.json')
        try:
            doc = json.load(open(f, 'r'))
            try:
                doc = self.db.get(doc['_id'])
            except NotFound:
                pass
        except Exception:
            doc = schema.create_filestore(copies)
            json.dump(doc, open(f, 'w'), sort_keys=True, indent=4)
            os.chmod(f, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        doc['connected'] = time.time()
        doc['connected_to'] = self.machine_id
        doc['statvfs'] = fs.statvfs()._asdict()
        self.db.save(doc)
        fs.id = doc['_id']
        fs.copies = doc.get('copies', copies)
        return fs

    def add_filestore(self, parentdir, copies=1, fast=True):
        pass

    def add_filestore(self, parentdir, copies=1, fast=True):
        pass

    def remove_filestore(self, parentdir):
        del self.local['stores'][parentdir]
        self.db.save(self.local)


class Core(object):
    def __init__(self, env, callback=None):
        self.env = env
        self.home = path.abspath(os.environ['HOME'])
        if not path.isdir(self.home):
            raise ValueError('HOME is not a dir: {!}'.format(self.home))
        self.db = Database('dmedia', env)
        self.db.ensure()
        self.manager = TransferManager(self.env, callback)
        self._filestores = {}

    def bootstrap(self):
        (self.local, self.machine) = self.init_local()
        self.machine_id = self.machine['_id']
        self.env['machine_id'] = self.machine_id
        self.env['filestore'] = self.init_filestores()
        init_views(self.db)

    def init_local(self):
        """
        Get the /dmedia/_local/dmedia document, creating it if needed.
        """
        try:
            local = self.db.get(LOCAL_ID)
        except NotFound:
            machine = create_machine()
            local = {
                '_id': LOCAL_ID,
                'machine': deepcopy(machine),
                'filestores': {},
            }
            self.db.save(local)
            self.db.save(machine)
        else:
            try:
                machine = self.db.get(local['machine']['_id'])
            except NotFound:
                machine = deepcopy(local['machine'])
                self.db.save(machine)
        return (local, machine)

    def init_filestores(self):
        if not self.local['filestores']:
            self.add_filestore(self.home)
        else:
            for (parentdir, store) in self.local['filestores'].items():
                assert store['parentdir'] == parentdir
                try:
                    self.init_filestore(store)
                except Exception:
                    pass
                try:
                    self.db.save(deepcopy(store))
                except Conflict:
                    pass
            if self.local.get('default_filestore') not in self.local['filestores']:
                self.local['default_filestore'] = store['parentdir']
        return self.local['filestores'][self.local['default_filestore']]

    def init_filestore(self, store):
        parentdir = store['parentdir']
        self._filestores[parentdir] = FileStore(parentdir)

    def add_filestore(self, parentdir):
        parentdir = path.abspath(parentdir)
        if not path.isdir(parentdir):
            raise ValueError('Not a directory: {!r}'.format(parentdir))
        try:
            return self.local['filestores'][parentdir]
        except KeyError:
            pass
        log.info('Added filestore at %r', parentdir)
        store = create_store(parentdir, self.machine_id)
        self.init_filestore(store)
        self.local['filestores'][parentdir] = deepcopy(store)
        self.local['default_filestore'] = store['parentdir']
        self.db.save(self.local)
        self.db.save(store)
        return store

    def get_file(self, file_id):
        for fs in self._filestores.values():
            filename = fs.path(file_id)
            if path.isfile(filename):
                return filename

    def upload(self, file_id, store_id):
        return self.manager.upload(file_id, store_id)

    def download(self, file_id, store_id):
        return self.manager.download(file_id, store_id)

    def verify(self, file_id, store_id):
        pass
