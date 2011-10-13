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

"""

import os
from os import path
import json
import time
import stat
import multiprocessing

from microfiber import Database, NotFound
from filestore import FileStore, check_root_hash, check_id

from dmedia import schema
from dmedia.local import LocalStores
from dmedia.views import init_views


LOCAL_ID = '_local/dmedia'


def file_server(env, queue):
    try:
        from wsgiref.simple_server import make_server
        from dmedia.server import ReadOnlyApp
        app = ReadOnlyApp(env)
        httpd = make_server('', 0, app)
        port = httpd.socket.getsockname()[1]
        queue.put(port)
        httpd.serve_forever()
    except Exception as e:
        queue.put(e)


def _start_process(target, *args):
    process = multiprocessing.Process(target=target, args=args)
    process.daemon = True
    process.start()
    return process


def start_file_server(env):
    q = multiprocessing.Queue()
    httpd = _start_process(file_server, env, q)
    port = q.get()
    if isinstance(port, Exception):
        raise port
    return (httpd, port)


class Core:
    def __init__(self, env, bootstrap=True):
        self.env = env
        self.db = Database('dmedia', env)
        self.stores = LocalStores()
        if bootstrap:
            self._bootstrap()

    def _bootstrap(self):
        self.db.ensure()
        init_views(self.db)
        self._init_local()
        self._init_stores()

    def _init_local(self):
        try:
            self.local = self.db.get(LOCAL_ID)
        except NotFound:
            machine = schema.create_machine()
            self.local = {
                '_id': LOCAL_ID,
                'machine_id': machine['_id'],
                'stores': {},
            }
            self.db.save(self.local)
            self.db.save(machine)
        self.machine_id = self.local['machine_id']
        self.env['machine_id'] = self.machine_id

    def _init_stores(self):
        if not self.local['stores']:
            return
        dirs = sorted(self.local['stores'])
        for parentdir in dirs:
            try:
                fs = self._init_filestore(parentdir)
                self.stores.add(fs)
                self.local['stores'][parentdir] = {
                    'id': fs.id,
                    'copies': fs.copies,
                }
            except Exception:
                del self.local['stores'][parentdir]
        self.db.save(self.local)
        assert set(self.local['stores']) == set(self.stores.parentdirs)
        assert set(s['id'] for s in self.local['stores'].values()) == set(self.stores.ids)

    def _init_filestore(self, parentdir, copies=1):
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

    def get_doc(self, _id):
        check_id(_id)
        try:
            return self.db.get(_id)
        except microfiber.NotFound:
            raise NoSuchFile(_id)        

    def content_hash(self, _id, unpack=True):
        doc = self.get_doc(_id)
        leaf_hashes = self.db.get_att(_id, 'leaf_hashes')[1]
        return check_root_hash(_id, doc['bytes'], leaf_hashes, unpack)

    def add_filestore(self, parentdir, copies=1):
        if parentdir in self.local['stores']:
            raise Exception('already have parentdir {!r}'.format(parentdir))
        fs = self._init_filestore(parentdir, copies)
        self.stores.add(fs)
        self.local['stores'][parentdir] = {
            'id': fs.id,
            'copies': fs.copies,
        }
        self.db.save(self.local)
        assert set(self.local['stores']) == set(self.stores.parentdirs)
        assert set(s['id'] for s in self.local['stores'].values()) == set(self.stores.ids)
        return fs

    def remove_filestore(self, parentdir):
        fs = self.stores.by_parentdir(parentdir)
        self.stores.remove(fs)
        del self.local['stores'][parentdir]
        self.db.save(self.local)
        assert set(self.local['stores']) == set(self.stores.parentdirs)
        assert set(s['id'] for s in self.local['stores'].values()) == set(self.stores.ids)

