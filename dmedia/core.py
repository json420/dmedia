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
from urllib.parse import urlparse
import logging

from microfiber import Database, NotFound, Conflict, random_id2
from filestore import FileStore, check_root_hash, check_id

import dmedia
from dmedia import schema
from dmedia.local import LocalStores, FileNotLocal
from dmedia.views import init_views
from dmedia.util import get_db, get_project_db, init_filestore
from dmedia.units import bytes10


LOCAL_ID = '_local/dmedia'
log = logging.getLogger()


def file_server(env, queue):
    try:
        from wsgiref.simple_server import make_server
        from dmedia.server import ReadOnlyApp
        app = ReadOnlyApp(env)
        httpd = make_server('', 0, app)
        port = httpd.socket.getsockname()[1]
        log.info('Starting HTTP file transfer server on port %d', port)
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


def init_filestore(parentdir, copies=1):
    fs = FileStore(parentdir)
    store = path.join(fs.basedir, 'store.json')
    try:
        doc = json.load(open(store, 'r'))
    except Exception:
        doc = schema.create_filestore(copies)
        json.dump(doc, open(store, 'w'), sort_keys=True, indent=4)
        os.chmod(store, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    fs.id = doc['_id']
    fs.copies = doc['copies']
    return (fs, doc)


class Base:
    def __init__(self, env):
        self.env = env
        self.logdb = Database('dmedia_log', env)

    def log(self, doc):
        doc['_id'] = random_id2()
        doc['machine_id'] = self.env.get('machine_id')
        return self.logdb.post(doc, batch='ok')


class Core:
    def __init__(self, env, get_parentdir_info=None, bootstrap=True):
        self.env = env
        self.db = Database(schema.DB_NAME, env)
        self.stores = LocalStores()
        self._get_parentdir_info = get_parentdir_info
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
        self.env['version'] = dmedia.__version__
        log.info('machine_id = %r', self.machine_id)

    def _init_stores(self):
        if not self.local['stores']:
            return
        dirs = sorted(self.local['stores'])
        log.info('Adding previous FileStore in _local/dmedia')
        for parentdir in dirs:
            try:
                fs = self._init_filestore(parentdir)
                self.stores.add(fs)
                self.local['stores'][parentdir] = {
                    'id': fs.id,
                    'copies': fs.copies,
                }
            except Exception:
                log.exception('Failed to init FileStore %r', parentdir)
                del self.local['stores'][parentdir]
                log.info('Removed %r from _local/dmedia', parentdir)
        self.db.save(self.local)
        assert set(self.local['stores']) == set(self.stores.parentdirs)
        assert set(s['id'] for s in self.local['stores'].values()) == set(self.stores.ids)

    def _init_filestore(self, parentdir, copies=1):
        (fs, doc) = init_filestore(parentdir, copies)
        try:
            doc = self.db.get(doc['_id'])
        except NotFound:
            pass
        doc['connected'] = time.time()
        doc['connected_to'] = self.machine_id
        doc['statvfs'] = fs.statvfs()._asdict()
        #if callable(self._get_parentdir_info):
        #    doc.update(self._get_parentdir_info(parentdir))
        self.db.save(doc)
        log.info('FileStore %r at %r', fs.id, fs.parentdir)
        return fs

    def init_project_views(self):
        for row in self.db.view('project', 'atime')['rows']:
            get_project_db(row['id'], self.env, True)
        log.info('Core.init_project_views() complete')

    def stat(self, _id):
        doc = self.db.get(_id)
        fs = self.stores.choose_local_store(doc)
        return fs.stat(_id)

    def stat2(self, doc):
        fs = self.stores.choose_local_store(doc)
        return fs.stat(doc['_id'])       

    def content_hash(self, _id, unpack=True):
        doc = self.get_doc(_id)
        leaf_hashes = self.db.get_att(_id, 'leaf_hashes')[1]
        return check_root_hash(_id, doc['bytes'], leaf_hashes, unpack)

    def add_filestore(self, parentdir, copies=1):
        log.info('add_filestore(%r, copies=%r)', parentdir, copies)
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
        log.info('remove_filestore(%r)', parentdir)
        fs = self.stores.by_parentdir(parentdir)
        self.stores.remove(fs)
        del self.local['stores'][parentdir]
        self.db.save(self.local)
        assert set(self.local['stores']) == set(self.stores.parentdirs)
        assert set(s['id'] for s in self.local['stores'].values()) == set(self.stores.ids)

    def resolve(self, _id):
        doc = self.db.get(_id)
        fs = self.stores.choose_local_store(doc)
        return fs.stat(_id).name

    def allocate_tmp(self):
        stores = self.stores.sort_by_avail()
        if len(stores) == 0:
            raise Exception('no filestores present')
        tmp_fp = stores[0].allocate_tmp()
        tmp_fp.close()
        return tmp_fp.name

    def resolve_uri(self, uri):
        if not uri.startswith('dmedia:'):
            raise ValueError('not a dmedia: URI {!r}'.format(uri))
        _id = uri[7:]
        doc = self.db.get(_id)
        if doc.get('proxies'):
            proxies = doc['proxies']
            for proxy in proxies:
                try:
                    st = self.stat(proxy)
                    return 'file://' + st.name
                except (NotFound, FileNotLocal):
                    pass
        st = self.stat2(doc)
        return 'file://' + st.name

    def hash_and_move(self, tmp, origin):
        parentdir = path.dirname(path.dirname(path.dirname(tmp)))
        fs = self.stores.by_parentdir(parentdir)
        tmp_fp = open(tmp, 'rb')
        ch = fs.hash_and_move(tmp_fp)
        stored = {
            fs.id: {
                'copies': fs.copies,
                'mtime': fs.stat(ch.id).mtime,
                'plugin': 'filestore',
            }
        }
        try:
            doc = self.db.get(ch.id)
            doc['stored'].update(stored)
        except NotFound:
            doc = schema.create_file(
                ch.id, ch.file_size, ch.leaf_hashes, stored, origin
            )
        schema.check_file(doc)
        self.db.save(doc)
        return {
            'file_id': ch.id,
            'file_path': fs.path(ch.id),
        }


SHARED = '/home'
PRIVATE = path.abspath(os.environ['HOME'])


class Core2:
    def __init__(self, env, private=None, shared=None, bootstrap=True):
        self.env = env
        self.db = get_db(env, init=True)
        self.stores = LocalStores()
        self._private = (PRIVATE if private is None else private)
        self._shared = (SHARED if shared is None else shared)
        if bootstrap:
            self._init_local()
            self._init_default_store()

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
        self.env['version'] = dmedia.__version__
        log.info('machine_id = %r', self.machine_id)

    def _init_default_store(self):
        default = self.local.get('default_store')
        if default not in ('private', 'shared'):
            if self.local.get('stores') != {}:
                self.local['stores'] = {}
                self.db.save(self.local)
            return
        parentdir = (self._private if default == 'private' else self._shared)
        (fs, doc) = init_filestore(parentdir)
        self._add_filestore(fs, doc)

    def _add_filestore(self, fs, doc):
        self.stores.add(fs)
        try:
            self.db.save(doc)
        except Conflict:
            pass
        stores = self.stores.local_stores()
        if self.local.get('stores') != stores:
            self.local['stores'] = stores
            self.db.save(self.local)

    def create_filestore(self, parentdir, label):
        """
        Create a new file-store in *parentdir*.
        """

    def connect_filestore(self, parentdir, store_id):
        """
        Put an existing file-store into the local storage pool.
        """

    def disconnect_filestore(self, parentdir, store_id):
        """
        Remove an existing file-store from the local storage pool.
        """

    def downgrade_store(self, store_id):
        """
        Mark all files in *store_id* as counting for zero copies.

        This method downgrades our confidence in a particular store.  Files are
        still tracked in this store, but they are all updated to have zero
        copies worth of durability in this store.  Some scenarios in which you
        might do this:

            1. It's been too long since a particular HDD has connected, so we
               play it safe and work from the assumption the HHD wont contain
               the expected files, or was run over by a bus.

            2. We're about to format a removable drive, so we first downgrade
               all the files it contains so addition copies can be created if
               needed, and so other nodes know not to count on these copies.

        Note that this method makes sense for remote cloud stores as well as for
        local file-stores.
        """

    def purge_store(self, store_id):
        """
        Purge all record of files in *store_id*.

        This method completely erases the record of a particular store, at least
        from the file persecutive.  This store will no longer count in the
        durability of any files, nor will Dmedia consider this store as a source
        for any files.

        Specifically, all of these will be deleted if they exist:

            * ``doc['stored'][store_id]``
            * ``doc['corrupt'][store_id]``
            * ``doc['partial'][store_id]``

        Some scenarios in which you might want to do this:
        
            1. The HDD was run over by a bus, the data is gone.  We need to
               embrace reality, the sooner the better.

            2. We're going to format or otherwise repurpose an HDD.  Ideally, we
               would have called `Core2.downgrade_store()` first.
 
        Note that this method makes sense for remote cloud stores as well as for
        local file-stores
        """
        
