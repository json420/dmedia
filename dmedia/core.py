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

import logging
import os
from os import path
import json
import time
import stat
import multiprocessing
from urllib.parse import urlparse
from queue import Queue
from subprocess import check_call, CalledProcessError
from copy import deepcopy
from base64 import b64encode

from microfiber import Server, Database, NotFound, Conflict, BulkConflict
from filestore import FileStore, check_root_hash, check_id

import dmedia
from dmedia.parallel import start_thread, start_process
from dmedia.server import run_server
from dmedia import util, schema, views
from dmedia.metastore import MetaStore
from dmedia.local import LocalStores, FileNotLocal


log = logging.getLogger()
LOCAL_ID = '_local/dmedia'
SHARED = '/home'
PRIVATE = path.abspath(os.environ['HOME'])


def start_httpd(couch_env, ssl_config):
    queue = multiprocessing.Queue()
    process = start_process(run_server, queue, couch_env, '0.0.0.0', ssl_config)
    env = queue.get()
    if isinstance(env, Exception):
        raise env
    log.info('Dmedia HTTPD: %s', env['url'])
    return (process, env)


def snapshot_worker(env, dumpdir, queue_in, queue_out):
    server = Server(env)
    try:
        check_call(['bzr', 'init', dumpdir])
        whoami = env['machine_id']
        check_call(['bzr', 'whoami', '--branch', '-d', dumpdir, whoami])
        log.info('Initialized bzr branch in %r', dumpdir)
    except CalledProcessError:
        pass
    while True:
        dbname = queue_in.get()
        if dbname is None:
            queue_out.put(None)
            break
        try:
            db = server.database(dbname)
            filename = path.join(dumpdir, dbname + '.json')  
            log.info('Dumping %r to %r', dbname, filename)
            start = time.time()
            db.dump(filename)
            log.info('** %.2f to dump %r', time.time() - start, dbname)
            check_call(['bzr', 'add', filename])
            msg = 'Snapshot of {!r}'.format(dbname)
            check_call(['bzr', 'commit', filename, '-m', msg, '--unchanged'])
            log.info('Committed snapshot of %r', dbname)
            queue_out.put((dbname, True))
        except Exception:
            log.exception('Error snapshotting %r', dbname)
            queue_out.put((dbname, False))


def projects_iter(server):
    assert isinstance(server, Server)
    for name in server.get('_all_dbs'):
        if name.startswith('_'):
            continue
        _id = schema.get_project_id(name)
        if _id is None:
            continue
        yield (name, _id)


def get_first_thumbnail(db):
    rows = db.view('user', 'thumbnail', limit=1)['rows']
    if rows:
        _id = rows[0]['id']
        return db.get_att(_id, 'thumbnail')


def has_thumbnail(doc):
    try:
        doc['_attachments']['thumbnail']
        return True
    except KeyError:
        return False


def encode_attachment(content_type, data):
    return {
        'content_type': content_type,
        'data': b64encode(data).decode('utf-8'),        
    }


def update_thumbnail(db, doc):
    if has_thumbnail(doc):
        return False
    att = get_first_thumbnail(db)
    if att is None:
        return False
    (content_type, data) = att
    if '_attachments' not in doc:
        doc['_attachments'] = {}
    doc['_attachments']['thumbnail'] = encode_attachment(content_type, data)
    return True


def update_count_and_bytes(db, doc):
    updated = False
    stats = db.view('user', 'bytes', reduce=True)['rows'][0]['value']
    if doc.get('count') != stats['count'] or doc.get('bytes') != stats['sum']:
        doc['count'] = stats['count']
        doc['bytes'] = stats['sum']
        updated = True
    if update_thumbnail(db, doc):
        updated = True
    if updated:
        db.save(doc)
    return updated


def update_project_stats(db, project_id):
    try:
        pdb_name = schema.project_db_name(project_id)
        pdb = Database(pdb_name, ctx=db.ctx)
        pdoc = pdb.get(project_id, attachments=True)
        if update_count_and_bytes(pdb, pdoc):
            try:
                doc = db.get(project_id)
                pdoc['_rev'] = doc['_rev']
                db.save(pdoc)
            except NotFound:
                del pdoc['_rev']
                db.save(pdoc)
            db.view('project', 'atime', limit=1)
            log.info('Updated project stats for %r', project_id)
    except Exception:
        log.exception('Error updating project stats for %r', project_id)


class Core:
    def __init__(self, env, private=None, shared=None):
        self.env = env
        self._private = (PRIVATE if private is None else private)
        self._shared = (SHARED if shared is None else shared)
        self.db = util.get_db(env, init=True)
        self.server = self.db.server()
        self.ms = MetaStore(self.db)
        self.stores = LocalStores()
        self.queue = Queue()
        self.thread = None
        self.default = None
        try:
            self.local = self.db.get(LOCAL_ID)
        except NotFound:
            self.local = {
                '_id': LOCAL_ID,
                'stores': {},
            }
        self.__local = deepcopy(self.local)

    def save_local(self):
        if self.local != self.__local:
            self.db.save(self.local)
            self.__local = deepcopy(self.local)

    def load_identity(self, machine, user):
        try:
            self.db.save_many([machine, user])
        except BulkConflict:
            pass
        log.info('machine_id = %s', machine['_id'])
        log.info('user_id = %s', user['_id'])
        self.env['machine_id'] = machine['_id']
        self.env['user_id'] = user['_id']
        self.local['machine_id'] = machine['_id']
        self.local['user_id'] = user['_id']
        self.save_local()

    def init_default_store(self):
        value = self.local.get('default_store')
        if value not in ('private', 'shared'):
            log.info('no default FileStore')
            self.default = None
            self._sync_stores()
            return
        if value == 'shared' and not util.isfilestore(self._shared):
            log.warning('Switching to private, no shared FileStore at %r', self._shared)
            value = 'private'
            self.local['default_store'] = value
            self.db.save(self.local)
        parentdir = (self._private if value == 'private' else self._shared)
        (fs, doc) = util.init_filestore(parentdir)
        self.default = fs
        log.info('Connecting default FileStore %r at %r', fs.id, fs.parentdir)
        self._add_filestore(fs, doc)

    def _sync_stores(self):
        self.local['stores'] = self.stores.local_stores()
        self.save_local()

    def _add_filestore(self, fs, doc):
        self.stores.add(fs)
        try:
            fs.purge_tmp()
        except Exception:
            log.exception('Error calling FileStore.purge_tmp():')
        try:
            self.db.save(doc)
        except Conflict:
            pass
        self._sync_stores()
        self.queue.put(fs)

    def _remove_filestore(self, fs):
        self.stores.remove(fs)
        self._sync_stores()

    def _background_worker(self):
        log.info('Background worker listing to queue...')
        while True:
            try:
                fs = self.queue.get()
                start = time.time()
                self.ms.scan(fs)
                self.ms.relink(fs)
                log.info('%.3f to check %r', time.time() - start, fs)
            except Exception as e:
                log.exception('Error in background worker:')

    def start_background_tasks(self):
        assert self.thread is None
        self.thread = start_thread(self._background_worker)

    def _iter_project_dbs(self):
        for (name, _id) in projects_iter(self.server):
            pdb = self.server.database(name)
            util.init_views(pdb, views.project)
            yield (pdb, _id)

    def init_project_views(self):
        start = time.time()
        try:
            items = tuple(self._iter_project_dbs())
            log.info('%.3f to init project views', time.time() - start)

            s = time.time()
            self.db.view('project', 'atime', limit=1)
            log.info('%.3f to prep project/atime view', time.time() - s)

            s = time.time()
            for (pdb, _id) in items:
                try:
                    pdoc = pdb.get(_id, attachments=True)
                except NotFound:
                    log.error('Project doc %r not in %r', _id, pdb.name)
                    continue
                update_count_and_bytes(pdb, pdoc)
                del pdoc['_rev']
                try:
                    doc = self.db.get(_id, attachments=True)
                    rev = doc.pop('_rev')
                    if doc != pdoc:
                        log.info('updating project stats for %s', _id)
                        pdoc['_rev'] = rev
                        self.db.save(pdoc)
                except NotFound:
                    log.info('missing project doc for %s', _id)
                    self.db.save(pdoc)
            log.info('%.3f to update project stats', time.time() - s)

            log.info('%.3f total for Core.init_project_views()', time.time() - start)
        except Exception:
            log.exception('Error in Core.init_project_views():')

    def update_project_stats(self, project_id):
        start_thread(update_project_stats, self.db, project_id)     

    def set_default_store(self, value):
        if value not in ('private', 'shared', 'none'):
            raise ValueError(
                "need 'private', 'shared', or 'none'; got {!r}".format(value)
            )
        if self.local.get('default_store') != value:
            self.local['default_store'] = value
            self.db.save(self.local)
        if self.default is not None:
            self.disconnect_filestore(self.default.parentdir, self.default.id)
            self.default = None
        self.init_default_store()

    def create_filestore(self, parentdir):
        """
        Create a new file-store in *parentdir*.
        """
        if util.isfilestore(parentdir):
            raise Exception(
                'Already contains a FileStore: {!r}'.format(parentdir)
            )
        log.info('Creating a new FileStore in %r', parentdir)
        (fs, doc) = util.init_filestore(parentdir)
        if path.ismount(parentdir) and (parentdir.startswith('/media/') or parentdir.startswith('/run/media/')):
            try:
                os.chmod(parentdir, 0o777)
            except Exception as e:
                pass
        return self.connect_filestore(parentdir, fs.id)

    def connect_filestore(self, parentdir, store_id):
        """
        Put an existing file-store into the local storage pool.
        """
        log.info('Connecting FileStore %r at %r', store_id, parentdir)
        (fs, doc) = util.get_filestore(parentdir, store_id)
        self._add_filestore(fs, doc)
        return fs

    def disconnect_filestore(self, parentdir, store_id):
        """
        Remove an existing file-store from the local storage pool.
        """
        log.info('Disconnecting FileStore %r at %r', store_id, parentdir)
        fs = self.stores.by_parentdir(parentdir)
        self._remove_filestore(fs)
        return fs

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
 
    def stat(self, _id):
        doc = self.db.get(_id)
        fs = self.stores.choose_local_store(doc)
        return fs.stat(_id)

    def stat2(self, doc):
        fs = self.stores.choose_local_store(doc)
        return fs.stat(doc['_id'])

    def resolve(self, _id):
        doc = self.db.get(_id)
        fs = self.stores.choose_local_store(doc)
        return fs.stat(_id).name

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

    def allocate_tmp(self):
        stores = self.stores.sort_by_avail()
        if len(stores) == 0:
            raise Exception('no filestores present')
        tmp_fp = stores[0].allocate_tmp()
        tmp_fp.close()
        return tmp_fp.name

    def hash_and_move(self, tmp, origin):
        parentdir = path.dirname(path.dirname(path.dirname(tmp)))
        fs = self.stores.by_parentdir(parentdir)
        tmp_fp = open(tmp, 'rb')
        ch = fs.hash_and_move(tmp_fp)
        stored = {
            fs.id: {
                'copies': fs.copies,
                'mtime': fs.stat(ch.id).mtime,
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
        
