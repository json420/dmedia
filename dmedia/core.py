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

from dbase32.rfc3548 import isb32
from microfiber import Server, Database, NotFound, Conflict, BulkConflict, id_slice_iter
from filestore import FileStore, check_root_hash, check_id, DOTNAME, FileNotFound

import dmedia
from dmedia.parallel import start_thread, start_process
from dmedia.server import run_server
from dmedia import util, schema, views
from dmedia.metastore import MetaStore, create_stored
from dmedia.local import LocalStores, FileNotLocal, LocalSlave


log = logging.getLogger()
LOCAL_ID = '_local/dmedia'

FLAG_RESOLVED = 0
FLAG_UNAVAILABLE = 1
FLAG_UNKNOWN = 2
FLAG_BAD_ID = 3


def start_httpd(couch_env, ssl_config):
    queue = multiprocessing.Queue()
    process = start_process(run_server, queue, couch_env, '0.0.0.0', ssl_config)
    env = queue.get()
    if isinstance(env, Exception):
        raise env
    log.info('Dmedia HTTPD: %s', env['url'])
    return (process, env)


def db_dump_iter(server):
    assert isinstance(server, Server)
    for name in server.get('_all_dbs'):
        if name.startswith('_') or name == 'thumbnails':
            continue
        yield name


def dump_all(server, dumpdir):
    log.info('Dumping __all__ into %r', dumpdir)
    start = time.time()
    files = []
    for name in db_dump_iter(server):
        log.info(name)
        db = server.database(name)
        filename = path.join(dumpdir, name + '.json')  
        db.dump(filename)
        check_call(['bzr', 'add', filename])
        files.append(filename)
    log.info('** %.3f to dump __all__', time.time() - start)
    msg = 'Snapshot __all__'
    cmd = ['bzr', 'commit', '-m', msg, '--unchanged']
    cmd.extend(files)
    check_call(cmd)
    log.info('Committed snapshot of __all__')


def dump_one(server, dumpdir, name):
    db = server.database(name)
    filename = path.join(dumpdir, name + '.json')  
    log.info('Dumping %r to %r', name, filename)
    start = time.time()
    db.dump(filename)
    log.info('** %.2f to dump %r', time.time() - start, name)
    check_call(['bzr', 'add', filename])
    msg = 'Snapshot of {!r}'.format(name)
    check_call(['bzr', 'commit', filename, '-m', msg, '--unchanged'])
    log.info('Committed snapshot of %r', name)


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
        name = queue_in.get()
        if name is None:
            queue_out.put(None)
            break
        try:
            if name == '__all__':
                dump_all(server, dumpdir)
            else:
                dump_one(server, dumpdir, name)
            queue_out.put((name, True))
        except Exception:
            log.exception('Error snapshotting %r', name)
            queue_out.put((name, False))


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


def update_stats(db, doc):
    rows = db.view('user', 'bytes', reduce=True)['rows']
    if not rows:
        return False
    stats = rows[0]['value']
    if doc.get('count') != stats['count'] or doc.get('bytes') != stats['sum']:
        doc['count'] = stats['count']
        doc['bytes'] = stats['sum']
        return True
    return False


def update_project_doc(db, doc):
    one = update_thumbnail(db, doc)
    two = update_stats(db, doc)
    updated = (one or two)
    if updated:
        db.save(doc)
    return updated


def update_project(db, project_id):
    try:
        pdb_name = schema.project_db_name(project_id)
        pdb = Database(pdb_name, ctx=db.ctx)
        pdoc = pdb.get(project_id, attachments=True)
        if update_project_doc(pdb, pdoc):
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


def vigilance(env, stores, first_run):
    try:
        log.info('vigilance() running %r', stores)
        db = util.get_db(env)
        ms = MetaStore(db)
        if first_run:
            ms.schema_check()
        filestores = []
        for (parentdir, info) in stores.items():
            fs = util.get_filestore(parentdir, info['id'], info['copies'])[0]
            log.info(fs)
            filestores.append(fs)
        for fs in filestores:
            ms.scan(fs)
        ms.downgrade_by_store_atime()
        for fs in filestores:
            ms.relink(fs)
        for fs in filestores:
            ms.verify_all(fs)
        ms.downgrade_by_never_verified()
        ms.downgrade_by_last_verified()
        increase_copies(env)
        decrease_copies(env)
        log.info('vigilance() is exiting...')
    except Exception:
        log.exception('Error in vigilance()')


def get_peers(db):
    try:
        return db.get('_local/peers')['peers']
    except NotFound:
        return {}


def increase_copies(env):
    db = util.get_db(env)
    slave = LocalSlave(db.env)
    slave.update_stores()
    connected = frozenset(slave.stores.ids)
    ms = MetaStore(db)
    total = 0
    new = 0
    for copies in range(3):
        rows = db.view('file', 'fragile', key=copies)['rows']
        for ids in id_slice_iter(rows):
            peers = get_peers(db)
            docs = db.get_many(ids)
            for doc in docs:
                stored = frozenset(doc['stored'])
                local = connected.intersection(stored)  # Any local copies?
                free = connected - stored  # Local drives without a copy?
                if local and free:
                    src = slave.stores.choose_local_store(doc)
                    size = src.stat(doc['_id']).size
                    dst = slave.stores.filter_by_avail(free, size, 3 - copies)
                    if dst:
                        ms.copy(src, doc, *dst)
                        total += 1
                        new += len(dst)
                elif free and peers:
                    log.info('Would try and download %s from %s', doc['_id'], sorted(peers))
    log.info('Created %s new copies of %s total files', new, total)


def decrease_copies(env):
    db = util.get_db(env)
    slave = LocalSlave(db.env)
    ms = MetaStore(db)
    slave.update_stores()
    for fs in slave.stores.sort_by_avail(reverse=False):
        total = 0
        while True:
            kw = {
                'startkey': [fs.id, None],
                'endkey': [fs.id, int(time.time())],
                'limit': 1,
            }
            rows = db.view('file', 'reclaimable', **kw)['rows']
            if not rows:
                break
            row = rows[0]
            _id = row['id']
            ms.remove(fs, _id)
            total += 1
        log.info('Deleted %s total copies in %s', total, fs.id)


class Core:
    def __init__(self, env):
        self.env = env
        self.db = util.get_db(env, init=True)
        self.server = self.db.server()
        self.ms = MetaStore(self.db)
        self.stores = LocalStores()
        self.vigilance = None
        self.vigilance_first_run = True
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

    def set_auto_format(self, value):
        if value not in ('true', 'false'):
            raise Exception('bad auto_format value: {!r}'.format(value))
        self.local['auto_format'] = json.loads(value)
        self.save_local()

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

    def load_default_filestore(self, parentdir):
        (fs, doc) = util.init_filestore(parentdir)
        log.info('Default FileStore %s at %r', doc['_id'], parentdir)
        self._add_filestore(fs, doc)
        return fs

    def _sync_stores(self):
        self.local['stores'] = self.stores.local_stores()
        self.save_local()
        if not self.vigilance_first_run:
            self.restart_vigilance()

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

    def _remove_filestore(self, fs):
        self.stores.remove(fs)
        self._sync_stores()

    def start_vigilance(self):
        assert self.vigilance is None
        stores = self.stores.local_stores()
        first_run = self.vigilance_first_run
        if first_run:
            self.vigilance_first_run = False
        self.vigilance = start_process(vigilance, self.env, stores, first_run)

    def stop_vigilance(self):
        if self.vigilance is not None:
            self.vigilance.terminate()
            self.vigilance.join()
            self.vigilance = None

    def restart_vigilance(self):
        self.stop_vigilance()
        self.start_vigilance()

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
                update_project_doc(pdb, pdoc)
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

    def update_project(self, project_id):
        update_project(self.db, project_id)     

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
        return self.ms.downgrade_store(store_id)

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
        return self.ms.purge_store(store_id)

    def stat(self, _id):
        doc = self.db.get(_id)
        fs = self.stores.choose_local_store(doc)
        return fs.stat(_id)

    def stat2(self, doc):
        fs = self.stores.choose_local_store(doc)
        return fs.stat(doc['_id'])

    def resolve(self, _id):
        """
        Resolve a Dmedia file ID into a regular file path.

        The return value is an ``(_id, status, filename)`` tuple.

        The ``status`` is an ``int`` with one of 4 values:

            0 - the ID was resolved successfully
            1 - the file is not available locally
            2 - the file is unknown (no doc in CouchDB)
            3 - the ID is malformed

        When the ``status`` is anything other than zero, ``filename`` will be an
        empty string.
        """
        if not (isb32(_id) and len(_id) == 48):
            return (_id, 3, '')
        try:
            doc = self.db.get(_id)
        except NotFound:
            return (_id, 2, '')
        try:
            fs = self.stores.choose_local_store(doc)
            st = fs.stat(_id)
        except (FileNotLocal, FileNotFound):
            return (_id, 1, '')
        # It's all good:
        return (_id, 0, st.name)

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
            raise Exception('no file-stores present')
        tmp_fp = stores[0].allocate_tmp()
        tmp_fp.close()
        return tmp_fp.name

    def hash_and_move(self, tmp, origin):
        parentdir = path.dirname(path.dirname(path.dirname(tmp)))
        fs = self.stores.by_parentdir(parentdir)
        tmp_fp = open(tmp, 'rb')
        ch = fs.hash_and_move(tmp_fp)
        stored = create_stored(ch.id, fs)
        try:
            doc = self.db.get(ch.id)
            doc['stored'].update(stored)
        except NotFound:
            doc = schema.create_file(time.time(), ch, stored, origin)
        schema.check_file(doc)
        self.db.save(doc)
        return {
            'file_id': ch.id,
            'file_path': fs.path(ch.id),
        }

