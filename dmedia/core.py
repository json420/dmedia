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
from collections import namedtuple, OrderedDict

from dbase32 import isdb32
from microfiber import Server, Database, NotFound, Conflict, BulkConflict, id_slice_iter
from filestore import FileStore, check_root_hash, check_id, DOTNAME, FileNotFound
from gi.repository import GLib

import dmedia
from dmedia.parallel import start_thread, start_process
from dmedia.server import run_server
from dmedia import util, schema, views
from dmedia.client import Downloader, get_client, build_ssl_context
from dmedia.metastore import MetaStore, create_stored, get_dict
from dmedia.local import LocalStores, FileNotLocal, MIN_FREE_SPACE


log = logging.getLogger()
LOCAL_ID = '_local/dmedia'


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


def vigilance_worker(env, key, ssl_config):
    """
    Run the event-based copy-increasing loop to maintain file durability.
    """
    assert key == '__vigilance__'
    db = util.get_db(env)
    ms = MetaStore(db)
    ssl_context = build_ssl_context(ssl_config)
    local_stores = ms.get_local_stores()
    if len(local_stores) == 0:
        log.warning('No connected local stores, cannot increase copies')
        return
    connected = frozenset(local_stores.ids)
    log.info('Connected %r', connected)

    for (doc, stored) in ms.iter_actionable_fragile(connected, True):
        _id = doc['_id']
        copies = sum(v['copies'] for v in doc['stored'].values())
        if copies >= 3:
            log.warning('%s already has copies >= 3, skipping', _id)
            continue
        size = doc['bytes']
        local = connected.intersection(stored)  # Any local copies?
        if local:
            free = connected - stored
            src = local_stores.choose_local_store(doc)
            dst = local_stores.filter_by_avail(free, size, 3 - copies)
            if dst:
                ms.copy(src, doc, *dst)
        elif ms.get_peers():
            peers = ms._peers
            for (machine_id, info) in peers.items():
                url = info['url']
                client = get_client(url, ssl_context)
                if not client.has_file(_id):
                    continue
                fs = local_stores.find_dst_store(size)
                if fs is None:
                    log.warning(
                        'No FileStore with avail space to download %s', _id
                    )
                    continue
                downloader = Downloader(doc, ms, fs)
                try:
                    downloader.download_from(client)
                except Exception:
                    log.exception('Error downloading %s from %s', _id, url)


def downgrade_worker(env, key):
    assert key == '__downgrade__'
    db = util.get_db(env)
    ms = MetaStore(db)
    ms.downgrade_by_store_atime()
    ms.downgrade_by_never_verified()
    ms.downgrade_by_last_verified()


def check_filestore_worker(env, parentdir, store_id):
    db = util.get_db(env)
    ms = MetaStore(db)
    fs = FileStore(parentdir, store_id)
    ms.scan(fs)
    ms.relink(fs)
    ms.verify_all(fs)


def scan_relink_worker(env, parentdir, store_id):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        fs = FileStore(parentdir, store_id)
        ms.scan(fs)
        ms.relink(fs)
    except Exception:
        log.exception('Error in scan_relink_worker():')


def _worker(env, parentdir, store_id):
    db = util.get_db(env)
    ms = MetaStore(db)
    fs = FileStore(parentdir, store_id)
    ms.scan(fs)
    ms.relink(fs)
    ms.verify_all(fs)


def is_file_id(_id):
    return isdb32(_id) and len(_id) == 48


def clean_file_id(_id):
    if is_file_id(_id):
        return _id
    return None


def background_worker(q, env, target, key, *extra):
    try:
        log.info('%s: %r', target.__name__, key)
        target(env, key, *extra)
    except Exception:
        log.exception('Error: %s: %r', target.__name__, key)
    finally:
        q.put(key)


class BackgroundManager:
    def __init__(self, env, ssl_config):
        self.env = env
        self.ssl_config = ssl_config
        self.workers = {}
        self.q = None
        self.thread = None
        self.__active = False
        
    @property
    def active(self):
        return self.__active

    def activate(self):
        assert self.__active is False
        self.__active = True
        self.start_listener()

    def start_listener(self):
        assert self.workers == {}
        assert self.q is None
        assert self.thread is None
        log.info('Starting listener thread...')
        self.q = multiprocessing.Queue()
        self.thread = start_thread(self.listener)

    def stop_listener(self):
        log.info('Stopping listener thread...')
        self.q.put(None)
        self.thread.join()
        self.thread = None
        self.q = None

    def listener(self):
        while True:
            key = self.q.get()
            if key is None:
                break
            GLib.idle_add(self.on_complete, key)

    def check_initial_filestores(self, key):
        try:
            self.initial_filestores.remove(key)
            log.info('initial_filestores: %r', sorted(self.initial_filestores))
            if len(self.initial_filestores) == 0:
                self.start_vigilance()   
        except KeyError:
            pass 

    def on_complete(self, key):
        log.info('Complete: %r', key)
        if key not in self.workers:
            log.warning('Could not find process for %r', key)
            return
        process = self.workers.pop(key)
        process.join()
        self.check_initial_filestores(key)

    def start(self, target, key, *extra):
        assert self.active
        if key in self.workers:
            log.warning('%s is already running', key)
            return False
        self.workers[key] = start_process(
            background_worker, self.q, self.env, target, key, *extra
        )
        return True

    def stop(self, key):
        if key not in self.workers:
            return False
        log.info('Stopping %r', key)
        process = self.workers.pop(key)
        process.terminate()
        process.join()
        self.check_initial_filestores(key)
        return True

    def restart(self, target, key, *extra):
        self.stop(key)
        return self.start(target, key, *extra)

    def check_filestore(self, fs):
        if self.active:
            self.start(check_filestore_worker, fs.parentdir, fs.id)

    def start_vigilance(self):
        self.start(vigilance_worker, '__vigilance__', self.ssl_config)
        self.start(downgrade_worker, '__downgrade__')

    def restart_vigilance(self):
        if self.active and self.stop('__vigilance__'):
            log.info('Restarting vigilance...')
            self.start(vigilance_worker, '__vigilance__', self.ssl_config)

    def start_all(self, filestores):
        self.activate()
        self.initial_filestores = set(fs.parentdir for fs in filestores)
        for fs in filestores:
            self.check_filestore(fs)

    def restart_all(self, filestores):
        assert self.active
        self.stop_listener()
        for process in self.workers.values():
            process.terminate()
            process.join()
        self.workers.clear()
        self.start_listener()
        self.initial_filestores = set(fs.id for fs in filestores)
        for fs in filestores:
            self.check_filestore(fs)


Task = namedtuple('Task', 'process thread')


class Background:
    __slots__ = ('running', 'task', 'pending')

    def __init__(self):
        self.running = False
        self.task = None
        self.pending = OrderedDict()

    def joiner(self):
        task = self.task
        task.process.join()
        GLib.idle_add(self.on_join, task)

    def on_join(self, task):
        assert task is self.task
        assert not task.process.is_alive()
        task.thread.join()
        self.task = None
        self.start_next()

    def start_next(self):
        if not self.running:
            return False
        if self.task is not None:
            return False
        if not self.pending:
            return False
        (key, value) = self.popitem()
        (target, *args) = value
        process = start_process(target, *args)
        self.task = Task(process, create_thread(self.joiner))
        self.task.thread.start()
        return True

    def start(self):
        assert self.running is False
        self.running = True
        self.start_next()

    def append(self, key, target, *args):
        value = (target,) + args
        self.pending[key] = value

    def popitem(self):
        key = list(self.pending)[0]
        return (key, self.pending.pop(key))


class Core:
    def __init__(self, env, ssl_config=None):
        self.env = env
        self.ssl_config = ssl_config
        self.db = util.get_db(env, init=True)
        self.log_db = self.db.database(schema.LOG_DB_NAME)
        self.log_db.ensure()
        self.server = self.db.server()
        self.ms = MetaStore(self.db)
        self.stores = LocalStores()
        self.background = BackgroundManager(env, ssl_config)
        try:
            self.local = self.db.get(LOCAL_ID)
        except NotFound:
            self.local = {
                '_id': LOCAL_ID,
                'stores': {},
            }
        self.__local = deepcopy(self.local)

    def start_background_tasks(self):
        self.background.start_all(tuple(self.stores))

    def restart_background_tasks(self):
        self.background.restart_all(tuple(self.stores))

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
        if util.isfilestore(parentdir):
            fs = util.migrate_if_needed(parentdir)
        else:
            fs = FileStore.create(parentdir)
        log.info('Default: %r', fs)
        self._add_filestore(fs)
        return fs

    def _sync_stores(self):
        self.local['stores'] = self.stores.local_stores()
        self.save_local()
        self.background.restart_vigilance()

    def _add_filestore(self, fs):
        log.info('Adding %r', fs)
        fs.check_layout()
        assert isdb32(fs.id)
        self.stores.add(fs)
        try:
            fs.purge_tmp()
        except Exception:
            log.exception('Error calling FileStore.purge_tmp():')
        try:
            self.db.save(fs.doc)
        except Conflict:
            pass
        self.background.check_filestore(fs)
        self._sync_stores()

    def _remove_filestore(self, fs):
        log.info('Removing %r', fs)
        self.stores.remove(fs)
        self.background.stop(fs.parentdir)
        self._sync_stores()

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
        fs = FileStore.create(parentdir)
        self._add_filestore(fs)
        return fs

    def connect_filestore(self, parentdir, expected_id=None):
        """
        Add an existing file-store into the local storage pool.
        """
        fs = util.migrate_if_needed(parentdir, expected_id)
        self._add_filestore(fs)
        return fs

    def disconnect_filestore(self, parentdir):
        """
        Remove an existing file-store from the local storage pool.
        """
        fs = self.stores.by_parentdir(parentdir)
        self._remove_filestore(fs)
        return fs

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
        if not is_file_id(_id):
            return (_id, 3, '')
        try:
            doc = self.db.get(_id)
        except NotFound:
            return (_id, 2, '')
        try:
            fs = self.stores.choose_local_store(doc)
            st = fs.stat(_id)
            return (_id, 0, st.name)
        except (FileNotLocal, FileNotFound):
            return (_id, 1, '')

    def _resolve_many_iter(self, ids):
        # Yes, we call is_file_id() twice on each ID, but the point is to make
        # only a single request to CouchDB, which is the real performance
        # bottleneck.
        clean_ids = list(map(clean_file_id, ids))
        docs = self.db.get_many(clean_ids)
        for (_id, doc) in zip(ids, docs):
            if not is_file_id(_id):
                yield (_id, 3, '')
            elif doc is None:
                yield (_id, 2, '')
            else:
                try:
                    fs = self.stores.choose_local_store(doc)
                    st = fs.stat(_id)
                    yield (_id, 0, st.name)
                except (FileNotLocal, FileNotFound):
                    yield (_id, 1, '')

    def resolve_many(self, ids):
        return list(self._resolve_many_iter(ids))

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

    def reclaim_if_possible(self):
        start_thread(self.ms.reclaim_all)
        return True
