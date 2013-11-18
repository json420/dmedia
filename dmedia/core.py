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
from dmedia.parallel import create_thread, start_thread, start_process
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


NO_DUMP = (
    'thumbnails',
    'thumbnails-1',
    'migrate-0-to-1',
)

def db_dump_iter(server):
    assert isinstance(server, Server)
    for name in server.get('_all_dbs'):
        if name == 'thumbnails':
            log.info('Deleting old thumbails DB...')
            server.delete(name)  # Replaced with thumbnails-1
        if name.startswith('_') or name in NO_DUMP:
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


def get_downgraded(doc):
    downgraded = []
    for (key, value) in doc['stored'].items():
        copies = value['copies']
        verified = value.get('verified')
        if copies == 0 and not isinstance(verified, int):
            downgraded.append(key)
    return downgraded


class Vigilance:
    def __init__(self, ms, ssl_config):
        self.ms = ms
        self.stores = ms.get_local_stores()
        for fs in self.stores:
            log.info('Vigilance: local store: %r', fs)
        self.local = frozenset(self.stores.ids)

        self.clients = {}
        self.store_to_client = {}
        remote = []
        peers = ms.get_local_peers()
        if peers:
            ssl_context = build_ssl_context(ssl_config)

            for (peer_id, info) in peers.items():
                url = info['url']
                log.info('Vigilance: peer %s at %s', peer_id, url)
                self.clients[peer_id] = get_client(url, ssl_context)

            for doc in ms.db.get_many(list(peers)):
                if doc is not None:
                    client = self.clients[doc['_id']]
                    for store_id in get_dict(doc, 'stores'):
                        if is_store_id(store_id):
                            remote.append(store_id)
                            self.store_to_client[store_id] = client

        self.remote = frozenset(remote)

    def run(self):
        log.info('Processing backlog of fragile files...')
        for doc in self.ms.iter_fragile_files():
            self.up_rank(doc)
        last_seq = self.db.get()['update_seq']
        log.info('Done processing backlog as of update_seq %r', last_seq)

    def up_rank(self, doc):
        """
        Implements the rank-increasing decision tree.

        There are 4 possible actions:

            1) Verify a local copy currently in a downgraded state

            2) Copy from a local FileStore to another local FileStore

            3) Download from a remote peer to a local FileStore

            4) Do nothing as no rank-increasing action is possible

        This is a high-level tree based on set operations.  The action taken
        here may not actually be possible because this method doesn't consider
        whether there is a local FileStore with enough available space, which
        when there isn't, actions (2) and (3) wont be possible.

        We use a simple IO cost accounting model: reading a copy costs one unit,
        and writing copy likewise costs one unit.  For example, consider these
        three operations:

            ====  ==============================================
            Cost  Action
            ====  ==============================================
            1     Verify a copy (1 read unit)
            2     Create a copy (1 read unit, 1 write unit)
            3     Create two copies (1 read unit, 2 write units)
            ====  ==============================================

        This method will take the least expensive route (in IO cost units) that
        will increase the file rank by at least 1.

        It's tempting to look for actions with a lower cost to benefit ratio,
        even when the cost is higher.  For example, consider these actions:

            ====  =====  =====  ========================
            Cost  +Rank  Ratio  Action
            ====  =====  =====  ========================
            1     1      1.00   Verify a downgraded copy
            2     2      1.00   Create a copy
            3     4      0.75   Create two copies
            ====  =====  =====  ========================

        In this sense, it's a better deal to create two new copies (which is the
        action Dmedia formerly would take).  However, because greater IO
        resources are consumed, this means it will necessarily delay acting on
        other equally fragile files (other files at the current rank being
        processed).

        Dmedia will now take the cheapest route to getting all files at rank=1
        up to at least rank=2, then getting all files at rank=2 up to at least
        rank=3, and so on.

        Another interesting "good deal" is creating new copies by reading from
        a local downgraded copy (because the source file is always verified as
        its read):

            ====  =====  =====  ========================================
            Cost  +Rank  Ratio  Action
            ====  =====  =====  ========================================
            1     1      1.00   Verify a downgraded copy
            2     2      1.00   Create a copy
            2     3      0.66   Create a copy from a downgraded copy
            3     4      0.75   Create two copies
            3     5      0.60   Create two copies from a downgraded copy
            ====  =====  =====  ========================================

        One place where this does make sense is when there is a locally
        available file at rank=1 (a single physical copy in a downgraded state),
        and a locally connected FileStore with enough free space to create a new
        copy.  As a state of having only a single physical copy is so dangerous,
        it makes sense to bend the rules here.

        However, the same will not be done for a file at rank=3 (two physical
        copies, one in a downgraded state).  In this case the downgraded copy
        will simply be verified, using 1 IO unit and increasing the rank to 4.

        Note that we use the same cost for a read whether reading from a local
        drive or downloading from a peer.  Although downloading is cheaper when
        looked at only from the perspective of the local node, it has the same
        cost when considering the total Dmedia library.

        The other peers will likewise be doing their best to address any fragile
        files.  And furthermore, local network IO is generally a more scarce
        resource (especially over WiFi), so we should only download when its
        absolutely needed (ie, when no local copy is available).        
        """
        stored = set(doc['stored'])
        local = stored.intersection(self.local)
        downgraded = local.intersection(get_downgraded(doc))
        free = self.local - stored
        remote = stored.intersection(self.remote)
        if local:
            if downgraded:
                return self.up_rank_by_verifying(doc, downgraded)
            elif free:
                return self.up_rank_by_copying(doc, free)
        elif remote:
            return self.up_rank_by_downloading(doc, remote)

    def up_rank_by_verifying(self, doc, downgraded):
        assert isinstance(downgraded, set)
        store_id = downgraded.pop()
        fs = self.stores.by_id(store_id)
        return self.ms.verify(fs, doc)

    def up_rank_by_copying(self, doc, free):
        dst = self.stores.filter_by_avail(free, doc['bytes'], 1)
        if dst:
            src = self.stores.choose_local_store(doc)
            return self.ms.copy(src, doc, *dst)

    def up_rank_by_downloading(self, doc, remote):
        fs = self.stores.find_dst_store(doc['bytes'])
        if fs is None:
            log.warning(
                'No FileStore with avail space to download %s', doc.get('_id')
            )
            return
        downloader = None
        _id = doc['_id']
        for store_id in remote:
            client = self.store_to_client[store_id]
            if not client.has_file(_id):
                continue
            if downloader is None:
                downloader = Downloader(doc, self.ms, fs)
            try:
                downloader.download_from(client)
            except Exception:
                log.exception('Error downloading %s from %s', _id, client)


def _vigilance_worker(env, ssl_config):
    """
    Run the event-based copy-increasing loop to maintain file durability.
    """
    db = util.get_db(env)
    ms = MetaStore(db)

    local_stores = ms.get_local_stores()
    if len(local_stores) == 0:
        log.warning('No connected local stores, cannot increase copies')
        return
    connected = frozenset(local_stores.ids)
    log.info('Connected %r', connected)

    clients = []
    peers = ms.get_local_peers()
    if peers:
        ssl_context = build_ssl_context(ssl_config)
        for (peer_id, info) in peers.items():
            url = info['url']
            log.info('Peer %s at %s', peer_id, url)
            clients.append(get_client(url, ssl_context))
    else:
        log.info('No known peers on local network')

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
        elif clients:
            fs = local_stores.find_dst_store(size)
            if fs is None:
                log.warning(
                    'No FileStore with avail space to download %s', _id
                )
                continue
            for client in clients:
                if not client.has_file(_id):
                    continue
                downloader = Downloader(doc, ms, fs)
                try:
                    downloader.download_from(client)
                except Exception:
                    log.exception('Error downloading %s from %s', _id, client)


def vigilance_worker(env, ssl_config):
    try:
        _vigilance_worker(env, ssl_config)
    except Exception:
        log.exception('Error in vigilance_worker():')


def vigilance_worker(env, ssl_config):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        vigilance = Vigilance(ms, ssl_config)
        vigilance.run()
    except Exception:
        log.exception('Error in vigilance_worker():')


def downgrade_worker(env):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        curtime = int(time.time())
        log.info('downgrading/purging as of timestamp %d', curtime)
        ms.purge_or_downgrade_by_store_atime(curtime)
        ms.downgrade_by_mtime(curtime)
        ms.downgrade_by_verified(curtime)
    except Exception:
        log.exception('Error in downgrade_worker():')


def scan_relink_worker(env, parentdir, store_id):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        fs = FileStore(parentdir, store_id)
        ms.scan(fs)
        ms.relink(fs)
    except Exception:
        log.exception('Error in scan_relink_worker():')


def verify_worker(env, parentdir, store_id):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        fs = FileStore(parentdir, store_id)
        ms.verify_all(fs)
    except Exception:
        log.exception('Error in verify_worker():')


def is_file_id(_id):
    return isinstance(_id, str) and len(_id) == 48 and isdb32(_id)


def is_store_id(_id):
    return isinstance(_id, str) and len(_id) == 24 and isdb32(_id)


def clean_file_id(_id):
    if is_file_id(_id):
        return _id
    return None


Task = namedtuple('Task', 'key process thread')


class TaskQueue:
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
        log.info('Finished: %r', task.key)
        self.start_next()

    def start_next(self):
        if not self.running:
            return False
        if self.task is not None:
            return False
        if not self.pending:
            return False
        (key, value) = self.popitem()
        log.info('Starting: %r', key)
        (target, *args) = value
        process = start_process(target, *args)
        thread = create_thread(self.joiner)
        self.task = Task(key, process, thread)
        self.task.thread.start()
        return True

    def start(self):
        assert self.running is False
        self.running = True
        self.start_next()

    def append(self, key, target, *args):
        value = (target,) + args
        self.pending[key] = value
        return self.start_next()

    def popitem(self):
        key = list(self.pending)[0]
        return (key, self.pending.pop(key))

    def pop(self, key):
        if self.task is not None and self.task.key == key:
            log.info('Terminating %r', key)
            self.task.process.terminate()
        else:
            self.pending.pop(key, None)


class TaskManager:
    def __init__(self, env, ssl_config):
        self.env = env
        self.ssl_config = ssl_config
        self.queue1 = TaskQueue()
        self.queue2 = TaskQueue()
        self.vigilance = None

    def queue_filestore_tasks(self, fs):
        args = (self.env, fs.parentdir, fs.id)
        key = ('scan_relink', fs.parentdir)
        self.queue1.append(key, scan_relink_worker, *args)
        key = ('verify', fs.parentdir)
        self.queue2.append(key, verify_worker, *args)

    def stop_filestore_tasks(self, fs): 
        self.queue1.pop(('scan_relink', fs.parentdir))
        self.queue2.pop(('verify', fs.parentdir))

    def requeue_filestore_tasks(self, filestores):
        for fs in filestores:
            self.queue_filestore_tasks(fs)
        self.queue1.append('downgrade', downgrade_worker, self.env)

    def start_tasks(self):
        self.queue1.append('downgrade', downgrade_worker, self.env)
        self.queue1.start()
        self.queue2.start()
        self.start_vigilance()

    def start_vigilance(self):
        if self.vigilance is not None:
            return False
        log.info('Starting vigilance_worker()...')
        self.vigilance = start_process(vigilance_worker, self.env, self.ssl_config)
        return True

    def stop_vigilance(self):
        if self.vigilance is None:
            return False
        log.info('Terminating vigilance_worker()...')
        self.vigilance.terminate()
        self.vigilance.join()
        self.vigilance = None
        return True

    def restart_vigilance(self):
        if self.stop_vigilance():
            self.start_vigilance()


def mark_machine_start(doc, atime):
    doc['atime'] = atime
    doc['stores'] = {}
    doc['peers'] = {}


def mark_add_filestore(doc, atime, fs_id, info):
    assert isinstance(info, dict)
    doc['atime'] = atime
    stores = get_dict(doc, 'stores')
    stores[fs_id] = info


def mark_remove_filestore(doc, atime, fs_id):
    doc['atime'] = atime
    stores = get_dict(doc, 'stores')
    stores.pop(fs_id, None)


def mark_connected_stores(doc, atime, stores):
    assert isinstance(stores, dict)
    doc['atime'] = atime
    doc['stores'] = stores

def mark_add_peer(doc, atime, peer_id, info):
    assert isinstance(info, dict)
    doc['atime'] = atime
    peers = get_dict(doc, 'peers')
    peers[peer_id] = info


def mark_remove_peer(doc, atime, peer_id):
    doc['atime'] = atime
    peers = get_dict(doc, 'peers')
    peers.pop(peer_id, None)


class Core:
    def __init__(self, env, machine, user, ssl_config=None):
        env.update({
            'machine_id': machine['_id'],
            'user_id': user['_id'],
        })
        self.env = env
        self.db = util.get_db(env, init=True)
        self.log_db = self.db.database(schema.LOG_DB_NAME)
        self.log_db.ensure()
        self.server = self.db.server()
        self.ms = MetaStore(self.db)
        self.stores = LocalStores()
        self.task_manager = TaskManager(env, ssl_config)
        self.ssl_config = ssl_config
        try:
            self.local = self.db.get(LOCAL_ID)
        except NotFound:
            self.local = {'_id': LOCAL_ID}
        self.local.update({
            'machine_id': machine['_id'],
            'user_id': user['_id'],
        })
        self.local.pop('stores', None)
        self.local.pop('peers', None)
        (self.machine, self.user) = self.db.get_defaults([machine, user])
        self.machine.update({
            'stores': {},
            'peers': {},
        })
        self.db.save_many([self.local, self.machine, self.user])
        log.info('machine_id = %s', machine['_id'])
        log.info('user_id = %s', user['_id'])

    def save_local(self):
        self.db.save(self.local)

    def start_background_tasks(self):
        self.task_manager.start_tasks()

    def requeue_filestore_tasks(self):
        self.task_manager.requeue_filestore_tasks(tuple(self.stores))

    def restart_vigilance(self):
        self.task_manager.restart_vigilance()

    def get_auto_format(self):
        return self.local.get('auto_format')

    def set_auto_format(self, flag):
        assert type(flag) is bool
        self.local['auto_format'] = flag
        self.save_local()

    def get_skip_internal(self):
        return self.local.get('skip_internal')

    def set_skip_internal(self, flag):
        assert type(flag) is bool
        self.local['skip_internal'] = flag
        self.save_local()

    def add_peer(self, peer_id, info):
        assert isdb32(peer_id) and len(peer_id) == 48
        assert isinstance(info, dict)
        assert isinstance(info['url'], str)
        self.machine = self.db.update(
            mark_add_peer, self.machine, int(time.time()), peer_id, info
        )
        self.restart_vigilance()

    def remove_peer(self, peer_id):
        if peer_id not in self.machine['peers']:
            return False
        self.machine = self.db.update(
            mark_remove_peer, self.machine, int(time.time()), peer_id
        )
        self.restart_vigilance()
        return True

    def _sync_stores(self):
        stores = self.stores.local_stores()
        self.machine = self.db.update(
            mark_connected_stores, self.machine, int(time.time()), stores
        )
        self.restart_vigilance()

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
            self.db.post(fs.doc)
        except Conflict:
            pass
        self.task_manager.queue_filestore_tasks(fs)
        self._sync_stores()

    def _remove_filestore(self, fs):
        log.info('Removing %r', fs)
        self.stores.remove(fs)
        self.task_manager.stop_filestore_tasks(fs)
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

    def create_filestore(self, parentdir, store_id=None, copies=1, **kw):
        """
        Create a new file-store in *parentdir*.
        """
        if util.isfilestore(parentdir):
            raise Exception(
                'Already contains a FileStore: {!r}'.format(parentdir)
            )
        log.info('Creating a new FileStore in %r', parentdir)
        fs = FileStore.create(parentdir, store_id, copies, **kw)
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
