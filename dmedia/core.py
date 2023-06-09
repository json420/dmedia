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
from os import path
import time
import queue
from subprocess import check_call, CalledProcessError
from base64 import b64encode
from collections import namedtuple

from dbase32 import isdb32
from microfiber import Server, Database, NotFound, Conflict, dumps
from filestore import FileStore, FileNotFound
from degu import EmbeddedSSLServer
from gi.repository import GLib

from dmedia.parallel import start_thread, start_process
from dmedia import util, schema, views
from dmedia.client import Downloader, get_client, build_client_sslctx
from dmedia.metastore import MetaStore, create_stored, get_dict
from dmedia.metastore import MIN_BYTES_FREE, MAX_BYTES_FREE
from dmedia.local import LocalStores, FileNotLocal
from dmedia.units import file_count


log = logging.getLogger()
LOCAL_ID = '_local/dmedia'


def build_root_app(couch_env):
    from .rgiapps import RootApp
    return RootApp(couch_env)


def start_httpd(couch_env, sslconfig):
    return EmbeddedSSLServer(
        sslconfig, ('0.0.0.0', 0), build_root_app, couch_env,
    )


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


def snapshot_worker(env, dumpdir, q_in, q_out):
    server = Server(env)
    try:
        check_call(['bzr', 'init', dumpdir])
        whoami = env['machine_id']
        check_call(['bzr', 'whoami', '--branch', '-d', dumpdir, whoami])
        log.info('Initialized bzr branch in %r', dumpdir)
    except CalledProcessError:
        pass
    while True:
        name = q_in.get()
        if name is None:
            q_out.put(None)
            break
        try:
            if name == '__all__':
                dump_all(server, dumpdir)
            else:
                dump_one(server, dumpdir, name)
            q_out.put((name, True))
        except Exception:
            log.exception('Error snapshotting %r', name)
            q_out.put((name, False))


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
        self.peers = ms.get_local_peers()
        if self.peers:
            ssl_context = build_client_sslctx(ssl_config)
            for (peer_id, info) in self.peers.items():
                url = info['url']
                log.info('Vigilance: peer %s at %s', peer_id, url)
                self.clients[peer_id] = get_client(url, ssl_context)

    def update_remote(self):
        """
        Update information about what FileStore are connected to peers.

        A still to be added feature in Core is for it to restart Vigilance when
        any FileStore are added or removed to a peer visible on the local
        network (by monitoring the _changes feed for changes in their machine
        docs).

        Hovever, as a failsafe, we want Vigilance to take responsibility for
        this itself also, but using a different mechanism (in particular,
        something other than the _changes feed).

        So this method is called first thing in each of these methods:

            * `Vigilance.process_backlog()`
            * `Vigilance.process_preempt()`
            * `Vigilance.run_event_loop()`
        """
        self.store_to_peer = {}
        remote = []
        for doc in self.ms.db.get_many(list(self.peers)):
            if doc is not None:
                for store_id in get_dict(doc, 'stores'):
                    if is_store_id(store_id):
                        remote.append(store_id)
                        self.store_to_peer[store_id] = doc['_id']
                        assert doc['_id'] in self.peers
        self.remote = frozenset(remote)
        log.info('Visible remote stores: %s', dumps(self.store_to_peer, pretty=True))

    def run(self):
        self.log_stats()
        for stop_rank in (2, 4, 6):
            last_seq = self.process_backlog(stop_rank)
        self.log_stats()
        self.process_preempt()
        self.log_stats()
        self.run_event_loop(last_seq)

    def log_stats(self):
        result = self.ms.db.view('file', 'rank',
            reduce=True,
            group=True,
            update_seq=True
        )
        log.info('## File ranks as of update_seq=%d:', result['update_seq'])
        for row in result['rows']:
            log.info('## rank=%d: %s', row['key'], file_count(row['value']))

    def process_backlog(self, stop):
        self.update_remote()
        for doc in self.ms.iter_fragile_files(stop):
            self.wrap_up_rank(doc, MIN_BYTES_FREE)
        last_seq = self.ms.db.get()['update_seq']
        log.info('Vigilance: processed backlog: stop=%d, update_seq=%r',
                stop, last_seq)
        return last_seq

    def process_preempt(self):
        self.update_remote()
        for doc in self.ms.iter_preempt_files():
            self.wrap_up_rank(doc, MAX_BYTES_FREE)

    def run_event_loop(self, last_seq):
        self.update_remote()
        log.info('Vigilance: starting event loop at %d', last_seq)
        while True:
            result = self.ms.wait_for_fragile_files(last_seq)
            last_seq = result['last_seq']
            #log.info('vigilance event loop at update_seq %s', last_seq)
            for row in result['results']:
                self.wrap_up_rank(row['doc'], MIN_BYTES_FREE)

    def wrap_up_rank(self, doc, threshold):
        try:
            return self.up_rank(doc, threshold)
        except Exception:
            log.exception('Error calling Vigilance.up_rank() for %r', doc)   

    def up_rank(self, doc, threshold):
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

        FIXME: Dmedia doesn't yet do this!  Probably the best way to implement
        this is for the decision tree here to work as it does, but to add some
        special case handling in Vigilance.up_rank_by_verifying().  Assuming the
        needed space isn't available on another FileStore, we should still at
        least verify the copy.

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
        assert isinstance(threshold, int) and threshold > 0
        stored = set(doc['stored'])
        local = stored.intersection(self.local)
        downgraded = local.intersection(get_downgraded(doc))
        free = self.local - stored
        remote = stored.intersection(self.remote)
        if local:
            if downgraded:
                return self.up_rank_by_verifying(doc, downgraded)
            elif free:
                return self.up_rank_by_copying(doc, free, threshold)
        elif remote:
            return self.up_rank_by_downloading(doc, remote, threshold)

    def up_rank_by_verifying(self, doc, downgraded):
        assert isinstance(downgraded, set)
        store_id = downgraded.pop()
        fs = self.stores.by_id(store_id)
        return self.ms.verify(fs, doc)

    def up_rank_by_copying(self, doc, free, threshold):
        dst = self.stores.filter_by_avail(free, doc['bytes'], 1, threshold)
        if dst:
            src = self.stores.choose_local_store(doc)
            return self.ms.copy(src, doc, *dst)

    def up_rank_by_downloading(self, doc, remote, threshold):
        fs = self.stores.find_dst_store(doc['bytes'], threshold)
        if fs is None:
            return
        peer_ids = frozenset(
            self.store_to_peer[store_id] for store_id in remote
        )
        downloader = None
        _id = doc['_id']
        for peer_id in peer_ids:
            client = self.clients[peer_id]
            if not client.has_file(_id):
                continue
            if downloader is None:
                downloader = Downloader(doc, self.ms, fs)
            try:
                downloader.download_from(client)
            except Exception:
                log.exception('Error downloading %s from %s', _id, client)
            if downloader.download_is_complete():
                return downloader.doc


def vigilance_worker(env, ssl_config):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        vigilance = Vigilance(ms, ssl_config)
        vigilance.run()
    except Exception:
        log.exception('Error in vigilance_worker():')


TIMEOUT = 240

def _pull_replication(peers, sslconfig, dst_id, dst):
    from microfiber.replicator import load_session, replicate
    sslctx = build_client_sslctx(sslconfig)
    start_time = time.monotonic()
    for (src_id, info) in peers.items():
        remaining = int(TIMEOUT + start_time - time.monotonic())
        if remaining < 2:
            log.warning('Reached %d second TIMEOUT', TIMEOUT)
            break
        src_env = {
            'url': info['url'] + 'couch/',
            'ssl': {'context': sslctx},
        }
        src = Database('dmedia-1', src_env)
        session = load_session(src_id, src, dst_id, dst, mode='pull')
        replicate(session, timeout=remaining)


def downgrade_worker(env, sslconfig):
    db = util.get_db(env)
    ms = MetaStore(db)
    peers = ms.get_local_peers()

    # Optionally do pull replication to make sure peer changes have been synced
    # to this node before doing downgrade, as the downgrade can cause a lot of
    # needless noise especially when you add a new peer to a large library; note
    # that we never want a failure here to prevent the downgrade from happening,
    # so we just catch and log any unhandled exception here; the pull
    # replication is really a user experience courtesy, not a data safety
    # feature:
    if peers:
        try:
            _pull_replication(peers, sslconfig, env['machine_id'], db)
        except Exception:
            log.exception('error doing pull-replication in downgrade_worker():')

    # Now do the downgrade; note there is no reason for this to ever fail, so if
    # it does, we specifically want an unhandled exception to terminate the
    # worker process so we get an Apport crash report:
    curtime = int(time.time())
    log.info('downgrading/purging as of timestamp %d', curtime)
    ms.purge_or_downgrade_by_store_atime(curtime)
    ms.downgrade_by_mtime(curtime)
    ms.downgrade_by_verified(curtime)

    # Finally, do a scan/relink here as a backup mechanism for what the
    # filestore_worker() does; note there are common, innocent scenarios under
    # which this will fail (namely, a user unplugs a drive as the scan/relink is
    # happening), so we just catch and log unhandled exceptions here:
    try:
        stores = ms.get_local_stores()
        for fs in stores:
            ms.scan(fs)
        for fs in stores:
            ms.relink(fs)
    except Exception:
        log.exception('error doing scan/relink in downgrade_worker():')


def filestore_worker(env, parentdir, store_id):
    try:
        db = util.get_db(env)
        ms = MetaStore(db)
        fs = FileStore(parentdir, store_id)
        ms.scan(fs)
        ms.relink(fs)
        ms.verify_all(fs, int(time.time()))
    except Exception:
        log.exception('Error in filestore_worker():')


def is_file_id(_id):
    return isinstance(_id, str) and len(_id) == 48 and isdb32(_id)


def is_store_id(_id):
    return isinstance(_id, str) and len(_id) == 24 and isdb32(_id)


def clean_file_id(_id):
    if is_file_id(_id):
        return _id
    return None


TaskInfo = namedtuple('TaskInfo', 'target args')
ActiveTask = namedtuple('ActiveTask', 'key process start_time')


class TaskPool:
    """
    A pool of background tasks executing in a ``multiprocessing.Process`` each.

    Vaguely similar to ``multiprocessing.Pool``, but with some special logic
    we can't easily layer on top.
    """

    def __init__(self, *restart_always):
        self.tasks = {}
        self.active_tasks = {}
        self.thread = None
        self.queue = queue.Queue()
        self.restart_always = frozenset(restart_always)
        self.restart_once = set()
        self.running = False

    def start_reaper(self):
        if self.thread is not None:
            return False
        self.thread = start_thread(self.reaper)
        return True

    def stop_reaper(self):
        if self.thread is None:
            return False
        self.queue.put(None)
        self.thread.join()
        self.thread = None
        return True

    def reaper(self, timeout=2):
        """
        Collect finished tasks and forward them to the main thread.

        As Dmedia will have a small number of relatively long-running background
        tasks, the reaper doesn't need to be especially responsive, so we use a
        relatively long timeout in order to minimize CPU wakeups.
        """
        running = True
        task_map = {}
        while running or task_map:  # Shutdown feature is for unit testing
            try:
                task = self.queue.get(timeout=timeout)
                if task is None:
                    log.info('reaper thread received shutdown request')
                    running = False
                else:
                    assert isinstance(task, ActiveTask)
                    if task.key in task_map:
                        raise ValueError(
                            'key {!r} is already in task_map'.format(task.key)
                        )
                    task_map[task.key] = task
            except queue.Empty:
                pass
            for key in sorted(task_map):  # Sorted to make unit testing easier
                task = task_map[key]
                task.process.join(timeout)
                if not task.process.is_alive():
                    del task_map[key]
                    self.forward_completed_task(task)

    def forward_completed_task(self, task):
        """
        Forward a completed task to the main thread using ``GLib.idle_add()``.

        This is in its own method so it can be mocked for unit tests.
        """
        GLib.idle_add(self.on_task_completed, task)

    def on_task_completed(self, task):
        assert isinstance(task, ActiveTask)
        expected = self.active_tasks.pop(task.key)
        assert task is expected
        task.process.join()  # Make sure process actually terminated
        log.info('on_task_completed: %r', task.key)
        if self.running and self.should_restart(task.key):
            self.queue_task_for_restart(task.key)

    def should_restart(self, key):
        if key not in self.tasks:
            return False
        if key in self.restart_always:
            return True
        try:
            self.restart_once.remove(key)
            return True
        except KeyError:
            return False

    def queue_task_for_restart(self, key):
        log.info('queue_task_for_restart: %r', key)
        GLib.timeout_add(5000, self.on_task_restart, key)

    def on_task_restart(self, key):
        self.start_task(key)

    def add_task(self, key, target, *args):
        assert callable(target)
        if key in self.tasks:
            log.warning('add_task: %r already in tasks', key)
            return False
        log.info('add_task: adding %r', key)
        self.tasks[key] = TaskInfo(target, args)
        if self.running is True:
            self.start_task(key)
        return True

    def remove_task(self, key):
        try:
            self.tasks.pop(key)
            log.info('remove_task: removed %r', key)
            self.stop_task(key)
            return True
        except KeyError:
            return False

    def start_task(self, key):
        if self.running is False:
            return
        if key in self.active_tasks:
            log.warning('start_task: %r already in active_tasks', key)
            return False
        info = self.tasks[key]
        assert isinstance(info, TaskInfo)
        log.info('start_task: starting %r', key)
        ts = time.time()
        process = start_process(info.target, *info.args)
        task = ActiveTask(key, process, ts)
        assert key not in self.active_tasks
        self.active_tasks[key] = task
        self.queue.put(task)
        return True

    def stop_task(self, key):
        try:
            self.active_tasks[key].process.terminate()
            log.info('stop_task: stopping %r', key)
            return True
        except KeyError:
            log.warning('stop_task: %r not in active_tasks', key)
            return False

    def restart_task(self, key):
        if self.running is False:
            log.warning('restart_task: TaskPool is not running: %r', key)
            return
        if key not in self.tasks:
            log.warning('restart_task: %r not in tasks', key)
            return 0
        log.info('restart_task: %r', key)
        if self.stop_task(key):
            if key not in self.restart_always:
                self.restart_once.add(key)
            return True
        self.start_task(key)
        return False

    def start(self):
        if self.running is True:
            return False
        self.running = True
        self.start_reaper()
        for key in sorted(self.tasks):  # Sorted to make unit testing easier
            self.start_task(key)
        return True

    def stop(self):
        if self.running is False:
            return False
        self.running = False
        for key in sorted(self.active_tasks):  # Sorted to make unit testing easier
            self.stop_task(key)
        return True 


def build_fs_key(fs):
    return ('filestore', fs.parentdir)


def build_replication_key(peer_id):
    """
    For example:

    >>> build_replication_key('mypeer')
    ('sync', 'mypeer')

    """
    return ('sync', peer_id)


def names_filter_func(name):
    if name.startswith('dmedia-0') or name.startswith('novacut-0'):
        return False
    return name not in {'thumbnails-1', 'rpk-dmedia-1'}


def replication_worker(src_env, dst_env):
    from microfiber.replicator import Replicator
    replicator = Replicator(src_env, dst_env, names_filter_func)
    replicator.run()


VIGILANCE = ('vigilance',)
DOWNGRADE = ('downgrade',)


class TaskMaster:
    def __init__(self, env, ssl_config):
        self.env = env
        self.ssl_config = ssl_config
        self.pool = TaskPool(VIGILANCE)  # vigilance is auto-restarted

    def add_filestore_task(self, fs):
        key = build_fs_key(fs)
        self.pool.add_task(key, filestore_worker, self.env, fs.parentdir, fs.id)

    def remove_filestore_task(self, fs):
        self.pool.remove_task(build_fs_key(fs))

    def restart_filestore_task(self, fs):
        self.pool.restart_task(build_fs_key(fs))

    def add_replication_task(self, peer_id, url):
        key = build_replication_key(peer_id)
        src_env = self.env
        dst_env = {
            'url': url + 'couch/',
            'ssl': self.ssl_config,
            'no_host': True,
            'no_user_agent': True,
        }
        # In case a previous replication with same peer_id but different url:
        self.pool.remove_task(key)
        self.pool.add_task(key, replication_worker, src_env, dst_env)

    def remove_replication_task(self, peer_id):
        key = build_replication_key(peer_id)
        self.pool.remove_task(key)

    def restart_replication_task(self, peer_id):
        key = build_replication_key(peer_id)
        self.pool.restart_task(key)

    def add_vigilance_task(self):
        self.pool.add_task(VIGILANCE, vigilance_worker, self.env, self.ssl_config)

    def restart_vigilance_task(self):
        self.pool.restart_task(VIGILANCE)

    def add_downgrade_task(self):
        self.pool.add_task(DOWNGRADE, downgrade_worker, self.env, self.ssl_config)

    def restart_downgrade_task(self):
        self.pool.restart_task(DOWNGRADE)

    def start_tasks(self):
        self.pool.start()


def update_machine(doc, timestamp, stores, peers):
    """
    Update func passed to `Database.update()` by `Core.update_machine()`.
    """
    assert isinstance(timestamp, float)
    assert isinstance(stores, dict)
    assert isinstance(peers, dict)
    doc['mtime'] = int(timestamp)
    doc['stores'] = stores
    doc['peers'] = peers


class Core:
    def __init__(self, env, machine, user, ssl_config):
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
        self.peers = {}
        self.task_master = TaskMaster(env, ssl_config)
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
            'mtime': int(time.time()),
            'stores': {},
            'peers': {},
        })
        self.db.save_many([self.local, self.machine, self.user])
        log.info('machine_id = %s', machine['_id'])
        log.info('user_id = %s', user['_id'])

    def save_local(self):
        self.db.save(self.local)

    def update_machine(self):
        """
        Synchronize the machine state to CouchDB.

        Each Dmedia machine (aka peer, node) is represented by a doc in CouchDB
        (with a type of "dmedia/machine").  This doc stores info about the
        currently connected stores, the currently visible peers, and a timestamp
        of when the machine state last changed.

        The currently connected stores are very important because this is used
        to decide which peer to attempt to download a file from.

        The currently connected peers aren't directly used, but are handy for
        out-of-band test harnesses.  For example, by monitoring changes in all
        the "dmedia/machine" docs, a test harness can determine whether the
        Avahi peer broadcast and discovery is working correctly.
        """
        stores = self.stores.local_stores()
        self.machine = self.db.update(
            update_machine, self.machine, time.time(), stores, self.peers
        )

    def start_background_tasks(self):
        self.task_master.start_tasks()

    def restart_replication_tasks(self):
        log.info('**** restart_replication_tasks')
        for peer_id in sorted(self.peers):
            self.task_master.restart_replication_task(peer_id)
        return True  # So GLib timeout call repeats

    def restart_filestore_tasks(self):
        log.info('**** restart_filestore_tasks')
        for fs in self.stores:
            self.task_master.restart_filestore_task(fs)
        return True  # So GLib timeout call repeats

    def restart_downgrade_task(self):
        log.info('**** restart_downgrade_task')
        self.task_master.restart_downgrade_task()
        return True  # So GLib timeout call repeats

    def restart_vigilance(self):
        log.info('**** restart_vigilance')
        # FIXME: Core should also restart Vigilance whenever the FileStore
        # connected to a peer change.  We should do this by monitoring the
        # _changes feed for changes to any of the machine docs corresponding to
        # the currently visible local peers.
        self.task_master.restart_vigilance_task()
        return True  # So GLib timeout call repeats

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
        self.peers[peer_id] = info
        self.update_machine()
        self.task_master.add_replication_task(peer_id, info['url'])
        self.restart_vigilance()

    def remove_peer(self, peer_id):
        if peer_id not in self.peers:
            return False
        self.peers.pop(peer_id)
        self.update_machine()
        self.task_master.remove_replication_task(peer_id)
        self.restart_vigilance()
        return True

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
        self.update_machine()
        self.task_master.add_filestore_task(fs)
        self.restart_vigilance()

    def _remove_filestore(self, fs):
        log.info('Removing %r', fs)
        self.task_master.remove_filestore_task(fs)
        self.stores.remove(fs)
        self.update_machine()
        self.restart_vigilance()

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
        fs = FileStore(parentdir, expected_id)
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
