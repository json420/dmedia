# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
A FileStore-like API that also updates the CouchDB docs.

The FileStore only deals with files, and does nothing with the metadata about
the files.

The idea with the MetaStore is to wrap both the file and metadata operations
together with a high-level API.  A good example is copying a file from one
FileStore to another, which involves a fairly complicated metadata update:

    1. As we verify as we read, upon a successful read we update the
       verification timestamp for the source FileStore; if the file is corrupt
       or missing, we likewise update the document accordingly

    2. We also need to update doc['stored'] with each new FileStore this file is
       now in


Types of background tasks:

    Pure metadata
        These tasks operate across the entire library metadata, regardless of
        what FileStore (drives) are connected to the local machine.

        Currently this includes the schema check and the various downgrade
        behaviors.

    All connected FileStore
        These tasks must consider all FileStore (drives) connected to the local
        machine, plus metadata across the entire library.

        Currently this includes the copy increasing and copy decreasing
        behaviors.

    Single connected FileStore
        These tasks consider only the metadata for files that are (assumed) to
        be stored in a single FileStore (drive) connected to the local machine.

        Currently this includes the scan, relink, and verify behaviors.
"""

import time
import os
import logging
from http.client import ResponseNotReady

from filestore import FileStore, CorruptFile, FileNotFound, check_root_hash
from microfiber import NotFound, Conflict, BulkConflict, id_slice_iter

from .units import count_and_size, bytes10
from .constants import TYPE_ERROR
from .local import LocalStores


log = logging.getLogger()

DAY = 24 * 60 * 60
WEEK = 7 * DAY
DOWNGRADE_BY_NEVER_VERIFIED = 2 * DAY
VERIFY_BY_MTIME = DOWNGRADE_BY_NEVER_VERIFIED // 4
VERIFY_THRESHOLD = WEEK
DOWNGRADE_BY_STORE_ATIME = WEEK
DOWNGRADE_BY_LAST_VERIFIED = 2 * WEEK
VERIFY_BY_VERIFIED = DOWNGRADE_BY_LAST_VERIFIED // 2

GiB = 1024**3
RECLAIM_BYTES = 16 * GiB


class MTimeMismatch(Exception):
    pass


class TimeDelta:
    __slots__ = ('start', 'end')

    def __init__(self):
        self.start = time.perf_counter()
        self.end = None

    @property
    def delta(self):
        if self.end is None:
            self.end = time.perf_counter()
        return self.end - self.start

    def log(self, msg, *args):
        log.info('%.3fs to ' + msg, self.delta, *args)

    def rate(self, size):
        rate = int(size / self.delta)
        return '{}/s'.format(bytes10(rate))


def get_dict(d, key):
    """
    Force value for *key* in *d* to be a ``dict``.

    For example:

    >>> doc = {}
    >>> get_dict(doc, 'foo')
    {}
    >>> doc
    {'foo': {}}

    """
    if not isinstance(d, dict):
        raise TypeError(TYPE_ERROR.format('d', dict, type(d), d))
    if not isinstance(key, str):
        raise TypeError(TYPE_ERROR.format('key', str, type(key), key))
    value = d.get(key)
    if isinstance(value, dict):
        return value
    d[key] = {}
    return d[key]


def get_mtime(fs, _id):
    return int(fs.stat(_id).mtime)


def create_stored_value(_id, fs, verified=None):
    value = {
        'copies': fs.copies,
        'mtime': get_mtime(fs, _id),
    }
    if verified is not None:
        assert isinstance(verified, (int, float))
        value['verified'] = int(verified)
    return value


def create_stored(_id, *filestores):
    """
    Create doc['stored'] for file with *_id* stored in *filestores*.
    """
    return dict(
        (fs.id, create_stored_value(_id, fs))
        for fs in filestores
    )


def merge_stored(old, new):
    """
    Update doc['stored'] based on storage information in *new*.
    """
    assert isinstance(old, dict)
    assert isinstance(new, dict)
    for (key, value) in new.items():
        assert isinstance(key, str)
        assert isinstance(value, dict)
        assert set(value) == set(['copies', 'mtime'])
        if key in old:
            old_value = get_dict(old, key)
            old_value.update(value)
            old_value.pop('verified', None)
        else:
            old[key] = value 


def mark_added(doc, new):
    old = get_dict(doc, 'stored')
    merge_stored(old, new)


def mark_downloading(doc, timestamp, fs_id):
    """
    Add download in progress entry in doc['partial'].
    """
    partial = get_dict(doc, 'partial')
    partial[fs_id] = {'time': timestamp}


def mark_downloaded(doc, fs_id, new):
    """
    Update doc appropriately after a download completes.
    """
    assert fs_id in new
    old = get_dict(doc, 'stored')
    merge_stored(old, new)
    partial = get_dict(doc, 'partial')
    partial.pop(fs_id, None)
    if not partial:
        del doc['partial']


def mark_removed(doc, *removed):
    stored = get_dict(doc, 'stored')
    for fs_id in removed:
        assert isinstance(fs_id, str)
        stored.pop(fs_id, None)


def mark_verified(doc, fs_id, new_value):
    assert isinstance(new_value, dict)
    assert set(new_value) == set(['copies', 'mtime', 'verified'])
    assert isinstance(new_value['copies'], int)
    assert isinstance(new_value['mtime'], int)
    assert isinstance(new_value['verified'], int)
    stored = get_dict(doc, 'stored')
    old_value = get_dict(stored, fs_id)
    old_value.update(new_value)


def mark_corrupt(doc, timestamp, fs_id):
    stored = get_dict(doc, 'stored')
    stored.pop(fs_id, None)
    corrupt = get_dict(doc, 'corrupt')
    corrupt[fs_id] = {'time': timestamp}


def mark_copied(doc, timestamp, src_id, new):
    assert src_id in new
    assert len(new) >= 2
    old = get_dict(doc, 'stored')
    merge_stored(old, new)
    old[src_id]['verified'] = int(timestamp)


def mark_mismatched(doc, fs_id, mtime):
    """
    Update 'mtime' and 'copies', delete 'verified', preserve 'pinned'.
    """
    assert isinstance(mtime, int)
    stored = get_dict(doc, 'stored')
    value = get_dict(stored, fs_id)
    value.update({'copies': 0, 'mtime': mtime})
    value.pop('verified', None)


class VerifyContext:
    __slots__ = ('db', 'fs', 'doc')

    def __init__(self, db, fs, doc):
        self.db = db
        self.fs = fs
        self.doc = doc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            log.info('Verified %s in %r', self.doc['_id'], self.fs)
            value = create_stored_value(self.doc['_id'], self.fs, time.time())
            self.db.update(mark_verified, self.doc, self.fs.id, value)
        elif issubclass(exc_type, CorruptFile):
            log.error('%s is corrupt in %r', self.doc['_id'], self.fs)
            self.db.update(mark_corrupt, self.doc, time.time(), self.fs.id)
        elif issubclass(exc_type, FileNotFound):
            log.warning('%s is not in %r', self.doc['_id'], self.fs)
            self.db.update(mark_removed, self.doc, self.fs.id)
        else:
            return False
        return True


class ScanContext:
    __slots__ = ('db', 'fs', 'doc')

    def __init__(self, db, fs, doc):
        self.db = db
        self.fs = fs
        self.doc = doc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            return
        if issubclass(exc_type, FileNotFound):
            log.warning('%s is not in %r', self.doc['_id'], self.fs)
            self.db.update(mark_removed, self.doc, self.fs.id)
        elif issubclass(exc_type, CorruptFile):
            log.warning('%s has wrong size in %r', self.doc['_id'], self.fs)
            self.db.update(mark_corrupt, self.doc, time.time(), self.fs.id)
        elif issubclass(exc_type, MTimeMismatch):
            log.warning('%s has wrong mtime in %r', self.doc['_id'], self.fs)
            mtime = get_mtime(self.fs, self.doc['_id'])
            self.db.update(mark_mismatched, self.doc, self.fs.id, mtime)
        else:
            return False
        return True


def relink_iter(fs, count=25):
    buf = []
    for st in fs:
        buf.append(st)
        if len(buf) >= count:
            yield buf
            buf = []
    if buf:
        yield buf


class BufferedSave:
    __slots__ = ('db', 'size', 'docs', 'count', 'conflicts')

    def __init__(self, db, size=25):
        self.db = db
        self.size = size
        self.docs = []
        self.count = 0
        self.conflicts = 0

    def __del__(self):
        self.flush()

    def save(self, doc):
        self.docs.append(doc)
        if len(self.docs) >= self.size:
            self.flush()

    def flush(self):
        if self.docs:
            self.count += len(self.docs)
            try:
                self.db.save_many(self.docs)
            except BulkConflict as e:
                log.exception('Conflicts in BufferedSave.flush()')
                self.conflicts += len(e.conflicts)
            self.docs = []


class MetaStore:
    def __init__(self, db):
        self.db = db

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.db)

    def doc_and_id(self, obj):
        if isinstance(obj, dict):
            return (obj, obj['_id'])
        if isinstance(obj, str):
            return (self.db.get(obj), obj)
        raise TypeError('obj must be a doc or _id (a dict or str)')

    def content_hash(self, doc_or_id, unpack=True):
        (doc, _id) = self.doc_and_id(doc_or_id)
        leaf_hashes = self.db.get_att(_id, 'leaf_hashes').data
        return check_root_hash(_id, doc['bytes'], leaf_hashes, unpack)

    def get_local_stores(self):
        try:
            doc = self.db.get('_local/dmedia')
        except NotFound:
            doc = {'stores': {}}
        local_stores = LocalStores()
        for (parentdir, info) in doc['stores'].items():
            fs = FileStore(parentdir, info['id'])
            local_stores.add(fs)
        return local_stores

    def get_peers(self):
        try:
            doc = self.db.get('_local/peers')
            self._peers = get_dict(doc, 'peers')
        except NotFound:
            self._peers = {}
        return self._peers

    def iter_stores(self):
        result = self.db.view('file', 'stored', reduce=True, group=True)
        for row in result['rows']:
            yield row['key']

    def schema_check(self):
        """
        If needed, migrate mtime from float to int.
        """
        buf = BufferedSave(self.db)
        rows = self.db.view('doc', 'type', key='dmedia/file')['rows']
        for ids in id_slice_iter(rows):
            for doc in self.db.get_many(ids):
                changed = False
                for value in doc['stored'].values():
                    if isinstance(value.get('mtime'), float):
                        changed = True
                        value['mtime'] = int(value['mtime'])
                if changed:
                    buf.save(doc)
        buf.flush()
        log.info('converted mtime from `float` to `int` for %d docs', buf.count)
        return buf.count

    def downgrade_by_never_verified(self, curtime=None):
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        endkey = curtime - DOWNGRADE_BY_NEVER_VERIFIED
        return self._downgrade_by_verified(endkey, 'never-verified')

    def downgrade_by_last_verified(self, curtime=None):
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        endkey = curtime - DOWNGRADE_BY_LAST_VERIFIED
        return self._downgrade_by_verified(endkey, 'last-verified')

    def _downgrade_by_verified(self, endkey, view):
        t = TimeDelta()
        count = 0
        while True:
            rows = self.db.view('file', view,
                endkey=endkey,
                include_docs=True,
                limit=100,
            )['rows']
            if not rows:
                break
            dmap = dict(
                (row['id'], row['doc']) for row in rows
            )
            for row in rows:
                doc = dmap[row['id']]
                doc['stored'][row['value']]['copies'] = 0
            docs = list(dmap.values())
            count += len(docs)
            try:
                self.db.save_many(docs)
            except BulkConflict as e:
                log.exception('Conflict in downgrade_by %r', view)
                count -= len(e.conflicts)
        t.log('downgrade %d files by %s', count, view)
        return count

    def downgrade_by_store_atime(self, curtime=None):
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        threshold = curtime - DOWNGRADE_BY_STORE_ATIME
        t = TimeDelta()
        result = {}
        for store_id in self.iter_stores():
            try:
                doc = self.db.get(store_id)
                atime = doc.get('atime')
                if isinstance(atime, int) and atime > threshold:
                    log.info('Store %s okay at atime %s', store_id, atime)
                    continue
            except NotFound:
                log.warning('doc NotFound for %s, forcing downgrade', store_id)
            result[store_id] = self.downgrade_store(store_id)
        total = sum(result.values())
        t.log('downgrade %d total copies in %d stores', total, len(result))
        return result

    def downgrade_store(self, store_id):
        t = TimeDelta()
        count = 0
        while True:
            rows = self.db.view('file', 'nonzero',
                key=store_id,
                include_docs=True,
                limit=100,
            )['rows']
            if not rows:
                break
            docs = [r['doc'] for r in rows]
            for doc in docs:
                doc['stored'][store_id]['copies'] = 0
                doc['stored'][store_id].pop('verified', None)
            count += len(docs)
            try:
                self.db.save_many(docs)
            except BulkConflict as e:
                log.exception('Conflict downgrading %s', store_id)
                count -= len(e.conflicts)
        t.log('downgrade %d copies in %s', count, store_id)
        return count

    def downgrade_all(self):
        """
        Downgrade every file in every store.

        Note: this is only really useful for testing.
        """
        t = TimeDelta()
        count = 0
        stores = tuple(self.iter_stores())
        for store_id in stores:
            count += self.downgrade_store(store_id)
        t.log('downgrade %d total copies in %d stores', count, len(stores))
        return count

    def purge_store(self, store_id):
        t = TimeDelta()
        count = 0
        while True:
            rows = self.db.view('file', 'stored',
                key=store_id,
                include_docs=True,
                limit=100,
            )['rows']
            if not rows:
                break
            docs = [r['doc'] for r in rows]
            for doc in docs:
                del doc['stored'][store_id]
            count += len(docs)
            try:
                self.db.save_many(docs)
            except BulkConflict:
                log.exception('Conflict purging %s', store_id)
                count -= len(e.conflicts)
        t.log('purge %d copies from %s', count, store_id)
        return count

    def purge_all(self):
        """
        Purge every file in every store.

        Note: this is only really useful for testing.
        """
        t = TimeDelta()
        kw = {
            'key': 'dmedia/file',
            'include_docs': True,
            'limit': 50,
            'skip': 0,
        }
        while True:
            rows = self.db.view('doc', 'type', **kw)['rows']
            if not rows:
                break
            kw['skip'] += len(rows)
            docs = [r['doc'] for r in rows]
            for doc in docs:
                doc['stored'] = {}    
            try:
                self.db.save_many(docs)
            except BulkConflict:
                log.exception('MetaStore.purge_all():')
        count = kw['skip']
        t.log('fully purge %d files', count)
        return count

    def scan(self, fs):
        """
        Make sure files we expect to be in the file-store *fs* actually are.

        A fundamental design tenet of Dmedia is that it doesn't particularly
        trust its metadata, and instead does frequent reality checks.  This
        allows Dmedia to work even though removable storage is constantly
        "offline".  In other distributed file-systems, this is usually called
        being in a "network-partitioned" state.

        Dmedia deals with removable storage via a quickly decaying confidence
        in its metadata.  If a removable drive hasn't been connected longer
        than some threshold, Dmedia will update all those copies to count for
        zero durability.

        And whenever a removable drive (on any drive for that matter) is
        connected, Dmedia immediately checks to see what files are actually on
        the drive, and whether they have good integrity.

        `MetaStore.scan()` is the most important reality check that Dmedia does
        because it's fast and can therefor be done quite often. Thousands of
        files can be scanned in a few seconds.

        The scan insures that for every file expected in this file-store, the
        file exists, has the correct size, and the expected mtime.

        If the file doesn't exist in this file-store, its store_id is deleted
        from doc['stored'] and the doc is saved.

        If the file has the wrong size, it's moved into the corrupt location in
        the file-store. Then the doc is updated accordingly marking the file as
        being corrupt in this file-store, and the doc is saved.

        If the file doesn't have the expected mtime is this file-store, this
        copy gets downgraded to zero copies worth of durability, and the last
        verification timestamp is deleted, if present.  This will put the file
        first in line for full content-hash verification.  If the verification
        passes, the durability is raised back to the appropriate number of
        copies.

        :param fs: a `FileStore` instance
        """
        t = TimeDelta()
        count = 0
        kw = {
            'key': fs.id,
            'include_docs': True,
            'limit': 25,
            'skip': 0,
        }
        while True:
            rows = self.db.view('file', 'stored', **kw)['rows']
            if not rows:
                break
            kw['skip'] += len(rows)
            count += len(rows)
            for row in rows:
                doc = row['doc']
                _id = doc['_id']
                try:
                    st = fs.stat(_id)
                    stored = get_dict(doc, 'stored')
                    value = get_dict(stored, fs.id)
                    if doc.get('bytes') != st.size:
                        log.error('%s has wrong size in %r', _id, fs)
                        src_fp = open(st.name, 'rb')
                        fs.move_to_corrupt(src_fp, _id,
                            file_size=doc['bytes'],
                            bad_file_size=st.size,
                        )
                        kw['skip'] -= 1
                        self.db.update(mark_corrupt, doc, time.time(), fs.id)
                    elif value.get('mtime') != int(st.mtime):
                        log.warning('%s has wrong mtime %r', _id, fs)
                        self.db.update(mark_mismatched, doc, fs.id, int(st.mtime))
                except FileNotFound:
                    log.warning('%s is not in %r', _id, fs)
                    kw['skip'] -= 1
                    self.db.update(mark_removed, doc, fs.id)
        # Update the atime for the dmedia/store doc
        try:
            doc = self.db.get(fs.id)
            assert doc['type'] == 'dmedia/store'
            doc['atime'] = int(time.time())
            self.db.save(doc)
        except NotFound:
            log.warning('No doc for FileStore %s', fs.id)
        t.log('scan %r files in %r', count, fs)
        return count

    def relink(self, fs):
        """
        Find known files that we didn't expect in `FileStore` *fs*.
        """
        t = TimeDelta()
        count = 0
        for buf in relink_iter(fs):
            docs = self.db.get_many([st.id for st in buf])
            for (st, doc) in zip(buf, docs):
                if doc is None:
                    continue
                stored = get_dict(doc, 'stored')
                if fs.id in stored:
                    continue
                log.info('Relinking %s in %r', st.id, fs)
                new = {
                    fs.id: {'copies': fs.copies, 'mtime': int(st.mtime)}
                }
                self.db.update(mark_added, doc, new)
                count += 1
        t.log('relink %d files in %r', count, fs)
        return count

    def remove(self, fs, _id):
        doc = self.db.get(_id)
        mark_removed(doc, fs.id)
        self.db.save(doc)
        fs.remove(_id)
        return doc

    def verify(self, fs, doc_or_id, return_fp=False):
        (doc, _id) = self.doc_and_id(doc_or_id)
        with VerifyContext(self.db, fs, doc):
            return fs.verify(_id, return_fp)

    def verify_all(self, fs, curtime=None):
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        count = 0
        size = 0

        t = TimeDelta()

        # First verify files with no 'verified' timestamp, whose 'mtime' is older
        # than 6 hours:
        kw = {
            'startkey': [fs.id, None],
            'endkey': [fs.id, curtime - VERIFY_BY_MTIME],
            'limit': 1,
            'include_docs': True,
        }
        while True:
            rows = self.db.view('file', 'store-mtime', **kw)['rows']
            if not rows:
                break
            doc = rows[0]['doc']
            self.verify(fs, doc)
            count += 1
            size += doc['bytes']

        # Now verify files whose 'verified' timestamp is older than 2 weeks:            
        kw['endkey'] = [fs.id, curtime - VERIFY_BY_VERIFIED]
        while True:
            rows = self.db.view('file', 'store-verified', **kw)['rows']
            if not rows:
                break
            doc = rows[0]['doc']
            self.verify(fs, doc)
            count += 1
            size += doc['bytes']

        t.log('verify %s in %r [%s]', count_and_size(count, size), fs, t.rate(size))
        return count

    def content_md5(self, fs, _id, force=False):
        doc = self.db.get(_id)
        if not force:
            try:
                return doc['content_md5']
            except KeyError:
                pass    
        with VerifyContext(self.db, fs, doc):
            (b16, b64) = fs.content_md5(_id)
            doc['content_md5'] = b64
            return b64

    def allocate_partial(self, fs, _id):
        doc = self.db.get(_id)
        (content_type, leaf_hashes) = self.db.get_att(_id, 'leaf_hashes')
        ch = check_root_hash(_id, doc['bytes'], leaf_hashes)
        tmp_fp = fs.allocate_partial(ch.file_size, ch.id)
        partial = get_dict(doc, 'partial')
        partial[fs.id] = {'mtime': os.fstat(tmp_fp.fileno()).st_mtime}
        self.db.save(doc)
        return tmp_fp

    def start_download(self, fs, doc):
        tmp_fp = fs.allocate_partial(doc['bytes'], doc['_id'])
        self.db.update(mark_downloading, doc, time.time(), fs.id)
        return tmp_fp

    def finish_download(self, fs, doc, tmp_fp):
        fs.move_to_canonical(tmp_fp, doc['_id'])
        new = create_stored(doc['_id'], fs)
        return self.db.update(mark_downloaded, doc, fs.id, new)

    def verify_and_move(self, fs, tmp_fp, _id):
        doc = self.db.get(_id)
        ch = fs.verify_and_move(tmp_fp, _id)
        partial = get_dict(doc, 'partial')
        try:
            del partial[fs.id]
        except KeyError:
            pass
        if not partial:
            del doc['partial']
        new = create_stored(_id, fs)
        mark_added(doc, new)
        self.db.save(doc)
        return ch

    def copy(self, src, doc_or_id, *dst):
        (doc, _id) = self.doc_and_id(doc_or_id)
        try:
            ch = src.copy(_id, *dst)
            log.info('Copied %s\n  from %r\n  to %r', _id, src, list(dst))
            new = create_stored(_id, src, *dst)
            self.db.update(mark_copied, doc, time.time(), src.id, new)
        except FileNotFound:
            log.warning('%s is not in %r', _id, src)
            self.db.update(mark_removed, doc, src.id)
        except CorruptFile:
            log.error('%s is corrupt in %r', _id, src)
            self.db.update(mark_corrupt, doc, time.time(), src.id)
        return doc

    def iter_fragile(self, monitor=False):
        """
        Yield doc for each fragile file.     
        """
        for copies in range(3):
            r = self.db.view('file', 'fragile', key=copies, update_seq=True)
            log.info('%d files with copies=%d', len(r['rows']), copies)
            for row in r['rows']:
                yield self.db.get(row['id'])
            update_seq = r.get('update_seq')
        if not monitor:
            return

        # Now we enter an event-based loop using the _changes feed:
        if update_seq is None:
            update_seq = self.db.get()['update_seq']
        kw = {
            'feed': 'longpoll',
            'include_docs': True,
            'filter': 'file/fragile',
            'since': update_seq,
        }
        while True:
            try:
                r = self.db.get('_changes', **kw)
                log.info('last_seq: %s', r['last_seq'])
                for row in r['results']:
                    yield row['doc']
                kw['since'] = r['last_seq']
            except ResponseNotReady:
                pass

    def iter_actionable_fragile(self, connected, monitor=False):
        """
        Yield doc for each fragile file that this node might be able to fix.

        To be "actionable", this machine must have at least one currently
        connected FileStore (drive) that does *not* already contain a copy of
        the fragile file.       
        """
        assert isinstance(connected, frozenset)
        for doc in self.iter_fragile(monitor):
            stored = frozenset(get_dict(doc, 'stored'))
            if (connected - stored):
                yield (doc, stored)

    def reclaim(self, fs, threshold=RECLAIM_BYTES):
        count = 0
        size = 0
        t = TimeDelta()
        while True:
            kw = {
                'startkey': [fs.id, None],
                'endkey': [fs.id, int(time.time())],
                'limit': 1,
            }
            rows = self.db.view('file', 'store-reclaimable', **kw)['rows']
            if not rows:
                break
            doc = self.remove(fs, rows[0]['id'])
            count += 1
            size += doc['bytes']
            if fs.statvfs().avail > threshold:
                break
        if count > 0:
            t.log('reclaim %s in %r', count_and_size(count, size), fs)
        return (count, size)

    def reclaim_all(self, threshold=RECLAIM_BYTES):
        try:
            count = 0
            size = 0
            t = TimeDelta()
            filestores = self.get_local_stores().sort_by_avail(reverse=False)
            for fs in filestores:
                if fs.statvfs().avail > threshold:
                    break
                (c, s) = self.reclaim(fs, threshold)
                count += c
                size += s
            n = len(filestores)
            t.log('reclaim %s in %d filestores', count_and_size(count, size), n)
            return (count, size, n)
        except Exception:
            log.exception('error in MetaStore.reclaim_all():')
            raise

