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
from random import SystemRandom
from copy import deepcopy

from dbase32 import log_id
from filestore import FileStore, CorruptFile, FileNotFound, check_root_hash
from microfiber import NotFound, Conflict, BadRequest, BulkConflict
from microfiber import id_slice_iter, dumps

from .units import count_and_size, bytes10
from .constants import TYPE_ERROR
from .local import LocalStores


log = logging.getLogger()
random = SystemRandom()

DAY = 86400  # Seconds in a day

DOWNGRADE_BY_MTIME = DAY
DOWNGRADE_BY_STORE_ATIME = 3 * DAY
PURGE_BY_STORE_ATIME = 7 * DAY
DOWNGRADE_BY_VERIFIED = 12 * DAY

VERIFY_BY_MTIME = DOWNGRADE_BY_MTIME // 8
VERIFY_BY_VERIFIED = DOWNGRADE_BY_VERIFIED // 2

GiB = 1024**3
RECLAIM_BYTES = 64 * GiB



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


def get_int(d, key):
    """
    Force value for *key* in *d* to be an ``int`` >= 0.

    For example:

    >>> doc = {'foo': 'BAR'}
    >>> get_int(doc, 'foo')
    0
    >>> doc
    {'foo': 0}

    """
    if not isinstance(d, dict):
        raise TypeError(TYPE_ERROR.format('d', dict, type(d), d))
    if not isinstance(key, str):
        raise TypeError(TYPE_ERROR.format('key', str, type(key), key))
    value = d.get(key)
    if isinstance(value, int) and value >= 0:
        return value
    d[key] = 0
    return d[key]


def get_rank(doc):
    """
    Calculate the rank of the file represented by *doc*.

    The rank of a file is the number of copies assumed to exist plus the sum
    of the assumed durability of those copies, basically::

        rank = len(doc['stored']) + sum(v['copies'] for v in doc['stored'].values())

    However, this function can cope with an arbitrarily broken *doc*, as long as
    *doc* is at least a ``dict`` instance.  For example:

    >>> doc = {
    ...     'stored': {
    ...         'FOO': {'copies': 1},
    ...         'BAR': {'copies': -6},
    ...         'BAZ': 'junk',
    ...     },
    ... }
    >>> get_rank(doc)
    4

    Any needed schema coercion is done in place:

    >>> doc == {
    ...     'stored': {
    ...         'FOO': {'copies': 1},
    ...         'BAR': {'copies': 0},
    ...         'BAZ': {'copies': 0},
    ...     },
    ... }
    True

    It even works with an empty doc:

    >>> doc = {}
    >>> get_rank(doc)
    0
    >>> doc
    {'stored': {}}

    """
    stored = get_dict(doc, 'stored')
    locations = len(stored)
    durability = 0
    for key in stored:
        value = get_dict(stored, key)
        durability += get_int(value, 'copies')
    return min(3, locations) + min(3, durability)


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


def mark_deleted(doc):
    doc['_deleted'] = True


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
    def __init__(self, db, log_db=None):
        self.db = db
        if log_db is None:
            log_db = db.database('log-1')
        self.log_db = log_db
        self.machine_id = db.env.get('machine_id')

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.db)

    def log(self, timestamp, _type, **kw):
        doc = kw
        doc.update({
            '_id': log_id(timestamp),
            'time': timestamp,
            'type': _type,
            'machine_id': self.machine_id, 
        })
        self.log_db.save(doc)
        return doc

    def log_file_corrupt(self, timestamp, fs, _id):
        return self.log(timestamp, 'dmedia/file/corrupt',
            file_id=_id,
            store_id=fs.id,
            drive_model=fs.doc.get('drive_model'),
            drive_serial=fs.doc.get('drive_serial'),
            filesystem_uuid=fs.doc.get('filesystem_uuid'),
        )

    def log_store_purge(self, timestamp, store_id, count):
        return self.log(timestamp, 'dmedia/store/purge',
            store_id=store_id,
            count=count,
        )

    def log_store_downgrade(self, timestamp, store_id, count):
        return self.log(timestamp, 'dmedia/store/downgrade',
            store_id=store_id,
            count=count,
        )

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

    def get_machine(self):
        try:
            return self.db.get(self.machine_id)
        except NotFound:
            return {}

    def get_local_stores(self):
        doc = self.get_machine()
        stores = get_dict(doc, 'stores')
        local_stores = LocalStores()
        for (_id, info) in stores.items():
            fs = FileStore(info['parentdir'], _id)
            local_stores.add(fs)
        return local_stores

    def get_local_peers(self):
        doc = self.get_machine()
        self._peers = get_dict(doc, 'peers')
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

    def downgrade_by_mtime(self, curtime):
        """
        Downgrade unverified copies with 'mtime' older than `DOWNGRADE_BY_MTIME`.

        This method only downgrades copies that meet all these criteria:

            1. The copy has never been verified (ie, it has no 'verified'
               timestamp)

            2. The copy isn't already downgraded (ie, copies != 0)

            3. The copy 'mtime' is older than `DOWNGRADE_BY_MTIME`

        When a new copy is created (for example, via new ingest), the copy will
        be in a normal, non-downgraded state (copies >= 1), but the copy wont
        have been verified yet (it wont have a 'verified' timestamp).

        So that verification doesn't annoyingly nip-at-the-heals of the copy
        increasing behaviors, there is a grace period of `VERIFY_BY_MTIME`
        seconds before this new copy will be verified.  This is a short interval
        (currently 3 hours).

        When `MetaStore.verify_by_mtime()` is periodically called for each
        connected `FileStore`, it will verify any copies whose 'mtime' is older
        than `VERIFY_BY_MTIME`.

        However, if that hasn't happened by the time a copy 'mtime' reaches
        `DOWNGRADE_BY_MTIME` seconds in age, this method will downgrade it.
        This is a longer, but still relatively short interval (currently 24
        hours).

        Unlike `MetaStore.verify_by_mtime()`, this method considers all files in
        the Dmedia library, not just those on currently connected `FileStore`.

        Also see `MetaStore.downgrade_by_verified()`.
        """
        return self._downgrade_by_view(
            curtime, DOWNGRADE_BY_MTIME, 'downgrade-by-mtime'
        )

    def downgrade_by_verified(self, curtime):
        """
        Downgrade copies with 'verified' older than `DOWNGRADE_BY_VERIFIED`.

        This method only downgrades copies that meet all these criteria:

            1. The copy was previously verified (ie, 'verified' is present and
               is a positive integer)

            2. The copy isn't already downgraded (ie, copies != 0)

            3. The copy 'verified' is older than `DOWNGRADE_BY_VERIFIED`

        After a copy has been verified, there is a grace period of
        `VERIFY_BY_VERIFIED` seconds before it will be verified again.  This is
        to prevent an endless cycle of verification constantly running at peak
        read throughput.  This is a long interval (currently 6 days).

        When `MetaStore.verify_by_verified()` is periodically called for each
        connected `FileStore`, it will verify any copies whose 'verified' is
        older than `VERIFY_BY_VERIFIED`.

        However, if that hasn't happened by the time a copy 'verified' reaches
        `DOWNGRADE_BY_VERIFIED` seconds in age, this method will downgrade it.
        This is an even longer interval (currently 12 days).

        Unlike `MetaStore.verify_by_verified()`, this method considers all files
        in the Dmedia library, not just those on currently connected `FileStore`.

        Also see `MetaStore.downgrade_by_mtime()`.
        """
        return self._downgrade_by_view(
            curtime, DOWNGRADE_BY_VERIFIED, 'downgrade-by-verified'
        )

    def _downgrade_by_view(self, curtime, threshold, view):
        assert isinstance(curtime, int) and curtime >= 0
        assert isinstance(threshold, int) and threshold >= 0
        assert threshold in (DOWNGRADE_BY_MTIME, DOWNGRADE_BY_VERIFIED)
        assert view in ('downgrade-by-mtime', 'downgrade-by-verified')
        endkey = curtime - threshold
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
                log.exception('Conflict in %r', view)
                count -= len(e.conflicts)
        if count > 0:
            t.log('%s %d files', view, count)
        return count

    def purge_or_downgrade_by_store_atime(self, curtime):
        assert isinstance(curtime, int) and curtime >= 0
        purge_threshold = curtime - PURGE_BY_STORE_ATIME
        downgrade_threshold = curtime - DOWNGRADE_BY_STORE_ATIME
        assert purge_threshold < downgrade_threshold
        result = {}
        for store_id in self.iter_stores():
            try:
                doc = self.db.get(store_id)
                atime = doc.get('atime')
                if not isinstance(atime, int):
                    atime = 0
            except NotFound:
                log.warning('doc NotFound for store %s', store_id)
                atime = 0
            if atime <= purge_threshold:
                result[store_id] = ('purge', self.purge_store(store_id))
            elif atime <= downgrade_threshold:
                result[store_id] = ('downgrade', self.downgrade_store(store_id))
            else:
                log.info('store %s okay at atime %s', store_id, atime)
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
                value = get_dict(get_dict(doc, 'stored'), store_id)
                value['copies'] = 0
                value.pop('verified', None)
            count += len(docs)
            try:
                self.db.save_many(docs)
            except BulkConflict as e:
                log.exception('Conflict downgrading %s', store_id)
                count -= len(e.conflicts)
        if count > 0:
            t.log('downgrade %d copies in %s', count, store_id)
            self.log_store_downgrade(time.time(), store_id, count)
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
                get_dict(doc, 'stored').pop(store_id, None)
            count += len(docs)
            try:
                self.db.save_many(docs)
            except BulkConflict:
                log.exception('Conflict purging %s', store_id)
                count -= len(e.conflicts)
        try:
            doc = self.db.get(store_id)
            log.info('Deleting: %s', dumps(doc, True))
            self.db.update(mark_deleted, doc)
        except NotFound:
            pass
        if count > 0:
            t.log('purge %d copies in %s', count, store_id)
            self.log_store_purge(time.time(), store_id, count)
        return count

    def purge_all(self):
        """
        Purge every file in every store.

        Note: this is only really useful for testing.
        """
        t = TimeDelta()
        count = 0
        stores = tuple(self.iter_stores())
        for store_id in stores:
            count += self.purge_store(store_id)
        t.log('purge %d total copies in %d stores', count, len(stores))
        return count

    def scan(self, fs):
        """
        Make sure files we expect to be in the file-store *fs* actually are.

        A fundamental design tenet of Dmedia is that it doesn't particularly
        trust its metadata, and instead does frequent reality checks.  This
        allows Dmedia to work even though removable storage is often offline,
        meaning the overall Dmedia library is often in a network-partitioned
        state even when all the peers in the library might be online.

        Dmedia deals with removable storage via a quickly decaying confidence
        in its metadata.  If a removable drive hasn't been connected longer
        than some threshold, Dmedia will update all those copies to count for
        zero durability.

        Whenever a removable drive (or any drive for that matter) is connected,
        Dmedia immediately checks to see what files are actually on the drive,
        and whether they have good integrity.

        `MetaStore.scan()` is the most important reality check that Dmedia does
        because it's fast and can therefor be done frequently. Thousands of
        files can be scanned in a few seconds.

        The scan insures that for every file expected in this file-store, the
        file exists, has the correct size, and the expected mtime.

        If the file doesn't exist in this file-store, its store_id is deleted
        from doc['stored'] and the doc is saved.

        If the file has the wrong size, it's moved into the corrupt location in
        the file-store. Then the doc is updated accordingly marking the file as
        being corrupt in this file-store, and the doc is saved.

        If the file doesn't have the expected mtime in this file-store, this
        copy gets downgraded to zero copies worth of durability, and the last
        verification timestamp is deleted, if present.  This will put the file
        first in line for full content-hash verification.  If the verification
        passes, the durability will be raised back to the appropriate number of
        copies (although note this is done by `MetaStore.verify_by_downgraded()`,
        not by this method).

        :param fs: a `FileStore` instance
        """
        t = TimeDelta()
        count = 0
        kw = {
            'key': fs.id,
            'include_docs': True,
            'limit': 50,
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
        except NotFound:
            doc = deepcopy(fs.doc)
        doc['atime'] = int(time.time())
        doc['bytes_avail'] = fs.statvfs().avail
        self.db.save(doc)
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
                    log.warning('Orphan %r in %r', st.id, fs)
                    continue
                stored = get_dict(doc, 'stored')
                if fs.id in stored:
                    continue
                log.info('Relinking %s in %r', st.id, fs)
                new = {
                    fs.id: {'copies': 0, 'mtime': int(st.mtime)}
                }
                self.db.update(mark_added, doc, new)
                count += 1
        t.log('relink %d files in %r', count, fs)
        return count

    def remove(self, fs, doc_or_id):
        """
        Remove a file from `FileStore` *fs*.
        """
        (doc, _id) = self.doc_and_id(doc_or_id)
        doc = self.db.update(mark_removed, doc, fs.id)
        fs.remove(_id)
        return doc

    def copy(self, fs, doc_or_id, *dst_fs):
        """
        Copy a file from `FileStore` *fs* to one or more *dst_fs*.
        """
        (doc, _id) = self.doc_and_id(doc_or_id)
        try:
            fs.copy(_id, *dst_fs)
            log.info('Copied %s from %r to %r', _id, fs, list(dst_fs))
            new = create_stored(_id, fs, *dst_fs)
            return self.db.update(mark_copied, doc, time.time(), fs.id, new)
        except FileNotFound:
            log.warning('%s is not in %r', _id, fs)
            return self.db.update(mark_removed, doc, fs.id)
        except CorruptFile:
            log.error('%s is corrupt in %r', _id, fs)
            timestamp = time.time()
            self.log_file_corrupt(timestamp, fs, _id)
            return self.db.update(mark_corrupt, doc, timestamp, fs.id)

    def verify(self, fs, doc_or_id):
        """
        Verify a file in `FileStore` *fs*.
        """
        (doc, _id) = self.doc_and_id(doc_or_id)
        try:
            fs.verify(_id)
            log.info('Verified %s in %r', _id, fs)
            value = create_stored_value(_id, fs, time.time())
            return self.db.update(mark_verified, doc, fs.id, value)
        except FileNotFound:
            log.warning('%s is not in %r', _id, fs)
            return self.db.update(mark_removed, doc, fs.id)
        except CorruptFile:
            log.error('%s is corrupt in %r', _id, fs)
            timestamp = time.time()
            self.log_file_corrupt(timestamp, fs, _id)
            return self.db.update(mark_corrupt, doc, timestamp, fs.id)

    def verify_by_downgraded(self, fs):
        """
        Verify all downgraded files in FileStore *fs*.
        """
        count = 0
        size = 0
        t = TimeDelta()
        kw = {
            'key': fs.id,
            'limit': 1,
            'include_docs': True,
        }
        while True:
            rows = self.db.view('file', 'store-downgraded', **kw)['rows']
            if not rows:
                break
            doc = rows[0]['doc']
            self.verify(fs, doc)
            count += 1
            size += doc['bytes']
        if count:
            t.log('verify (by downgraded) %s in %r [%s]',
                    count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def verify_by_mtime(self, fs, curtime=None):
        """
        Verify files never verified whose "mtime" is older than 6 hours.
        """
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        count = 0
        size = 0
        t = TimeDelta()
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
        if count:
            t.log('verify (by mtime) %s in %r [%s]',
                    count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def verify_by_verified(self, fs, curtime=None):
        """
        Verify files whose "verified" timestamp is older than 2 weeks.
        """
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        count = 0
        size = 0
        t = TimeDelta()
        kw = {
            'startkey': [fs.id, None],
            'endkey': [fs.id, curtime - VERIFY_BY_VERIFIED],
            'limit': 1,
            'include_docs': True,
        }
        while True:
            rows = self.db.view('file', 'store-verified', **kw)['rows']
            if not rows:
                break
            doc = rows[0]['doc']
            self.verify(fs, doc)
            count += 1
            size += doc['bytes']
        if count:
            t.log('verify (by verified) %s in %r [%s]',
                    count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def verify_all(self, fs, curtime=None):
        if curtime is None:
            curtime = int(time.time())
        assert isinstance(curtime, int) and curtime >= 0
        log.info('Verifying files in %r as of %d...', fs, curtime)
        t = TimeDelta()
        (c1, s1) = self.verify_by_downgraded(fs)
        (c2, s2) = self.verify_by_mtime(fs, curtime)
        (c3, s3) = self.verify_by_verified(fs, curtime)
        count = c1 + c2 + c3
        size = s1 + s2 + s3
        t.log('verify %s in %r [%s]',
                count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def finish_download(self, fs, doc, tmp_fp):
        log.info('Finishing download of %s in %r', doc['_id'], fs)
        fs.move_to_canonical(tmp_fp, doc['_id'])
        new = create_stored(doc['_id'], fs)
        return self.db.update(mark_added, doc, new)

    def iter_files_at_rank(self, rank):
        if not isinstance(rank, int):
            raise TypeError(TYPE_ERROR.format('rank', int, type(rank), rank))
        if not (0 <= rank <= 5):
            raise ValueError('Need 0 <= rank <= 5; got {}'.format(rank))
        LIMIT = 25
        kw = {
            'limit': LIMIT,
            'key': rank,
        }
        while True:
            rows = self.db.view('file', 'rank', **kw)['rows']
            if not rows:
                break
            log.info('Considering %d files at rank=%d starting at %s',
                len(rows), rank, rows[0]['id']
            )
            ids = [r['id'] for r in rows]
            if rows[0]['id'] == kw.get('startkey_docid'):
                ids.pop(0)
            random.shuffle(ids)
            for _id in ids:
                try:
                    yield self.db.get(_id)
                except NotFound:
                    log.warning('doc NotFound for %s at rank=%d', _id, rank)
            if len(rows) < LIMIT:
                break
            kw['startkey_docid'] = rows[-1]['id']

    def iter_fragile_files(self):
        for rank in range(6):
            for doc in self.iter_files_at_rank(rank):
                doc_rank = get_rank(doc)
                if doc_rank <= rank:
                    yield doc
                else:
                    log.info('Now at rank %d > %d, skipping %s',
                        doc_rank, rank, doc['_id']
                    )

    def wait_for_fragile(self, last_seq):
        kw = {
            'feed': 'longpoll',
            'include_docs': True,
            'filter': 'file/fragile',
            'since': last_seq,
        }
        while True:
            try:
                return self.db.get('_changes', **kw)
            # FIXME: Sometimes we get a 400 Bad Request from CouchDB, perhaps
            # when `since` gets ahead of the `update_seq` as viewed by the
            # changes feed?  By excepting `BadRequest` here, we prevent the
            # vigilence process from sometimes crashing once it enters the event
            # phase.  This seems to happen only during a fairly high DB load
            # when multiple peers are syncing.
            #
            # Note that even without this we're generally still pretty safe as
            # the vigilence process gets restarted every 29 minutes anyway, in
            # order to minimize the impact of unexpected crashes or hangs. 
            except (ResponseNotReady, BadRequest):
                pass

    def iter_fragile(self, monitor=False):
        """
        Yield doc for each fragile file.     
        """
        for doc in self.iter_fragile_files():
            yield doc
        if not monitor:
            return

        # Now we enter an event-based loop using the _changes feed:
        update_seq = self.db.get()['update_seq']
        kw = {
            'feed': 'longpoll',
            'include_docs': True,
            'filter': 'file/fragile',
            'since': update_seq,
        }
        while True:
            try:
                result = self.db.get('_changes', **kw)
                for row in result['results']:
                    yield row['doc']
                kw['since'] = result['last_seq']
            # FIXME: Sometimes we get a 400 Bad Request from CouchDB, perhaps
            # when `since` gets ahead of the `update_seq` as viewed by the
            # changes feed?  By excepting `BadRequest` here, we prevent the
            # vigilence process from sometimes crashing once it enters the event
            # phase.  This seems to happen only during a fairly high DB load
            # when multiple peers are syncing.
            #
            # Note that even without this we're generally still pretty safe as
            # the vigilence process gets restarted every 29 minutes anyway, in
            # order to minimize the impact of unexpected crashes or hangs. 
            except (ResponseNotReady, BadRequest):
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
            if count > 0:
                t.log('reclaim %s in %d filestores', count_and_size(count, size), n)
            return (count, size, n)
        except Exception:
            log.exception('error in MetaStore.reclaim_all():')
            raise

