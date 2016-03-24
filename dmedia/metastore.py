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
import logging
from random import SystemRandom
from copy import deepcopy

from dbase32 import log_id, isdb32
from filestore import FileStore, CorruptFile, FileNotFound, check_root_hash
from microfiber import NotFound, BadRequest, BulkConflict, id_slice_iter, dumps

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

GB = 1000000000
MIN_BYTES_FREE =  4 * GB
MAX_BYTES_FREE = 64 * GB


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

    The rank of a file is the number of physical drives its assumed to be stored
    upon plus the sum of the assumed durability of those copies, basically::

        rank = len(doc['stored']) + sum(v['copies'] for v in doc['stored'].values())

    However, this function can cope with an arbitrarily broken *doc*, as long as
    *doc* is at least a ``dict`` instance.  For example:

    >>> doc = {
    ...     'stored': {
    ...         '333333333333333333333333': {'copies': 1},
    ...         '999999999999999999999999': {'copies': -6},
    ...         'AAAAAAAAAAAAAAAAAAAAAAAA': 'junk',
    ...         'YYYYYYYYYYYYYYYY': 'store_id too short',
    ...         42: 'the ultimate key to the ultimate value',
    ...     },
    ... }
    >>> get_rank(doc)
    4

    Any needed schema coercion is done in-place:

    >>> doc == {
    ...     'stored': {
    ...         '333333333333333333333333': {'copies': 1},
    ...         '999999999999999999999999': {'copies': 0},
    ...         'AAAAAAAAAAAAAAAAAAAAAAAA': {'copies': 0},
    ...     },
    ... }
    True

    It even works with an empty doc:

    >>> doc = {}
    >>> get_rank(doc)
    0
    >>> doc
    {'stored': {}}

    The rank of a file is used to order (prioritize) the copy increasing
    behavior, which is done from lowest rank to highest rank (from most fragile
    to least fragile).

    Also see the "file/rank" CouchDB view function in `dmedia.views`.
    """
    stored = get_dict(doc, 'stored')
    copies = 0
    for key in tuple(stored):
        if isinstance(key, str) and len(key) == 24 and isdb32(key):
            value = get_dict(stored, key)
            copies += get_int(value, 'copies')
        else:
            del stored[key]
    return min(3, len(stored)) + min(3, copies)


def get_copies(doc):
    """
    Calculate the durability of the file represented by *doc*.

    For example:

    >>> doc = {
    ...     'stored': {
    ...         '333333333333333333333333': {'copies': 1},
    ...         '999999999999999999999999': {'copies': -6},
    ...         'AAAAAAAAAAAAAAAAAAAAAAAA': 'junk',
    ...         'YYYYYYYYYYYYYYYY': 'store_id too short',
    ...         42: 'the ultimate key to the ultimate value',
    ...     },
    ... }
    >>> get_copies(doc)
    1

    Any needed schema coercion is done in-place:

    >>> doc == {
    ...     'stored': {
    ...         '333333333333333333333333': {'copies': 1},
    ...         '999999999999999999999999': {'copies': 0},
    ...         'AAAAAAAAAAAAAAAAAAAAAAAA': {'copies': 0},
    ...     },
    ... }
    True

    It even works with an empty doc:

    >>> doc = {}
    >>> get_copies(doc)
    0
    >>> doc
    {'stored': {}}

    """
    stored = get_dict(doc, 'stored')
    copies = 0
    for key in tuple(stored):
        if isinstance(key, str) and len(key) == 24 and isdb32(key):
            value = get_dict(stored, key)
            copies += get_int(value, 'copies')
        else:
            del stored[key]
    return copies


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


def update_store(doc, timestamp, bytes_avail):
    """
    Used by `MetaStore.scan()` to update the dmedia/store doc.
    """
    doc['atime'] = int(timestamp)
    doc['bytes_avail'] = bytes_avail


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

    def content_hash(self, doc, unpack=True):
        if not isinstance(doc, dict):
            raise TypeError(TYPE_ERROR.format('doc', dict, type(doc), doc))
        _id = doc['_id']
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

        Note: this is only really useful for testing.  It can be triggered from
        the command line like this::

            dmedia-cli DowngradeAll
        """
        t = TimeDelta()
        file_count = 0
        copy_count = 0
        buf = BufferedSave(self.db, 50)
        for doc in self.db.iter_view('doc', 'type', 'dmedia/file'):
            stored = get_dict(doc, 'stored')
            for key in tuple(stored):
                if isinstance(key, str) and len(key) == 24 and isdb32(key):
                    value = get_dict(stored, key)
                    value['copies'] = 0
                    value.pop('verified', None)
                else:
                    del stored[key]
            buf.save(doc)
            file_count += 1
            copy_count += len(stored)
        buf.flush()
        t.log('downgrade all %d files (%d total copies)', file_count, copy_count)
        return (file_count, copy_count)

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
            except BulkConflict as e:
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

        Note: this is only really useful for testing.  It can be triggered from
        the command line like this::

            dmedia-cli PurgeAll
        """
        t = TimeDelta()
        file_count = 0
        buf = BufferedSave(self.db, 200)
        for doc in self.db.iter_view('doc', 'type', 'dmedia/file', 200):
            if doc.get('stored') != {}:
                doc['stored'] = {}
                buf.save(doc)
                file_count += 1
        buf.flush()
        t.log('purge all %d files', file_count)
        return file_count

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
        # Do the scan for all files in fs.id:        
        count = 0
        for doc in self.db.iter_view('file', 'stored', fs.id):
            _id = doc['_id']
            count += 1
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
                    self.db.update(mark_corrupt, doc, time.time(), fs.id)
                elif value.get('mtime') != int(st.mtime):
                    log.warning('%s has wrong mtime %r', _id, fs)
                    self.db.update(mark_mismatched, doc, fs.id, int(st.mtime))
            except FileNotFound:
                log.warning('%s is not in %r', _id, fs)
                self.db.update(mark_removed, doc, fs.id)
        # Update the atime for the dmedia/store doc
        try:
            doc = self.db.get(fs.id)
        except NotFound:
            doc = deepcopy(fs.doc)
        self.db.update(update_store, doc, time.time(), fs.statvfs().avail)
        t.log('scan %r files in %r', count, fs)
        return count

    def relink(self, fs):
        """
        Find known files that we didn't expect in `FileStore` *fs*.

        Dmedia periodically iterates through all the files on each connected
        drive looking for files that exist in the library, but whose current
        metadata doc does not indicate that the file in question is stored on
        the drive in question.

        When Dmedia finds such a file, it "re-links" the file by updating its
        doc to reflect that it exists on the drive in question.  However,
        Dmedia is pessimistic about the integrity of the file, so it updates the
        metadata with a confidence of {'copies': 0}.  This means that a newly
        re-linked file can only contribute 1 to the rank of a file (whereas
        were it in a verified state, it would contribute 2 to the rank).

        As a newly re-linked file is unverified, it will be verified the next
        time `MetaStore.verify_all()` is run on the `FileStore` in question.  If
        the file turns out to be corrupt, it will be marked as such and be
        removed from doc['stored'].  If the file integrity is good, the doc will
        be updated with a confidence of {'copies': 1}.  Likewise,
        `core.Vigilance` will do the same thing if it happens to process the
        re-linked file first.

        As this is a rank-increasing metadata update, we can play things a bit
        fast and loose when saving the docs to CouchDB: we use a `BufferedSave`
        instance to save the docs for 25 re-linked files at a time, and we
        ignore any conflicts.  If a conflict prevents a file from being
        re-linked, it will simply be re-linked the next time this method is run
        for the `FileStore` in question.  This is acceptable because in the case
        of a conflict, we're underestimating the rank of a file.

        In contrast, `BufferedSave` should *never* be used for rank-decreasing
        metadata updates.  Methods like `MetaStore.scan()` should always save
        their rank-decreasing metadata updates to CouchDB one document at a
        time, immediately after such a condition is found, and they should
        always save the document using `microfiber.Database.update()`, which
        will automatically re-try the update when there has been a conflicting
        change in the time between when a doc was retrieved and when it was
        saved.

        Using `BufferedSave` instead of `microfiber.Database.update()` makes
        this method is roughly 4x faster for real-world re-link scenarios (the
        test was to re-link 42,703 files stored on a 2TB mechanical HDD).  In
        the grand scheme of things, this helps Dmedia reach its equilibrium
        state more quickly, especially considering the scenarios under which
        files end up in an un-linked state.

        In normal usage, you can end up with un-linked files when multiple
        Dmedia peers in the same library are all making metadata updates for the
        same files at roughly the same time, which can result in a lot of
        conflicts by the time these changes are replicated to the other peers.

        Although `core.Vigilance.process_backlog()` randomizes the order in
        which fragile files are handled, which considerably reduces conflicts,
        `core.Vigilance.run_event_loop()` has no such mechanism.  So after a
        group a peers have all processed their backlog and are now running their
        event loop, importing a file (rank=2) on one peer means the other peers
        will all immediately try to download the new file from the first peer as
        soon as they pick up its corresponding document from the changes feed.
        If two peers download the same fragile file at the same time such a
        conflict is created, the winning document revision will only reflect one
        of these new file copies.  So one peer will have the file *physically*
        stored with a confidence of {'copies': 1} without this actually being
        reflected in the wining document revision.  The file is in an unlinked
        state, which wont be detected till the next time this method is run for
        the `FileStore` in question.

        An extreme demonstration case can be setup like this:

            1.  Setup Dmedia on four (or more) computers, all peered together,
                all starting with an empty Dmedia library

            2.  On one of the computers, import a large number of small files
                using something like:

                    dmedia-migrate /usr/share

        Small files are best for this as they stress the metadata layer more
        heavily (because the files themselves can be downloaded so quickly).
        Likewise, running this experiment on four computers is better at
        stressing the metadata layer because for every new file coming through
        the changes feed, Dmedia in total will typically be downloading one (or
        more) additional copies than needed to reach rank=6.

        In such a setup, you'll see Dmedia spending a lot of time relinking
        files it has already downloaded.  This creates a lot of needless load
        and means Dmedia is much slower at reaching its equilibrium point than
        it should be.

        As relinking is one of the key parts of the problem, changing this
        method to use `BufferedSave` should improve the situation.  The new
        approach has a number of advantages:

            1.  Relinking itself is faster, meaning the peers are updated more
                quickly about the current state of the library.

            2.  Batch saves reduce the load on CouchDB and also allow the
                replicator to batch these changes more efficiently, resulting in
                higher replication throughput.

            3.  Ignoring conflicts when relinking provides a nice load-shedding
                mechanism: when the rate of conflict is extremely high, its
                probably best to just ignore them when relinking instead of
                compounding the problem by injecting yet more changes; again
                it's only okay to do this here because this method makes
                rank-increasing updates... this approach is *never* okay for
                methods that make rank-decreasing updates.
        """
        t = TimeDelta()
        buf = BufferedSave(self.db)
        count = 0
        for group in relink_iter(fs):
            docs = self.db.get_many([st.id for st in group])
            for (st, doc) in zip(group, docs):
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
                mark_added(doc, new)
                buf.save(doc)
                count += 1
        buf.flush()
        t.log('relink %d files in %r', count, fs)
        return count

    def remove(self, fs, doc):
        """
        Remove a file from `FileStore` *fs*.
        """
        if not isinstance(doc, dict):
            raise TypeError(TYPE_ERROR.format('doc', dict, type(doc), doc))
        _id = doc['_id']
        doc = self.db.update(mark_removed, doc, fs.id)
        fs.remove(_id)
        return doc

    def copy(self, fs, doc, *dst_fs):
        """
        Copy a file from `FileStore` *fs* to one or more *dst_fs*.
        """
        if not isinstance(doc, dict):
            raise TypeError(TYPE_ERROR.format('doc', dict, type(doc), doc))
        _id = doc['_id']
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

    def verify(self, fs, doc):
        """
        Verify a file in `FileStore` *fs*.
        """
        if not isinstance(doc, dict):
            raise TypeError(TYPE_ERROR.format('doc', dict, type(doc), doc))
        _id = doc['_id']
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
            'limit': 17,
            'include_docs': True,
        }
        while True:
            rows = self.db.view('file', 'store-downgraded', **kw)['rows']
            if not rows:
                break
            for row in rows:
                doc = row['doc']
                self.verify(fs, doc)
                count += 1
                size += doc['bytes']
        if count:
            t.log('verify (by downgraded) %s in %r [%s]',
                    count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def _verify_by_view(self, fs, curtime, threshold, view):
        assert isinstance(curtime, int) and curtime >= 0
        count = 0
        size = 0
        t = TimeDelta()
        kw = {
            'startkey': [fs.id, None],
            'endkey': [fs.id, curtime - threshold],
            'limit': 17,
            'include_docs': True,
        }
        while True:
            rows = self.db.view('file', view, **kw)['rows']
            if not rows:
                break
            for row in rows:
                doc = row['doc']
                self.verify(fs, doc)
                count += 1
                size += doc['bytes']
        if count:
            t.log('verify (by %s) %s in %r [%s]', view,
                    count_and_size(count, size), fs, t.rate(size))
        return (count, size)

    def verify_by_mtime(self, fs, curtime):
        """
        Verify files never verified whose "mtime" is older than 6 hours.
        """
        return self._verify_by_view(
            fs, curtime, VERIFY_BY_MTIME, 'store-mtime'
        ) 

    def verify_by_verified(self, fs, curtime):
        """
        Verify files whose "verified" timestamp is older than 2 weeks.
        """
        return self._verify_by_view(
            fs, curtime, VERIFY_BY_VERIFIED, 'store-verified'
        )

    def verify_all(self, fs, curtime):
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
        LIMIT = 50
        kw = {
            'limit': LIMIT,
            'key': rank,
        }
        log.info('Considering files at rank=%d', rank)
        while True:
            rows = self.db.view('file', 'rank', **kw)['rows']
            if not rows:
                break
            ids = [r['id'] for r in rows]
            if ids[0] == kw.get('startkey_docid'):
                ids.pop(0)
            if not ids:
                break
            random.shuffle(ids)
            for _id in ids:
                try:
                    doc = self.db.get(_id)
                    doc_rank = get_rank(doc)
                    if doc_rank <= rank:
                        yield doc
                    else:
                        log.info('Now at rank %d > %d, skipping %s',
                            doc_rank, rank, doc.get('_id')
                        )
                except NotFound:
                    log.warning('doc NotFound for %s at rank=%d', _id, rank)
            if len(rows) < LIMIT:
                break
            kw['startkey_docid'] = rows[-1]['id']
            self.db.wait_for_compact()

    def iter_fragile_files(self, stop=6):
        if not isinstance(stop, int):
            raise TypeError(TYPE_ERROR.format('stop', int, type(stop), stop))
        if not (2 <= stop <= 6):
            raise ValueError('Need 2 <= stop <= 6; got {}'.format(stop))
        for rank in range(stop):
            for doc in self.iter_files_at_rank(rank):
                yield doc

    def wait_for_fragile_files(self, last_seq):
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
            except (OSError, BadRequest):
                pass

    def iter_preempt_files(self):
        kw = {
            'limit': 300,
            'descending': True,
        }
        rows = self.db.view('file', 'preempt', **kw)['rows']
        if not rows:
            return
        ids = [r['id'] for r in rows]
        log.info('Considering %d files for preemptive copy increasing', len(ids))
        random.shuffle(ids)
        for _id in ids:
            try:
                doc = self.db.get(_id)
                copies = get_copies(doc)
                if copies == 3:
                    yield doc
                else:
                    log.info('Now at copies=%d, skipping %s', copies, _id)
            except NotFound:
                log.warning('preempt doc NotFound for %s', _id)

    def reclaim(self, fs, threshold=MAX_BYTES_FREE):
        count = 0
        size = 0
        t = TimeDelta()
        while True:
            kw = {
                'startkey': [fs.id, None],
                'endkey': [fs.id, int(time.time())],
                'limit': 1,
                'include_docs': True,
            }
            rows = self.db.view('file', 'store-reclaimable', **kw)['rows']
            if not rows:
                break
            doc = rows[0]['doc']
            doc = self.remove(fs, doc)
            count += 1
            size += doc['bytes']
            if fs.statvfs().avail > threshold:
                break
        if count > 0:
            t.log('reclaim %s in %r', count_and_size(count, size), fs)
        return (count, size)

    def reclaim_all(self, threshold=MAX_BYTES_FREE):
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

