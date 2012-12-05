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
Doodle.
"""

import time
import os
import logging

from filestore import CorruptFile, FileNotFound, check_root_hash
from microfiber import NotFound, Conflict, id_slice_iter

from .util import get_db


log = logging.getLogger()


class MTimeMismatch(Exception):
    pass


class UpdateConflict(Exception):
    pass


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
    value = d.get(key)
    if isinstance(value, dict):
        return value
    d[key] = {}
    return d[key]


def update_doc(db, _id, func, *args):
    for retry in range(2):
        doc = db.get(_id)
        func(doc, *args)
        try:
            db.save(doc)
            return doc
        except Conflict:
            pass
    raise UpdateConflict()


def create_stored(_id, *filestores):
    """
    Create doc['stored'] for file with *_id* stored in *filestores*.
    """
    return dict(
        (
            fs.id,
            {
                'copies': fs.copies,
                'mtime': fs.stat(_id).mtime,
            }
        )
        for fs in filestores
    )


def merge_stored(old, new):
    """
    Update doc['stored'] based on new storage information in *new*.
    """
    for (key, value) in new.items():
        assert set(value) == set(['copies', 'mtime'])
        if key in old:
            old[key].update(value)
            old[key].pop('verified', None)
        else:
            old[key] = value 


def update(d, key, new):
    old = get_dict(d, key)
    old.update(new)


def add_to_stores(doc, *filestores):
    _id = doc['_id']
    stored = get_dict(doc, 'stored')
    for fs in filestores:
        new = {
            'copies': fs.copies,
            'mtime': fs.stat(_id).mtime,
            'verified': 0,
        }
        update(stored, fs.id, new)


def remove_from_stores(doc, *filestores):
    stored = get_dict(doc, 'stored')
    for fs in filestores:
        try:
            del stored[fs.id]
        except KeyError:
            pass


def mark_verified(doc, fs, timestamp):
    _id = doc['_id']
    stored = get_dict(doc, 'stored')
    new = {
        'copies': fs.copies,
        'mtime': fs.stat(_id).mtime,
        'verified': int(timestamp),
    }
    update(stored, fs.id, new)


def mark_corrupt(doc, fs, timestamp):
    stored = get_dict(doc, 'stored')
    try:
        del stored[fs.id]
    except KeyError:
        pass
    corrupt = get_dict(doc, 'corrupt')
    corrupt[fs.id] = {'time': timestamp}


def mark_mismatch(doc, fs):
    """
    Update mtime and copies, delete verified, preserve pinned.
    """
    _id = doc['_id']
    stored = get_dict(doc, 'stored')
    value = get_dict(stored, fs.id)
    value.update(
        mtime=fs.stat(_id).mtime,
        copies=0,
    )
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
            mark_verified(self.doc, self.fs, time.time())
        elif issubclass(exc_type, CorruptFile):
            mark_corrupt(self.doc, self.fs, time.time())
        elif issubclass(exc_type, FileNotFound):
            remove_from_stores(self.doc, self.fs)
        self.db.save(self.doc)


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
            remove_from_stores(self.doc, self.fs)
        elif issubclass(exc_type, CorruptFile):
            log.warning('%s has wrong size in %r', self.doc['_id'], self.fs)
            mark_corrupt(self.doc, self.fs, time.time())
        elif issubclass(exc_type, MTimeMismatch):
            log.warning('%s has wrong mtime in %r', self.doc['_id'], self.fs)
            mark_mismatch(self.doc, self.fs)
        else:
            return False
        self.db.save(self.doc)
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


class MetaStore:
    def __init__(self, db):
        self.db = db

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.db)

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
        start = time.time()
        log.info('Scanning FileStore %s at %r', fs.id, fs.parentdir)
        rows = self.db.view('file', 'stored', key=fs.id)['rows']
        for ids in id_slice_iter(rows):
            for doc in self.db.get_many(ids):
                _id = doc['_id']
                with ScanContext(self.db, fs, doc):
                    st = fs.stat(_id)
                    if st.size != doc.get('bytes'):
                        src_fp = open(st.name, 'rb')
                        raise fs.move_to_corrupt(src_fp, _id,
                            file_size=doc['bytes'],
                            bad_file_size=st.size,
                        )
                    stored = get_dict(doc, 'stored')
                    s = get_dict(stored, fs.id)
                    if st.mtime != s['mtime']:
                        raise MTimeMismatch()
        # Update the atime for the dmedia/store doc
        try:
            doc = self.db.get(fs.id)
            assert doc['type'] == 'dmedia/store'
            doc['atime'] = int(time.time())
            self.db.save(doc)
        except NotFound:
            log.warning('No doc for FileStore %s', fs.id)
        count = len(rows)
        log.info('%.3f to scan %r files in %r', time.time() - start, count, fs)
        return count

    def relink(self, fs):
        """
        Find known files that we didn't expect in `FileStore` *fs*.
        """
        start = time.time()
        log.info('Relinking FileStore %r at %r', fs.id, fs.parentdir)
        for buf in relink_iter(fs):
            docs = self.db.get_many([st.id for st in buf])
            for (st, doc) in zip(buf, docs):
                if doc is None:
                    continue
                stored = get_dict(doc, 'stored')
                value = get_dict(stored, fs.id)
                if value:
                    continue
                log.info('Relinking %s in %r', st.id, fs)
                value.update(
                    mtime=st.mtime,
                    copies=fs.copies,
                )
                self.db.save(doc)
        log.info('%.3f to relink %r', time.time() - start, fs)

    def remove(self, fs, _id):
        doc = self.db.get(_id)
        remove_from_stores(doc, fs)
        self.db.save(doc)
        fs.remove(_id)  
        return doc

    def verify(self, fs, _id, return_fp=False):
        doc = self.db.get(_id)
        with VerifyContext(self.db, fs, doc):
            return fs.verify(_id, return_fp)

    def verify_iter(self, fs, _id):
        doc = self.db.get(_id)
        file_size = doc['bytes']
        (content_type, leaf_hashes) = self.db.get_att(_id, 'leaf_hashes')
        with VerifyContext(self.db, fs, doc):
            for leaf in fs.verify_iter(_id, file_size, leaf_hashes):
                yield leaf

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
        new = {'mtime': os.fstat(tmp_fp.fileno()).st_mtime}
        update(partial, fs.id, new)
        self.db.save(doc)
        return tmp_fp

    def verify_and_move(self, fs, tmp_fp, _id):
        doc = self.db.get(_id)
        ch = fs.verify_and_move(tmp_fp, _id)
        partial = get_dict(doc, 'partial')
        try:
            del partial[fs.id]
        except KeyError:
            pass
        add_to_stores(doc, fs)
        self.db.save(doc)
        return ch

    def copy(self, src_filestore, _id, *dst_filestores):
        doc = self.db.get(_id)
        with VerifyContext(self.db, src_filestore, doc):
            ch = src_filestore.copy(_id, *dst_filestores)
            add_to_stores(doc, *dst_filestores)
            return ch
        
