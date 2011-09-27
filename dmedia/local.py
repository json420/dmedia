# dmedia: dmedia hashing protocol and file layout
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
A FileStore-like API that abstract the specific FileStore away.
"""

from random import Random

from filestore import check_id, check_root_hash, FileStore
import microfiber


class NoSuchFile(Exception):
    def __init__(self, _id):
        self.id = _id
        super().__init__(_id)


class FileNotLocal(Exception):
    def __init__(self, _id):
        self.id = _id
        super().__init__(_id)


class NotLocalStore(Exception):
    def __init__(self, store_id):
        self.id = store_id
        super().__init__(store_id)


def get_filestore(doc):
    return FileStore(doc['parentdir'], doc['_id'], doc.get('copies', 0))


def choose_local_store(doc, fast, slow):
    """
    Load balance across multiple local hard disks.

    The idiomatic dmedia use-case involves multiple `FileStore` located on
    different physical hard disks, where a copy of a file is likely stored once
    on each disk.

    So the trick is choosing which copy to read.  When choosing among a set of
    files that are store on multiple hard-drives, we want:

        1. The distribution for all files to be randomly (and roughly evenly)
           spread across the drives so we can utilize their IO in parallel

        2. For a given file ID to be consistently read from the same hard drive
           so we can take advantage of times when the file is already in the
           page cache

    And that's exactly what this function does:

    >>> doc = {
    ...     '_id': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    ...     'stored': {
    ...         'CCCCCCCCCCCCCCCCCCCCCCCC': {
    ...             'mtime': 1234567890,
    ...             'copies': 1,
    ...         },
    ...         'DDDDDDDDDDDDDDDDDDDDDDDD': {
    ...             'mtime': 1234567890,
    ...             'copies': 1,
    ...         },
    ...     },
    ... }
    >>> fast = set(['CCCCCCCCCCCCCCCCCCCCCCCC', 'DDDDDDDDDDDDDDDDDDDDDDDD'])
    >>> slow = set()
    >>> choose_local_store(doc, fast, slow)
    'DDDDDDDDDDDDDDDDDDDDDDDD'
    >>> choose_local_store(doc, fast, slow)
    'DDDDDDDDDDDDDDDDDDDDDDDD'
    
    And now with a different doc ID, to see that it to gets a stable choice:
    
    >>> doc['_id'] = 'BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
    >>> choose_local_store(doc, fast, slow)
    'CCCCCCCCCCCCCCCCCCCCCCCC'
    >>> choose_local_store(doc, fast, slow)
    'CCCCCCCCCCCCCCCCCCCCCCCC'

    """
    stored = set(doc['stored'])
    local = fast.intersection(stored) or slow.intersection(stored)
    if not local:
        raise FileNotLocal(doc['_id'])
    if len(local) == 1:
        return local.pop()
    return Random(doc['_id']).choice(sorted(local))


class LocalMaster:
    def __init__(self, env):
        self.db = microfiber.Database('dmedia', env)
        self.db.ensure()
        try:
            self.local = {'_id': '_local/stores', 'stores': {}}
            self.db.save(self.stores)
        except microfiber.Conflict:
            self.local = self.db.get('_local/stores')
        self._ids = {}
        self._parentdirs = {}
        self.fast = set()

    def add(self, parentdir, _id, copies, fast=True):
        fs = self.init(parentdir, _id, copies, fast)
        self.local['stores'][parentdir] = {
            'id': _id,
            'copies': copies,
            'fast': fast,
        }
        self.db.save(self.local)
        return fs

    def init(self, parentdir, _id, copies, fast=True):
        fs = FileStore(parentdir, _id, copies)
        self._ids[fs.id] = fs
        self._parentdirs[fs.parentdir] = fs
        if fast:
            self.fast.add(fs.id)
        return fs

    def destroy(self, fs):
        del self._ids[fs.id]
        del self._parentdirs[fs.parentdir]
        try:
            self.fast.remove(fs.id)
        except KeyError:
            pass    


class LocalStores:
    __slots__ = ('ids', 'parentdirs', 'fast', 'slow')

    def __init__(self):
        self.ids = {}
        self.parentdirs = {}
        self.fast = set()
        self.slow = set()

    def add(self, fs, fast=True):
        if fs.id in self.ids:
            raise Exception('already have ID {!r}'.format(fs.id))
        if fs.parentdir in self.parentdirs:
            raise Exception('already have parentdir {!r}'.format(fs.parentdir))
        self.ids[fs.id] = fs
        self.parentdirs[fs.parentdir] = fs
        speed = (self.fast if fast else self.slow)
        speed.add(fs.id)
        assert not self.fast.intersection(self.slow)
        assert set(self.ids) == self.fast.union(self.slow)

    def remove(self, fs):
        del self.ids[fs.id]
        del self.parentdirs[fs.parentdir]
        for speed in (self.fast, self.slow):
            try:
                speed.remove(fs.id)
            except KeyError:
                pass
        assert not self.fast.intersection(self.slow)
        assert set(self.ids) == self.fast.union(self.slow)

    def choose_local_store(self, doc):
        store_id = choose_local_store(doc, self.fast, self.slow)
        return self.ids[store_id]


class Stores:
    def __init__(self, env):
        self.db = microfiber.Database('dmedia', env)
        self.db.ensure()

    def get_local(self):
        try:
            docs = self.db.get('_local/stores')['stores']
        except microfiber.NotFound:
            return ({}, set(), set())
        stores = dict(
            (doc['_id'], doc) for doc in docs
        )
        internal = set()
        removable = set()
        for doc in docs:
            assert doc['plugin'] in ('filestore', 'filestore.removable')
            if doc['plugin'] == 'filestore.removable':
                removable.add(doc['_id'])
            else:
                internal.add(doc['_id'])
        return (stores, internal, removable)

    def get_doc(self, _id):
        check_id(_id)
        try:
            return self.db.get(_id)
        except microfiber.NotFound:
            raise NoSuchFile(_id)

    def content_hash(self, _id, unpack=True):
        doc = self.get_doc(_id)
        leaf_hashes = self.db.get_att(_id, 'leaf_hashes')[1]
        return check_root_hash(_id, doc['bytes'], leaf_hashes, unpack)

    def path(self, _id):
        doc = self.get_doc(_id)
        (stores, internal, removable) = self.get_local()
        store_id = choose_local_store(doc, internal, removable)





