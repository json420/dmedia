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


def get_store_id(doc, internal, removable=frozenset()):
    stored = set(doc['stored'])
    local = (
        set(internal).intersection(stored)
        or set(removable).intersection(stored)
    )
    if not local:
        raise FileNotLocal(doc['_id'])
    if len(local) == 1:
        return local.pop()
    return Random(doc['_id']).choice(sorted(local))


class LocalStores:
    def __init__(self):
        self._id_map = {}
        self._parentdir_map = {}
        self._internal = set()
        self._removable = set()

    def add(self, doc):
        fs = get_filestore(doc)
        self._id_map[fs.id] = fs
        self._parentdir_map[fs.parentdir] = fs
        plugin = doc['plugin']
        assert plugin in ('filestore.internal', 'filestore.removable')
        if plugin == 'filestore.internal':
            self._internal.add(fs.id)
        else:
            self._removable.add(fs.id)
        return fs
        
    def by_id(self, _id):
        return self._id_map[_id]

    def by_parentdir(self, parentdir):
        return self._parentdir_map[parentdir]



class Stores:
    def __init__(self, env):
        self.db = microfiber.Database('dmedia', env)
        self.db.ensure()

    def get_local(self):
        try:
            docs = self.db.get('_local/stores').get('stores', [])
        except microfiber.NotFound:
            return ({}, set(), set())
        stores = dict(
            (doc['_id'], doc) for doc in docs
        )
        internal = set()
        removable = set()
        for doc in docs:
            assert doc['plugin'] in ('filestore.internal', 'filestore.removable')
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
        stores = self.db.get('_local/stores')
        store_id = get_store_id(doc, stores['internal'], stores['removable'])
        if store_id in stores['internal']:
            return get_filestore(stores['internal'])





