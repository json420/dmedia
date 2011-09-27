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


class Stores:
    def __init__(self, env):
        self.db = microfiber.Database('dmedia', env)

    def get_store_id(self, doc):
        local = set(self.stores).intersection(doc['stored'])
        if not local:
            raise FileNotLocal(doc['_id'])
        if len(local) == 1:
            return local.pop()
        return Random(doc['_id']).choice(sorted(local))

    def get_doc(self, _id):
        check_id(_id)
        try:
            return self.db.get(_id)
        except microfiber.NotFound:
            raise NoSuchFile(_id)

    def get_filestore(self, store_id):
        try:
            return self._stores[store_id]
        except KeyError:
            raise NotLocalStore(store_id)

    def content_hash(self, _id):
        try:
            doc = self.db.get(_id)
        except microfiber.NotFound:
            raise NoSuchFile(_id)

    def path(self, _id):
        doc = self.get_doc(_id)
        local = self._local.intersection(doc['stored'])
        if not local:
            raise FileNotLocal(_id)





