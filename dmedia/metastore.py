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

from filestore import CorruptFile, FileNotFound

from .util import get_db


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


def mark_verified(doc, fs, timestamp=None):
    timestamp = (time.time() if timestamp is None else timestamp)
    _id = doc['_id']
    stored = get_dict(doc, 'stored')
    new = {
        'copies': fs.copies,
        'mtime': fs.stat(_id).mtime,
        'verified': timestamp,
    }
    update(stored, fs.id, new)


def mark_corrupt(doc, fs, timestamp=None):
    timestamp = (time.time() if timestamp is None else timestamp)
    stored = get_dict(doc, 'stored')
    try:
        del stored[fs.id]
    except KeyError:
        pass
    corrupt = get_dict(doc, 'corrupt')
    corrupt[fs.id] = {'time': timestamp}


class MetaStore:
    def __init__(self, env):
        self.db = get_db(env)

    def remove(self, fs, _id):
        doc = self.db.get(_id)
        try:
            fs.remove(_id)
        finally:
            remove_from_stores(doc, fs)
            self.db.save(doc)

    def verify(self, fs, _id, return_fp=False):
        doc = self.db.get(_id)
        try:
            ret = fs.verify(_id, return_fp)
            mark_verified(doc, fs, time.time())
            return ret
        except CorruptFile as e:
            mark_corrupt(doc, fs, time.time())
            raise e
        except FileNotFound as e:
            remove_from_stores(doc, fs)
            raise e
        finally:
            self.db.save(doc)

    def content_md5(self, fs, _id, force=False):
        doc = self.db.get(_id)
        if not force:
            try:
                return doc['content_md5']
            except KeyError:
                pass
        try:
            (b16, b64) = fs.content_md5(_id)
            mark_verified(doc, fs, time.time())
            doc['content_md5'] = b64
            return b64
        except CorruptFile as e:
            mark_corrupt(doc, fs, time.time())
            raise e
        except FileNotFound as e:
            remove_from_stores(doc, fs)
            raise e
        finally:
            self.db.save(doc)

    def copy(self, src_filestore, _id, *dst_filestores):
        doc = self.db.get(_id)
        try:
            ch = src_filestore.copy(_id, *dst_filestores)
            mark_verified(doc, src_filestore, time.time())
            add_to_stores(doc, *dst_filestores)
            return ch
        except CorruptFile as e:
            mark_corrupt(doc, src_filestore, time.time())
            raise e
        except FileNotFound as e:
            remove_from_stores(doc, src_filestore)
            raise e
        finally:
            self.db.save(doc)
        
