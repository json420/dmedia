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

from filestore import FileStore, CorruptFile, FileNotFound


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
    def verify(self, _id, fs, return_fp=False):
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

        
        
            
            
        
