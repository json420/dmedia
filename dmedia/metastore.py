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
    value = d.get(key)
    if isinstance(value, dict):
        return value
    d[key] = {}
    return d[key]


def update(d, key, new):
    old = get_dict(d, key)
    old.update(new)


def mark_verified(doc, fs):
    _id = doc['_id']
    stored = get_dict(doc, 'stored')
    new = {
        'copies': fs.copies,
        'mtime': fs.stat(_id).mtime,
        'verified': time.time(),
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


def add_to_stores(doc, *filestores):
    _id = doc['_id']
    stored = ensure_dict(doc, 'stored')
    for fs in filestores:
        new = {
            'copies': fs.copies,
            'mtime': fs.stat(_id).mtime,
            'verified': 0,
        }
        update(stored, fs.id, new)


class MetaStore:
    def verify(self, _id, fs, return_fp=False):
        self.db.head(_id)
        try:
            ret = fs.verify(_id, return_fp)
            doc = self.db.get(_id)
            mark_verified(doc, 
            self.db.save(doc)
            return ret
        except CorruptFile as e:
            try:
                del doc['stored'][fs.id]
            except KeyError:
                pass
            corrupt = doc.get('corrupt', {})
            
            self.db.save(doc)

        
        
            
            
        
