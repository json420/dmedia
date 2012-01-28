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


from filestore import FileStore, CorruptFile, FileNotFound


def ensure_dict(doc, key):
    d = doc.get(key)
    if not isinstance(d, dict):
        doc[key] = {}
        return doc[key]
    return d


def update(d, key, new):
    old = ensure_dict(d, key)
    old.update(new)


def mark_verified(doc, fs):
    _id = doc['_id']
    s = {
        'copies': fs.copies,
        'mtime': fs.stat(_id).mtime,
        'verified': time.time(),
    }
    stored = ensure_dict(doc, 'stored')
    update(stored, fs.id, s)
        
        
def add_stores(doc, filestores):
    _id = doc['_id']
    s = {
        'copies': fs.copies,
        'mtime': fs.stat(_id).mtime,
        'verified': time.time(),
    }
    stored = ensure_dict(doc, 'stored')
    if fs.id in stored:
        stored[fs.id].update(s)
    else:
        stored[fs.id] = s
    


class MetaStore:
    def verify(self, _id, fs, return_fp=False):
        doc = self.db.get(_id)
        try:
            ret = fs.verify(_id, return_fp)
            s = {
                'copies': fs.copies,
                'mtime': fs.stat(_id).mtime,
                'verified': time.time(),
            }
            if fs.id in doc['stored']:
                doc['stored'][fs.id].update(s)
            else:
                doc['stored'][fs.id] = s
            self.db.save(doc)
            return ret
        except CorruptFile as e:
            try:
                del doc['stored'][fs.id]
            except KeyError:
                pass
            corrupt = doc.get('corrupt', {})
            
            self.db.save(doc)
 
    def _verified(self, _id, fs):
        doc = self.db.get(_id)
        s = {
            'copies': fs.copies,
            'mtime': fs.stat(_id).mtime,
            'verified': time.time(),
        }
        if fs.id in doc['stored']:
            doc['stored'][fs.id].update(s)
        else:
            doc['stored'][fs.id] = s
        self.db.save(doc)

    def _corrupt(self, _id, fs):
        doc = self.db.get(_id)
        try:
            del doc['stored'][fs.id]
        except KeyError:
            pass
        doc['corrupt'][fs.id] = {
            'time': time.time(),
        }
        db.save(doc)
        
        
            
            
        
