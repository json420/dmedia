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
Verify integrity of files on disk.
"""

import time

from filestore import FileStore, CorruptFile, FileNotFound

from dmedia.util import get_db


ONE_WEEK = 60 * 60 * 24 * 7


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


def mark_corrupt(doc, fs):
    stored = get_dict(doc, 'stored')
    try:
        del stored[fs.id]
    except KeyError:
        pass
    corrupt = get_dict(doc, 'corrupt')
    doc['corrupt'][fs.id] = {
        'time': time.time(),
    }


def verify(env, parentdir, max_delta=ONE_WEEK):
    db = get_db(env)
    local = db.get('_local/dmedia')
    info = local['stores'][parentdir]
    fs = FileStore(parentdir, info['id'], info['copies'])
    start = [fs.id, None]
    end = [fs.id, time.time() - max_delta]
    while True:
        r = db.view('file', 'verified', startkey=start, endkey=end, limit=1)
        if not r['rows']:
            break
        row = r['rows'][0]
        _id = row['id']
        doc = db.get(_id)
        try:
            fs.verify(_id)
            doc['stored'][fs.id] = {
                'copies': fs.copies,
                'mtime': fs.stat(_id).mtime,
                'verified': int(time.time()),
            }
        except CorruptFile:
            mark_corrupt(doc, fs)
        except FileNotFound:
            del doc['stored'][fs.id]
        db.save(doc)
        
