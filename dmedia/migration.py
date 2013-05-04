# dmedia: distributed media library
# Copyright (C) 2013 Novacut Inc
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
Migrate from the V0 to V1 hashing protocol and schema.
"""

from dbase32 import db32enc, isdb32
from dbase32.rfc3548 import b32dec, isb32
from copy import deepcopy
import re

from .metastore import get_dict, BufferedSave
from . import schema


V0_PROJECT_DB = re.compile('^dmedia-0-([234567abcdefghijklmnopqrstuvwxyz]{24})$')


def b32_to_db32(_id):
    """
    Re-encode an ID from Base32 to Dbase32.

    >>> b32_to_db32('5Q2VGOCRKWGDCSJOHDMXYFCE')
    'WJTO9H5KDP965LCHA6FQR857'

    """
    return db32enc(b32dec(_id))


def migrate_file(old, mdoc):
    assert isb32(old['_id'])
    assert isdb32(mdoc['v1_id'])
    assert old['_id'] == mdoc['_id']
    assert old['bytes'] == mdoc['bytes']
    new = {
        '_id': mdoc['v1_id'],
        '_attachments': {
            'leaf_hashes': mdoc['_attachments']['v1_leaf_hashes']
        },
        'type': 'dmedia/file',
        'time': old['time'],
        'atime': old['atime'],
        'bytes': old['bytes'],
        'origin': old['origin'],
        'stored': dict(
            (b32_to_db32(key), value) for (key, value) in old['stored'].items()
        ),
    }
    schema.check_file(new)
    return new


def migrate_store(old):
    new = deepcopy(old)
    del new['_rev']
    new['_id'] = b32_to_db32(old['_id'])
    schema.check_store(new)
    return new


def migrate_project(old):
    new = deepcopy(old)
    del new['_rev']
    new['_id'] = b32_to_db32(old['_id'])
    new['db_name'] = schema.project_db_name(new['_id'])
    schema.check_project(new)
    return new


def migrate_batch(old):
    new = deepcopy(old)
    del new['_rev']
    new['_id'] = b32_to_db32(old['_id'])
    new['imports'] = dict(
        (b32_to_db32(key), value) for (key, value) in old['imports'].items()
    )
    return new


def migrate_import(old, id_map):
    new = deepcopy(old)
    del new['_rev']
    new['_id'] = b32_to_db32(old['_id'])
    new['batch_id'] = b32_to_db32(old['batch_id'])
    for f in new['files'].values():
        if f.get('id') in id_map:
            f['id'] = id_map[f['id']]
    return new


def iter_v0_project_dbs(server):
    for name in server.get('_all_dbs'):
        match = V0_PROJECT_DB.match(name)
        if match:
            _id = match.group(1).upper()
            yield (name, _id)

