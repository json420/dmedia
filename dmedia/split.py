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
Migrate from monolithic dmedia database to dmedia-0 plus project databases.
"""

_core = (
    '_id',
    'ver',
    'type',
    'time',
    'atime',
    'bytes',
    'content_type',
    'origin',
    'stored',
)


def file_to_core(doc):
    assert doc['type'] == 'dmedia/file'
    assert doc['ver'] == 0
    for key in _core:
        yield (key, doc[key])
    att = doc['_attachments']
    yield ('_attachments', {'leaf_hashes': att['leaf_hashes']})


def doc_to_core(doc):
    if doc.get('type') not in ('dmedia/file', 'dmedia/store', 'dmedia/machine'):
        return
    if doc['type'] == 'dmedia/file':
        return dict(file_to_core(doc))
    return doc


def file_to_project(doc):
    pass


