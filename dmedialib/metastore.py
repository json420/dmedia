# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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

"""
Store meta-data in desktop-couch.
"""

from textwrap import dedent
from couchdb import ResourceNotFound
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record
from desktopcouch.local_files import DEFAULT_CONTEXT


_sum = '_sum'
_count = '_count'

type_type = """
function(doc) {
    if (doc.type) {
        emit(doc.type, null);
    }
}
"""

import_time_start = """
function(doc) {
    if (doc.type == 'dmedia/import') {
        emit(doc.time_start, null);
    }
}
"""

file_bytes = """
function(doc) {
    if (doc.type == 'dmedia/file' && typeof(doc.bytes) == 'number') {
        emit(doc.bytes, doc.bytes);
    }
}
"""

file_ext = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.ext, null);
    }
}
"""

file_content_type = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.content_type, null);
    }
}
"""

file_mtime = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.mtime, null);
    }
}
"""

file_tags = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.tags) {
        doc.tags.forEach(function(tag) {
            emit(tag, null);
        });
    }
}
"""

file_quickid = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.quickid, null);
    }
}
"""

file_import_id = """
function(doc) {
    if (doc.type == 'dmedia/file') {
        emit(doc.import_id, null);
    }
}
"""


def build_design_doc(design, views):
    _id = '_design/' + design
    d = {}
    for (view, map_, reduce_) in views:
        d[view] = {'map': map_}
        if reduce_ is not None:
            d[view]['reduce'] = reduce_
    doc = {
        '_id': _id,
        'language': 'javascript',
        'views': d,
    }
    return (_id, doc)


class MetaStore(object):
    designs = (
        ('type', (
            ('type', type_type, _count),
        )),

        ('import', (
            ('time_start', import_time_start, None),
        )),

        ('file', (
            ('quickid', file_quickid, None),
            ('import_id', file_import_id, None),
            ('bytes', file_bytes, _sum),
            ('ext', file_ext, _count),
            ('content_type', file_content_type, _count),
            ('mtime', file_mtime, None),
            ('tags', file_tags, _count),
        )),
    )

    def __init__(self, dbname='dmedia', ctx=None):
        self.dbname = dbname
        # FIXME: once lp:672481 is fixed, this wont be needed.  See:
        # https://bugs.launchpad.net/desktopcouch/+bug/672481
        if ctx is None:
            ctx = DEFAULT_CONTEXT
        # /FIXME
        self.desktop = CouchDatabase(self.dbname, create=True, ctx=ctx)
        self.server = self.desktop._server
        self.db = self.server[self.dbname]
        self.create_views()

    def create_views(self):
        for (name, views) in self.designs:
            (_id, doc) = build_design_doc(name, views)
            try:
                old = self.db[_id]
                doc['_rev'] = old['_rev']
                if old != doc:
                    self.db[_id] = doc
            except ResourceNotFound:
                self.db[_id] = doc

    def by_quickid(self, quickid):
        for row in self.db.view('_design/file/_view/quickid', key=quickid):
            yield row.id

    def total_bytes(self):
        for row in self.db.view('_design/file/_view/bytes'):
            return row.value
        return 0

    def extensions(self):
        for row in self.db.view('_design/file/_view/ext', group=True):
            yield (row.key, row.value)
