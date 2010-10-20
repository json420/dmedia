# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Store meta-data in desktop-couch.
"""

from textwrap import dedent
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record


reduce_sum = '_sum'
reduce_count = '_count'

map_total_bytes = """
function(doc) {
    if (doc.bytes) {
        emit('bytes', doc.bytes);
    }
}
"""

map_ext = """
function(doc) {
    if (doc.ext) {
        emit(doc.ext, null);
    }
}
"""

map_mtime = """
function(doc) {
    if (doc.mtime) {
        emit(doc.mtime, null);
    }
}
"""

map_links = """
function(doc) {
    if (doc.links) {
        doc.links.forEach(function(link) {
            emit(link, null);
        });
    }
}
"""

map_tags = """
function(doc) {
    if (doc.tags) {
        doc.tags.forEach(function(tag) {
            emit(tag, null);
        });
    }
}
"""

map_project = """
function(doc) {
    if (doc.project) {
        emit(doc.project, null);
    }
}
"""


class MetaStore(object):
    type_url = 'http://example.com/dmedia'

    views = {
        'total_bytes': (map_total_bytes, reduce_sum),
        'ext': (map_ext, reduce_count),
        'mtime': (map_mtime, None),
        'links': (map_links, None),
        'tags': (map_tags, reduce_count),
        'project': (map_project, reduce_count),
    }

    def __init__(self, name='dmedia', test=False):
        if test:
            self.name = name + '_test'
        else:
            self.name = name
        self.test = test
        self.desktop = CouchDatabase(self.name, create=True)
        self.server = self.desktop._server
        if test:
            del self.server[self.name]
            self.server.create(self.name)
        self.db = self.server[self.name]
        self.create_views()

    def create_views(self):
        for (key, value) in self.views.iteritems():
            if not self.desktop.view_exists(key):
                (map_, reduce_) = value
                self.desktop.add_view(key, map_, reduce_)

    def new(self, kw):
        return Record(kw, self.type_url)

    def total_bytes(self):
        for row in self.desktop.execute_view('total_bytes'):
            return row.value
        return 0

    def extensions(self):
        for row in self.desktop.execute_view('ext', group=True):
            yield (row.key, row.value)
