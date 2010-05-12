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


reduce_sum = """
function(keys, values) {
    return sum(values);
}
"""

map_size = """
function(doc) {
    if (doc.size) {
        emit('total_size', doc.size);
    }
}
"""

map_ext = """
function(doc) {
    if (doc.ext) {
        emit(doc.ext, 1);
    }
}
"""


class MetaStore(object):
    def __init__(self, name='dmedia', type_url='http://example.com/dmedia'):
        self.db = CouchDatabase(name, create=True)
        self.type_url = type_url

        if not self.db.view_exists('total_size'):
            self.db.add_view('total_size', map_size, reduce_sum)
        if not self.db.view_exists('ext'):
            self.db.add_view('ext', map_ext, reduce_sum)

    def new(self, kw):
        return Record(kw, self.type_url)

    def total_size(self):
        return tuple(self.db.execute_view('total_size'))[0].value

    def extensions(self):
        for r in self.db.execute_view('ext', group=True):
            yield (r.key, r.value)
