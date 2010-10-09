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

map_bytes = """
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


class MetaStore(object):
    def __init__(self, name='dmedia', type_url='http://example.com/dmedia'):
        self.db = CouchDatabase(name, create=True)
        self.type_url = type_url

        if not self.db.view_exists('bytes'):
            self.db.add_view('bytes', map_bytes, reduce_sum)
        if not self.db.view_exists('ext'):
            self.db.add_view('ext', map_ext, reduce_count)

    def new(self, kw):
        return Record(kw, self.type_url)

    def bytes(self):
        return tuple(self.db.execute_view('bytes'))[0].value

    def extensions(self):
        for r in self.db.execute_view('ext', group=True):
            yield (r.key, r.value)
