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

from os import path
import time
import socket
import platform
import gnomekeyring
from couchdb import ResourceNotFound, ResourceConflict
import desktopcouch
from desktopcouch.records.server import  CouchDatabase
from desktopcouch.records.record import  Record
from desktopcouch.local_files import DEFAULT_CONTEXT, Context
from .util import random_id


_sum = '_sum'
_count = '_count'

type_type = """
function(doc) {
    if (doc.type) {
        emit(doc.type, null);
    }
}
"""

batch_time = """
function(doc) {
    if (doc.type == 'dmedia/batch') {
        emit(doc.time, null);
    }
}
"""

import_time = """
function(doc) {
    if (doc.type == 'dmedia/import') {
        emit(doc.time, null);
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

file_qid = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.qid) {
        emit(doc.qid, null);
    }
}
"""

file_import_id = """
function(doc) {
    if (doc.type == 'dmedia/file' && doc.import_id) {
        emit(doc.import_id, null);
    }
}
"""


def dc_context(couchdir):
    """
    Create a desktopcouch Context for testing purposes.
    """
    assert path.isdir(couchdir)
    return Context(
        path.join(couchdir, 'cache'),
        path.join(couchdir, 'data'),
        path.join(couchdir, 'config'),
    )


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


def create_machine():
    return {
        '_id': '_local/machine',
        'machine_id': random_id(),
        'type': 'dmedia/machine',
        'time': time.time(),
        'hostname': socket.gethostname(),
        'distribution': platform.linux_distribution(),
    }


class MetaStore(object):
    designs = (
        ('type', (
            ('type', type_type, _count),
        )),

        ('batch', (
            ('time', batch_time, None),
        )),

        ('import', (
            ('time', import_time, None),
        )),

        ('file', (
            ('qid', file_qid, None),
            ('import_id', file_import_id, None),
            ('bytes', file_bytes, _sum),
            ('ext', file_ext, _count),
            ('content_type', file_content_type, _count),
            ('mtime', file_mtime, None),
            ('tags', file_tags, _count),
        )),
    )

    def __init__(self, dbname='dmedia', couchdir=None):
        self.dbname = dbname
        # FIXME: once lp:672481 is fixed, this wont be needed.  See:
        # https://bugs.launchpad.net/desktopcouch/+bug/672481
        self.ctx = (DEFAULT_CONTEXT if couchdir is None else dc_context(couchdir))
        # /FIXME
        self.desktop = CouchDatabase(self.dbname, create=True, ctx=self.ctx)
        self.server = self.desktop._server
        self.db = self.server[self.dbname]
        self.create_views()
        self._machine_id = None

    def get_basic_auth(self):
        data = gnomekeyring.find_items_sync(
            gnomekeyring.ITEM_GENERIC_SECRET,
            {'desktopcouch': 'basic'}
        )
        (user, password) = data[0].secret.split(':')
        return (user, password)

    def get_port(self):
        return desktopcouch.find_port()

    def get_app_uri(self):
        (user, password) = self.get_basic_auth()
        return 'http://%s:%s@localhost:%s/dmedia/app/browser' % (
            user, password, self.get_port()
        )

    def create_machine(self):
        try:
            loc = self.db['_local/machine']
        except ResourceNotFound:
            loc = self.sync(create_machine())
        doc = dict(loc)
        doc['_id'] = doc['machine_id']
        try:
            self.db[doc['_id']] = doc
        except ResourceConflict:
            pass
        return loc['machine_id']

    @property
    def machine_id(self):
        if self._machine_id is None:
            self._machine_id = self.create_machine()
        return self._machine_id

    def update(self, doc):
        """
        Create *doc* if it doesn't exists, update doc only if different.
        """
        _id = doc['_id']
        try:
            old = self.db[_id]
            doc['_rev'] = old['_rev']
            if old != doc:
                self.db[_id] = doc
        except ResourceNotFound:
            self.db[_id] = doc

    def sync(self, doc):
        _id = doc['_id']
        self.db[_id] = doc
        return self.db[_id]

    def create_views(self):
        for (name, views) in self.designs:
            (_id, doc) = build_design_doc(name, views)
            self.update(doc)

    def by_quickid(self, qid):
        for row in self.db.view('_design/file/_view/qid', key=qid):
            yield row.id

    def total_bytes(self):
        for row in self.db.view('_design/file/_view/bytes'):
            return row.value
        return 0

    def extensions(self):
        for row in self.db.view('_design/file/_view/ext', group=True):
            yield (row.key, row.value)
