# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Import files into dmedia.
"""

import time

import microfiber
from filestore import FileStore, scandir, batch_import_iter

from dmedia.workers import CouchWorker
from dmedia import schema


class ImportWorker(CouchWorker):
    def __init__(self, env, q, key, args):
        super().__init__(env, q, key, args)
        self.srcdir = args[0]
        self.id = None
        self.doc = None

    def execute(self, srcdir):
        pass

    def start(self):
        self.doc = schema.create_import(self.srcdir,
            machine_id=self.env.get('machine_id'),
            batch_id=self.env.get('batch_id'),
        )
        self.id = self.doc['_id']
        self.db.save(self.doc)
        self.emit('started', self.id)

    def scan(self):
        self.batch = scandir(self.srcdir)
        stats = {
            'bytes': self.batch.size,
            'count': self.batch.count,
        }
        self.doc['stats']['all'] = stats
        self.doc['files_found'] = [
            {'src': file.name, 'bytes': file.size, 'mtime': file.mtime}
            for file in self.batch.files
        ]
        self.db.save(self.doc)
        self.emit('scanned', stats)

    def get_filestores(self):
        store = FileStore(self.env['filestore']['parentdir'])
        store.id = self.env['filestore']['_id']
        store.copies = 1
        return (store,)

    def import_all(self):
        stores = self.get_filestores()
        for (state, file, doc) in self.import_iter(*stores):
            self.db.save(doc)
            if state == 'empty':
                entry = file.name
            else:
                entry = {
                    'src': file.name,
                    'id': doc['_id'],
                }
            self.doc['log'][state].append(entry)
            self.doc['stats'][state]['count'] += 1
            self.doc['stats'][state]['bytes'] += file.size
        self.doc['time_end'] = time.time()
        self.db.save(self.doc)

    def import_iter(self, *filestores):
        for (file, ch) in batch_import_iter(self.batch, *filestores):
            if ch is None:
                assert file.size == 0
                yield ('empty', file, None)
                continue
            stored = dict(
                (fs.id, {'copies': fs.copies, 'mtime': fs.stat(ch.id).mtime})
                for fs in filestores
            )
            try:
                doc = self.db.get(ch.id)
                doc['stored'].update(stored)
                yield ('duplicate', file, doc)
            except microfiber.NotFound:
                doc = schema.create_file(
                    ch.id, ch.file_size, ch.leaf_hashes, stored
                )
                yield ('new', file, doc)

    def log(self, type, **kw):
        doc = {
            '_id': random_id2(),
            'type': type,
            'machine_id': self.env.get('machine_id'),
        }


#import
#access
#download
#upload
#remove
