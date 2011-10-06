# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com>
#   David Green <david4dev@gmail.com>
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
Store media files based on content-hash.
"""

import time
from copy import deepcopy
from subprocess import check_call
import logging

import microfiber
from filestore import FileStore, scandir, batch_import_iter

from dmedia import workers, schema


log = logging.getLogger()


def accumulate_stats(accum, stats):
    for (key, d) in stats.items():
        if key not in accum:
            accum[key] = {'count': 0, 'bytes': 0}
        for (k, v) in d.items():
            accum[key][k] += v


class ImportWorker(workers.CouchWorker):
    def __init__(self, env, q, key, args):
        super().__init__(env, q, key, args)
        self.basedir = args[0]
        self.id = None
        self.doc = None

    def execute(self, basedir):
        self.start()
        self.scan()
        self.import_all()

    def start(self):
        self.doc = schema.create_import(self.basedir,
            machine_id=self.env.get('machine_id'),
            batch_id=self.env.get('batch_id'),
        )
        self.id = self.doc['_id']
        self.db.save(self.doc)
        self.emit('started', self.id)

    def scan(self):
        self.batch = scandir(self.basedir)
        self.doc['stats']['total'] = {
            'bytes': self.batch.size,
            'count': self.batch.count,
        }
        self.doc['import_order'] = [file.name for file in self.batch.files]
        self.doc['files'] = dict(
            (file.name, {'bytes': file.size, 'mtime': file.mtime})
            for file in self.batch.files
        )
        self.db.save(self.doc)
        self.emit('scanned', self.batch.count, self.batch.size)

    def get_filestores(self):
        stores = []
        for doc in self.env['filestores']:
            fs = FileStore(doc['parentdir'], doc['_id'], doc['copies'])
            stores.append(fs)
        return stores

    def import_all(self):
        stores = self.get_filestores()
        try:
            for (status, file, doc) in self.import_iter(*stores):
                self.doc['stats'][status]['count'] += 1
                self.doc['stats'][status]['bytes'] += file.size
                self.doc['files'][file.name]['status'] = status
                if doc is not None:
                    self.db.save(doc)
                    self.doc['files'][file.name]['id'] = doc['_id']
                self.emit('progress', file.size)
            self.doc['time_end'] = time.time()
        finally:
            self.db.save(self.doc)
        self.emit('finished', self.doc['stats'])

    def import_iter(self, *filestores):
        common = {
            'import_id': self.id,
            'machine_id': self.env.get('machine_id'),
            'batch_id': self.env.get('batch_id'),
        }
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
                doc['import'] = {
                    'src': file.name,
                    'mtime': file.mtime,
                }
                doc['import'].update(common)
                doc['ctime'] = file.mtime
                yield ('new', file, doc)


class ImportManager(workers.CouchManager):
    def __init__(self, env, callback=None):
        super().__init__(env, callback)
        self.doc = None
        self._reset_counters()
        if not workers.isregistered(ImportWorker):
            workers.register(ImportWorker)

    def _reset_counters(self):
        self._count = 0
        self._total_count = 0
        self._bytes = 0
        self._total_bytes = 0

    def get_worker_env(self, worker, key, args):
        env = deepcopy(self.env)
        env['batch_id'] = self.doc['_id']
        return env

    def first_worker_starting(self):
        assert self.doc is None
        assert self._workers == {}
        self._reset_counters()
        self.doc = schema.create_batch(self.env.get('machine_id'))
        self.db.save(self.doc)
        self.emit('batch_started', self.doc['_id'])

    def last_worker_finished(self):
        assert self._workers == {}
        self.doc['time_end'] = time.time()
        self.db.save(self.doc)
        self.emit('batch_finished', self.doc['_id'], self.doc['stats'])
        self.doc = None

    def on_error(self, key, exception, message):
        super(ImportManager, self).on_error(key, exception, message)
        if self.doc is None:
            return
        self.doc['errors'].append(
            {'key': key, 'name': exception, 'msg': message}
        )
        self.db.save(self.doc)

    def on_started(self, key, import_id):
        self.doc['imports'].append(import_id)
        self.db.save(self.doc)
        self.emit('import_started', key, import_id)

    def on_scanned(self, key, total_count, total_bytes):
        self._total_count += total_count 
        self._total_bytes += total_bytes
        self.emit('batch_progress',
            self._count, self._total_count,
            self._bytes, self._total_bytes,
        )

    def on_progress(self, key, file_size):
        self._count += 1
        self._bytes += file_size
        self.emit('batch_progress',
            self._count, self._total_count,
            self._bytes, self._total_bytes,
        )

    def on_finished(self, key, stats):
        accumulate_stats(self.doc['stats'], stats)
        self.db.save(self.doc)

    def get_batch_progress(self):
        with self._lock:
            return (self._count, self._total_count, self._bytes, self._total_bytes)

    def start_import(self, base):
        return self.start_job('ImportWorker', base, base)
