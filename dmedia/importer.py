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
from os import path
import os
from gettext import gettext as _
from gettext import ngettext
import logging
import mimetypes
import shutil
from queue import Queue

from microfiber import NotFound, has_attachment, encode_attachment
from filestore import FileStore, scandir, batch_import_iter, statvfs

from dmedia.parallel import start_thread
from dmedia.util import get_project_db
from dmedia.units import bytes10
from dmedia import workers, schema
from dmedia.metastore import MetaStore, create_stored, update_duplicate_file
from dmedia.extractor import extract, merge_thumbnail


log = logging.getLogger()
mimetypes.init()


def normalize_ext(filename):
    ext = path.splitext(filename)[1]
    if ext:
        return ext.strip('.').lower()


def notify_started(basedirs):
    assert len(basedirs) >= 1
    summary = ngettext(
        'Importing files from {count} card:',
        'Importing files from {count} cards:',
        len(basedirs)
    ).format(count=len(basedirs))
    body = '\n'.join(basedirs)
    return (summary, body)


def notify_stats2(stats):
    new = stats['new']['count']
    duplicate = stats['duplicate']['count']
    empty = stats['empty']['count']
    if new == 0 and duplicate == 0 and empty == 0:
        return [_('No files found')]
    if new > 0:
        summary = ngettext(
            '{count} new file, {size}',
            '{count} new files, {size}',
            new
        ).format(
            count=new,
            size=bytes10(stats['new']['bytes']),
        )
    else:
        summary = _('No new files')
    lines = [summary]
    if duplicate > 0:
        msg = ngettext(
            '{count} duplicate file, {size}',
            '{count} duplicate files, {size}',
            duplicate
        ).format(
            count=duplicate,
            size=bytes10(stats['duplicate']['bytes']),
        )
        lines.append(msg)
    if empty > 0:
        msg = ngettext(
            '{count} empty file',
            '{count} empty files',
            empty
        ).format(count=empty)
        lines.append(msg) 
    return lines


def notify_stats(stats):
    lines = notify_stats2(stats)
    summary = lines[0]
    lines = lines[1:]
    body = ('\n'.join(lines) if lines else None)
    return (summary, body)


def label_text(count, size):
    return ngettext(
        '{count} file, {size}',
        '{count} files, {size}',
        count
    ).format(count=count, size=bytes10(size))


def accumulate_stats(accum, stats):
    for (key, d) in stats.items():
        if key not in accum:
            accum[key] = {'count': 0, 'bytes': 0}
        for (k, v) in d.items():
            accum[key][k] += v


def sum_progress(progress):
    """
    Sum the progress stats for all imports to for 'batch_progress' signal.

    For example:

    >>> progress = {
    ...     'UEZ2ZH25CZSEQEVENYJMHKZH': (10, 20, 30, 40),
    ...     'OE36HPQOUVAV5EXYCCUV4R55': (100, 200, 300, 400),
    ... }
    ... 
    >>> sum_progress(progress)
    (110, 220, 330, 440)

    """
    values = tuple(progress.values())
    return tuple(
        sum(v[i] for v in values)
        for i in range(4)
    )


def get_rate(doc):
    try:
        elapsed = doc['time_end'] - doc['time']
        rate = doc['stats']['total']['bytes'] / elapsed
        return bytes10(rate) + '/s'
    except Exception:
        pass       


class ImportWorker(workers.CouchWorker):
    def __init__(self, env, q, key, args):
        super().__init__(env, q, key, args)
        self.basedir = args[0]
        self.extra = None
        self.id = None
        self.doc = None
        self.extract = self.env.get('extract', True)
        self.log_db = self.db.database(schema.LOG_DB_NAME)
        self.log_db.ensure()
        self.project = get_project_db(self.env['project_id'], self.env)
        self.project.ensure()
        self.extraction_queue = Queue(10)

    def execute(self, basedir, extra=None):
        self.extra = extra
        self.start()
        self.scan()
        self.import_all()

    def start(self):
        self.doc = schema.create_import(self.basedir,
            machine_id=self.env.get('machine_id'),
            batch_id=self.env.get('batch_id'),
        )
        st = statvfs(self.basedir)
        self.doc['statvfs'] = st._asdict()
        self.doc['basedir_ismount'] = path.ismount(self.basedir)
        self.doc['stores'] = self.env['stores']
        if self.extra:
            self.doc.update(self.extra)
        self.id = self.doc['_id']
        self.db.save(self.doc)
        self.emit('started', self.id, self.extra)

    def scan(self):
        self.batch = scandir(self.basedir)
        log.info('%r has %d files, %s', self.basedir, self.batch.count,
            bytes10(self.batch.size)
        )
        self.doc['stats']['total'] = {
            'bytes': self.batch.size,
            'count': self.batch.count,
        }
        self.doc['files'] = dict(
            (file.name, {'bytes': file.size, 'mtime': file.mtime})
            for file in self.batch.files
        )
        self.db.save(self.doc)
        self.emit('scanned', self.id, self.batch.count, self.batch.size)

    def get_filestores(self):
        # FIXME: Should pick up to 2 filestores based size of import and
        # available space on the filestores.
        stores = []
        for _id in sorted(self.env['stores']):
            info = self.env['stores'][_id]
            fs = FileStore(info['parentdir'], _id)
            stores.append(fs)
        return stores

    def import_all(self):
        self.thumbnail = None
        extractor = start_thread(self.extractor)
        stores = self.get_filestores()
        try:
            for (status, file, ch) in self.import_iter(*stores):
                self.doc['stats'][status]['count'] += 1
                self.doc['stats'][status]['bytes'] += file.size
                self.doc['files'][file.name]['status'] = status
                if ch is not None:
                    self.doc['files'][file.name]['id'] = ch.id
            self.doc['time_end'] = time.time()
            self.doc['rate'] = get_rate(self.doc)
        finally:
            self.db.save(self.doc)
            self.extraction_queue.put(None)
            extractor.join()
            if self.thumbnail:
                self.doc['_attachments'] = {
                    'thumbnail': encode_attachment(self.thumbnail)
                }
                self.db.save(self.doc)
            del self.doc['_rev']
            self.project.post(self.doc)
        self.emit('finished', self.id, self.doc['stats'])

    def import_iter(self, *filestores):
        common = {
            'import_id': self.id,
            'batch_id': self.env.get('batch_id'),
            'project_id': self.env.get('project_id'),
            'machine_id': self.env.get('machine_id'),
        }
        for (file, ch) in batch_import_iter(self.batch, *filestores,
            callback=self.progress_callback
        ):
            if ch is None:
                assert file.size == 0
                yield ('empty', file, None)
                continue
            timestamp = time.time()
            self.extraction_queue.put((timestamp, file, ch))
            self.log_db.save(
                schema.log_file_import(timestamp, ch.id, file, **common)
            )
            stored = create_stored(ch.id, *filestores)
            try:
                doc = self.db.get(ch.id)
                self.db.update(update_duplicate_file, doc, timestamp, stored)
                yield ('duplicate', file, ch)
            except NotFound:
                doc = schema.create_file(timestamp, ch, stored)
                self.db.save(doc)
                yield ('new', file, ch)

    def progress_callback(self, count, size):
        self.emit('progress', self.id,
            count, self.batch.count,
            size, self.batch.size
        )

    def extractor(self):
        need_thumbnail = True
        common = {
            'import_id': self.id,
            'batch_id': self.env.get('batch_id'),
            'machine_id': self.env.get('machine_id'),
        }
        while True:
            item = self.extraction_queue.get()
            if item is None:
                break
            try:
                (timestamp, file, ch) = item
                try:
                    doc = self.project.get(ch.id)
                except NotFound:
                    doc = schema.create_project_file(timestamp, ch, file)
                    ext = normalize_ext(file.name)
                    if ext:
                        doc['ext'] = ext
                    extract(file.name, doc)
                    merge_thumbnail(file.name, doc)
                    doc.update(common)
                    self.project.save(doc)
                if need_thumbnail and has_attachment(doc, 'thumbnail'):
                    need_thumbnail = False
                    self.thumbnail = self.project.get_att(ch.id, 'thumbnail') 
                    self.emit('import_thumbnail', self.id, ch.id)            
            except Exception:
                log.exception('Error in extractor thread:')


class ImportManager(workers.CouchManager):
    def __init__(self, env, callback=None):
        super().__init__(env, callback)
        self.doc = None
        self._reset()
        if not workers.isregistered(ImportWorker):
            workers.register(ImportWorker)
        self.ms = MetaStore(self.db)

    def _reset(self):
        self._error = None
        self._progress = {}

    def get_worker_env(self, worker, key, args):
        env = deepcopy(self.env)
        env['batch_id'] = self.doc['_id']
        env['stores'] = self.doc['stores']
        return env

    def first_worker_starting(self):
        assert self.doc is None
        assert self._workers == {}
        self._reset()
        self.machine = self.ms.get_machine()
        stores = self.machine['stores']
        assert isinstance(stores, dict)
        if not stores:
            raise ValueError('No FileStores to import into!')
        self.copies = sum(v['copies'] for v in stores.values())
        if self.copies < 1:
            raise ValueError('must have at least durability of copies=1')
        self.doc = schema.create_batch(self.env.get('machine_id'))
        self.doc['stores'] = stores
        self.doc['copies'] = self.copies
        self.db.save(self.doc)
        self.emit('batch_started', self.doc['_id'])

    def last_worker_finished(self):
        assert self._workers == {}
        #t = TimeDelta()
        #os.sync()
        #t.log('called os.sync()')
        self.doc['time_end'] = time.time()
        self.doc['rate'] = get_rate(self.doc)
        log.info('Combined batch import rate: %s', self.doc['rate'])
        self.db.save(self.doc)
        self.emit('batch_finished',
            self.doc['_id'],
            self.doc['stats'],
            self.copies,
            notify_stats2(self.doc['stats'])
        )
        self.doc = None

    def on_error(self, basedir, exception, message):
        error = {
            'basedir': basedir,
            'name': exception,
            'message': message,
        }
        try:
            if self.doc is not None:
                self.doc['error'] = error
                self.db.save(self.doc)
        finally:
            self.doc = None
            self.abort_with_error(error)

    def on_started(self, basedir, import_id, extra):
        assert import_id not in self.doc['imports']
        self.doc['imports'][import_id] = {
            'basedir': basedir,
        }
        self.db.save(self.doc)
        self.emit('import_started', basedir, import_id, extra)

    def on_scanned(self, basedir, _id, total_count, total_size):
        self.emit('import_scanned', basedir, _id, total_count, total_size)
        self._progress[_id] = (0, total_count, 0, total_size)
        self.emit('batch_progress', *sum_progress(self._progress))

    def on_progress(self, basedir, _id, count, total_count, size, total_size):
        self._progress[_id] = (count, total_count, size, total_size)
        self.emit('batch_progress', *sum_progress(self._progress))

    def on_finished(self, basedir, import_id, stats):
        self.doc['imports'][import_id]['stats'] = stats
        self.db.save(self.doc)
        accumulate_stats(self.doc['stats'], stats)
        self.db.save(self.doc)

    def get_batch_progress(self):
        with self._lock:
            return sum_progress(self._progress)

    def start_import(self, base, extra=None):
        return self.start_job('ImportWorker', base, base, extra)


def has_magic_lantern(basedir):
    dcim = path.join(basedir, 'DCIM')
    autoexec = path.join(basedir, 'AUTOEXEC.BIN')
    return path.isdir(dcim) and path.isfile(autoexec)


def iter_names(basedir, *parents):
    d = path.join(basedir, *parents)
    for name in sorted(os.listdir(d)):
        f = path.join(d, name)
        if path.isfile(f):
            yield parents + (name,)


def get_magic_lantern_names(basedir):
    for tup in iter_names(basedir):
        yield tup
    for tup in iter_names(basedir, 'CROPMKS'):
        yield tup
    for tup in iter_names(basedir, 'DOC'):
        yield tup


def copy_magic_lantern(src, dst):
    assert has_magic_lantern(src)
    for name in ('CROPMKS', 'DCIM', 'DOC'):
        os.mkdir(path.join(dst, name))
    names = get_magic_lantern_names(src)
    for tup in names:
        shutil.copy2(path.join(src, *tup), path.join(dst, *tup))

