# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com)
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

import os
from os import path
import mimetypes
import time
from base64 import b64encode
import logging

import couchdb

from .util import random_id
from .errors import DuplicateFile
from .workers import Worker, Manager, register, isregistered, exception_name
from .filestore import FileStore, quick_id, safe_open, safe_ext, pack_leaves
from .metastore import MetaStore
from .extractor import merge_metadata


mimetypes.init()
DOTDIR = '.dmedia'
log = logging.getLogger()


def normalize_ext(name):
    """
    Return (root, ext) from *name* where extension is normalized to lower-case.

    If *name* has no extension, ``None`` is returned as 2nd item in (root, ext)
    tuple:

    >>> normalize_ext('IMG_2140.CR2')
    ('IMG_2140', 'cr2')
    >>> normalize_ext('test.jpg')
    ('test', 'jpg')
    >>> normalize_ext('hello_world')
    ('hello_world', None)
    """
    parts = name.rsplit('.', 1)
    if len(parts) == 2:
        (root, ext) = parts
        if root and ext:
            try:
                return (root, safe_ext(ext.lower()))
            except (ValueError, TypeError):
                pass
    return (name, None)


def scanfiles(base, extensions=None):
    """
    Recursively iterate through files in directory *base*.
    """
    try:
        names = sorted(os.listdir(base))
    except StandardError:
        return
    dirs = []
    for name in names:
        if name.startswith('.') or name.endswith('~'):
            continue
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            (root, ext) = normalize_ext(name)
            if extensions is None or ext in extensions:
                yield {
                    'src': fullname,
                    'base': base,
                    'root': root,
                    'doc': {
                        'name': name,
                        'ext': ext,
                    },
                }
        elif path.isdir(fullname):
            dirs.append(fullname)
    for fullname in dirs:
        for d in scanfiles(fullname, extensions):
            yield d


def files_iter(base):
    """
    Recursively iterate through files in directory *base*.

    This is used for importing files from a card, after which the card will be
    automatically formatted, so we always import all files to be on the safe
    side.

    On the other hand, `scanfiles()` is used for migrating an existing library
    to dmedia... in which case we want to be more selective about which files to
    consider.

    Note that `files_iter()` does not catch errors like ``OSError``.  We
    specifically want these errors to propagate up!  We don't want a permission
    error to be interpreted as there being no files on the card!
    """
    if path.isfile(base):
        s = os.stat(base)
        yield (base, s.st_size, s.st_mtime)
        return
    names = sorted(os.listdir(base))
    dirs = []
    for name in names:
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            s = os.stat(fullname)
            yield (fullname, s.st_size, s.st_mtime)
        elif path.isdir(fullname):
            dirs.append(fullname)
    for fullname in dirs:
        for tup in files_iter(fullname):
            yield tup


def create_batch(machine_id=None):
    """
    Create initial 'dmedia/batch' accounting document.
    """
    return {
        '_id': random_id(),
        'type': 'dmedia/batch',
        'time': time.time(),
        'machine_id': machine_id,
        'imports': [],
        'stats': {
            'considered': {'count': 0, 'bytes': 0},
            'imported': {'count': 0, 'bytes': 0},
            'skipped': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
            'error': {'count': 0, 'bytes': 0},
        }
    }


def create_import(base, batch_id=None, machine_id=None):
    """
    Create initial 'dmedia/import' accounting document.
    """
    return {
        '_id': random_id(),
        'type': 'dmedia/import',
        'time': time.time(),
        'batch_id': batch_id,
        'machine_id': machine_id,
        'base': base,
        'log': {
            'imported': [],
            'skipped': [],
            'empty': [],
            'error': [],
        },
        'stats': {
            'imported': {'count': 0, 'bytes': 0},
            'skipped': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
            'error': {'count': 0, 'bytes': 0},
        }
    }


class Importer(object):
    def __init__(self, batch_id, base, extract, dbname=None):
        self.batch_id = batch_id
        self.base = base
        self.extract = extract
        self.home = path.abspath(os.environ['HOME'])
        self.metastore = MetaStore(dbname=dbname)
        self.db = self.metastore.db
        self.filestore = FileStore(
            path.join(self.home, DOTDIR),
            self.metastore.machine_id
        )
        try:
            self.db.save(self.filestore._doc)
        except couchdb.ResourceConflict:
            pass

        self.filetuples = None
        self._processed = []
        self.doc = None
        self._id = None

    def save(self):
        """
        Save current 'dmedia/import' record to CouchDB.
        """
        self.db.save(self.doc)

    def start(self):
        """
        Create the initial 'dmedia/import' record, return that record's ID.
        """
        assert self._id is None
        self.doc = create_import(self.base,
            batch_id=self.batch_id,
            machine_id=self.metastore.machine_id,
        )
        self._id = self.doc['_id']
        self.save()
        return self._id

    def scanfiles(self):
        assert self.filetuples is None
        self.filetuples = tuple(files_iter(self.base))
        self.doc['log']['considered'] = [
            {'src': src, 'bytes': size, 'mtime': mtime}
            for (src, size, mtime) in self.filetuples
        ]
        total_bytes = sum(size for (src, size, mtime) in self.filetuples)
        self.doc['stats']['considered'] = {
            'count': len(self.filetuples), 'bytes': total_bytes
        }
        self.save()
        return self.filetuples

    def _import_file(self, src):
        fp = safe_open(src, 'rb')
        stat = os.fstat(fp.fileno())
        if stat.st_size == 0:
            return ('empty', None)

        name = path.basename(src)
        (root, ext) = normalize_ext(name)
        try:
            (chash, leaves) = self.filestore.import_file(fp, ext)
            action = 'imported'
        except DuplicateFile as e:
            chash = e.chash
            leaves = e.leaves
            action = 'skipped'
            assert e.tmp.startswith(self.filestore.join('imports'))
            # FIXME: We should really probably move this into duplicates/ or
            # something and not delete till we verify integrity of what is
            # already in the filestore.
            os.remove(e.tmp)

        try:
            doc = self.db[chash]
            if self.filestore._id not in doc['stored']:
                doc['stored'][self.filestore._id] =  {
                    'copies': 1,
                    'time': time.time(),
                }
                self.db.save(doc)
            return (action, doc)
        except couchdb.ResourceNotFound as e:
            pass

        ts = time.time()
        doc = {
            '_id': chash,
            '_attachments': {
                'leaves': {
                    'data': b64encode(pack_leaves(leaves)),
                    'content_type': 'application/octet-stream',
                }
            },
            'type': 'dmedia/file',
            'time': ts,
            'bytes': stat.st_size,
            'ext': ext,
            'origin': 'user',
            'stored': {
                self.filestore._id: {
                    'copies': 1,
                    'time': ts,
                },
            },

            'import_id': self._id,
            'mtime': stat.st_mtime,
            'name': name,
            'dir': path.relpath(path.dirname(src), self.base),
        }
        if ext:
            doc['content_type'] = mimetypes.types_map.get('.' + ext)
        if self.extract:
            merge_metadata(src, doc)
        (_id, _rev) = self.db.save(doc)
        assert _id == chash
        return (action, doc)

    def import_file(self, src, size):
        self._processed.append(src)
        try:
            (action, doc) = self._import_file(src)
            if action == 'empty':
                entry = src
            elif action == 'skipped':
                entry = {
                    'mtime': doc['mtime'],
                    'src': src,
                    'id': doc['_id'],
                    'bytes': doc['bytes']
                }
            else:
                entry = {
                    'src': src,
                    'id': doc['_id'],
                    'bytes': doc['bytes']
                }
        except Exception as e:
            log.exception('Error importing %r', src)
            action = 'error'
            entry = {
                'src': src,
                'name': exception_name(e),
                'msg': str(e),
            }
        self.doc['log'][action].append(entry)
        self.doc['stats'][action]['count'] += 1
        self.doc['stats'][action]['bytes'] += size
        if action == 'error':
            self.save()
        return (action, entry)

    def import_all_iter(self):
        for (src, size, mtime) in self.filetuples:
            (action, entry) = self.import_file(src, size)
            yield (src, action)

    def finalize(self):
        assert len(self.filetuples) == len(self._processed)
        assert list(t[0] for t in self.filetuples) == self._processed
        self.doc['time_end'] = time.time()
        self.save()
        return self.doc['stats']


class ImportWorker(Worker):
    def execute(self, batch_id, base, extract=False, dbname=None):

        adapter = Importer(batch_id, base, extract, dbname)

        import_id = adapter.start()
        self.emit('started', import_id)

        files = adapter.scanfiles()
        total = len(files)
        self.emit('count', import_id, total)

        c = 1
        for (src, action) in adapter.import_all_iter():
            self.emit('progress', import_id, c, total,
                dict(
                    action=action,
                    src=src,
                )
            )
            c += 1

        stats = adapter.finalize()
        self.emit('finished', import_id, stats)


def to_dbus_stats(stats):
    return dict(
        imported=stats['imported']['count'],
        imported_bytes=stats['imported']['bytes'],
        skipped=stats['skipped']['count'],
        skipped_bytes=stats['skipped']['bytes'],
    )


def accumulate_stats(accum, stats):
    for (key, d) in stats.items():
        if key not in accum:
            accum[key] = {'count': 0, 'bytes': 0}
        for (k, v) in d.items():
            accum[key][k] += v


class ImportManager(Manager):
    def __init__(self, callback=None, dbname=None):
        super(ImportManager, self).__init__(callback)
        self._dbname = dbname
        self.metastore = MetaStore(dbname=dbname)
        self.db = self.metastore.db
        self.doc = None
        self._total = 0
        self._completed = 0
        if not isregistered(ImportWorker):
            register(ImportWorker)

    def save(self):
        """
        Save current 'dmedia/batch' record to CouchDB.
        """
        self.db.save(self.doc)

    def _start_batch(self):
        assert self.doc is None
        assert self._workers == {}
        self._total = 0
        self._completed = 0
        self.doc = create_batch(self.metastore.machine_id)
        self.save()
        self.emit('BatchStarted', self.doc['_id'])

    def _finish_batch(self):
        assert self._workers == {}
        self.doc['time_end'] = time.time()
        self.save()
        self.emit('BatchFinished', self.doc['_id'],
            to_dbus_stats(self.doc['stats'])
        )
        self.doc = None
        self.db.compact()

    def on_terminate(self, key):
        super(ImportManager, self).on_terminate(key)
        if len(self._workers) == 0:
            self._finish_batch()

    def on_started(self, key, import_id):
        self.doc['imports'].append(import_id)
        self.save()
        self.emit('ImportStarted', key, import_id)

    def on_count(self, key, import_id, total):
        self._total += total
        self.emit('ImportCount', key, import_id, total)

    def on_progress(self, key, import_id, completed, total, info):
        self._completed += 1
        self.emit('ImportProgress', key, import_id, completed, total, info)

    def on_finished(self, key, import_id, stats):
        accumulate_stats(self.doc['stats'], stats)
        self.save()
        self.emit('ImportFinished', key, import_id, to_dbus_stats(stats))

    def get_batch_progress(self):
        with self._lock:
            return (self._completed, self._total)

    def start_import(self, base, extract=True):
        with self._lock:
            if base in self._workers:
                return False
            if len(self._workers) == 0:
                self._start_batch()
            return self.do('ImportWorker', base,
                self.doc['_id'], base, extract, self._dbname
            )

    def list_imports(self):
        with self._lock:
            return sorted(self._workers)
