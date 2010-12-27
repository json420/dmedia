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
from .util import random_id
from .workers import Worker, Manager, register, isregistered
from .filestore import FileStore, quick_id, safe_open
from .metastore import MetaStore
from .extractor import merge_metadata

mimetypes.init()


DOTDIR = '.dmedia'


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
        return (parts[0], parts[1].lower())
    return (parts[0], None)


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

    Note that `file_iter()` does not catch errors like ``OSError``.  We
    specifically want these errors to propagate up!  We don't want a permission
    error to be interpreted as there being no files on the card!
    """
    if path.isfile(base):
        yield base
        return
    names = sorted(os.listdir(base))
    dirs = []
    for name in names:
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            yield fullname
        elif path.isdir(fullname):
            dirs.append(fullname)
    for fullname in dirs:
        for f in files_iter(fullname):
            yield f


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
        'imported': {'count': 0, 'bytes': 0},
        'skipped': {'count': 0, 'bytes': 0},
    }


def create_import(base, batch_id=None, machine_id=None):
    """
    Create initial 'dmedia/import' accounting document.
    """
    return {
        '_id': random_id(),
        'type': 'dmedia/import',
        'batch_id': batch_id,
        'machine_id': machine_id,
        'base': base,
        'time': time.time(),
    }


class Importer(object):
    def __init__(self, batch_id, base, extract, couchdir=None):
        self.batch_id = batch_id
        self.base = base
        self.extract = extract
        self.home = path.abspath(os.environ['HOME'])
        self.filestore = FileStore(path.join(self.home, DOTDIR))
        self.metastore = MetaStore(couchdir=couchdir)
        self.db = self.metastore.db

        self.__stats = {
            'imported': {
                'count': 0,
                'bytes': 0,
            },
            'skipped': {
                'count': 0,
                'bytes': 0,
            },
        }
        self.__files = None
        self.__imported = []
        self._import = None
        self._import_id = None

    def start(self):
        """
        Create the initial import record, return that record's ID.
        """
        doc = create_import(self.base,
            batch_id=self.batch_id,
            machine_id=self.metastore.machine_id,
        )
        self._import_id = doc['_id']
        assert self.metastore.db.create(doc) == self._import_id
        self._import = self.metastore.db[self._import_id]
        return self._import_id

    def get_stats(self):
        return dict(
            (k, dict(v)) for (k, v) in self.__stats.iteritems()
        )

    def scanfiles(self):
        if self.__files is None:
            self.__files = tuple(files_iter(self.base))
        return self.__files

    def __import_file(self, src):
        fp = safe_open(src, 'rb')
        quickid = quick_id(fp)
        ids = list(self.metastore.by_quickid(quickid))
        if ids:
            # FIXME: Even if this is a duplicate, we should check if the file
            # is stored on this machine, and if not copy into the FileStore.
            doc = self.metastore.db[ids[0]]
            return ('skipped', doc)
        basename = path.basename(src)
        (root, ext) = normalize_ext(basename)
        (chash, action) = self.filestore.import_file(fp, quickid, ext)
        stat = os.fstat(fp.fileno())
        doc = {
            '_id': chash,
            'type': 'dmedia/file',
            'qid': quickid,
            'import_id': self._import_id,
            'bytes': stat.st_size,
            'mtime': stat.st_mtime,
            'basename': basename,
            'dirname': path.relpath(path.dirname(src), self.base),
            'ext': ext,
        }
        if ext:
            doc['content_type'] = mimetypes.types_map.get('.' + ext)
        if self.extract:
            merge_metadata(src, doc)
        assert self.metastore.db.create(doc) == chash
        return ('imported', doc)

    def import_file(self, src):
        (action, doc) = self.__import_file(src)
        self.__imported.append(src)
        self.__stats[action]['count'] += 1
        self.__stats[action]['bytes'] += doc['bytes']
        return (action, doc)

    def import_all_iter(self):
        for src in self.scanfiles():
            (action, doc) = self.import_file(src)
            yield (src, action, doc)

    def finalize(self):
        files = self.scanfiles()
        assert len(files) == len(self.__imported)
        assert set(files) == set(self.__imported)
        s = self.get_stats()
        self._import.update(s)
        self._import['time_end'] = time.time()
        self.db[self._import_id] = self._import
        assert s['imported']['count'] + s['skipped']['count'] == len(files)
        return s


class ImportWorker(Worker):
    def execute(self, batch_id, base, extract=False, couchdir=None):

        adapter = Importer(batch_id, base, extract, couchdir)

        import_id = adapter.start()
        self.emit('started', import_id)

        files = adapter.scanfiles()
        total = len(files)
        self.emit('count', import_id, total)

        c = 1
        for (src, action, doc) in adapter.import_all_iter():
            self.emit('progress', import_id, c, total,
                dict(
                    action=action,
                    src=src,
                    _id=doc['_id'],
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
        for (k, v) in d.items():
            accum[key][k] += v


class ImportManager(Manager):
    def __init__(self, callback=None, couchdir=None):
        super(ImportManager, self).__init__(callback)
        self._couchdir = couchdir
        self.metastore = MetaStore(couchdir=couchdir)
        self.db = self.metastore.db
        self._batch = None
        self._total = 0
        self._completed = 0
        if not isregistered(ImportWorker):
            register(ImportWorker)

    def _sync(self, doc):
        _id = doc['_id']
        self.db[_id] = doc
        return self.db[_id]

    def _start_batch(self):
        assert self._batch is None
        assert self._workers == {}
        self._total = 0
        self._completed = 0
        self._batch = self._sync(create_batch(self.metastore.machine_id))
        self.emit('BatchStarted', self._batch['_id'])

    def _finish_batch(self):
        assert self._workers == {}
        self._batch['time_end'] = time.time()
        self._batch = self._sync(self._batch)
        self.emit('BatchFinished', self._batch['_id'],
            to_dbus_stats(self._batch)
        )
        self._batch = None

    def on_terminate(self, key):
        super(ImportManager, self).on_terminate(key)
        if len(self._workers) == 0:
            self._finish_batch()

    def on_started(self, key, import_id):
        self._batch['imports'].append(import_id)
        self._batch = self._sync(self._batch)
        self.emit('ImportStarted', key, import_id)

    def on_count(self, key, import_id, total):
        self._total += total
        self.emit('ImportCount', key, import_id, total)

    def on_progress(self, key, import_id, completed, total, info):
        self._completed += 1
        self.emit('ImportProgress', key, import_id, completed, total, info)

    def on_finished(self, key, import_id, stats):
        accumulate_stats(self._batch, stats)
        self._batch = self._sync(self._batch)
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
                self._batch['_id'], base, extract, self._couchdir
            )

    def list_imports(self):
        with self._lock:
            return sorted(self._workers)
