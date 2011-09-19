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

import os
from os import path
import mimetypes
import time
from base64 import b64encode
import logging

import microfiber

from .schema import (
    random_id, create_file, create_batch, create_import, create_drive,
    create_partition
)
from .errors import DuplicateFile
from .workers import (
    CouchWorker, CouchManager, register, isregistered, exception_name
)
from .filestore import FileStore, quick_id, safe_open, safe_ext, pack_leaves
from .extractor import merge_metadata

mimetypes.init()
DOTDIR = '.dmedia'
log = logging.getLogger()


# FIXME: This needs to be done with some real inspection of the file contents,
# but this is just a stopgap for the sake of getting the schema stable:
MEDIA_MAP = {
    'ogv': 'video',
    'mov': 'video',
    'avi': 'video',
    'mts': 'video',

    'oga': 'audio',
    'flac': 'audio',
    'wav': 'audio',
    'mp3': 'audio',

    'jpg': 'image',
    'cr2': 'image',
    'png': 'image',
}


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


class ImportWorker(CouchWorker):
    def __init__(self, env, q, key, args):
        super(ImportWorker, self).__init__(env, q, key, args)
        (self.base, self.extract) = args
        self.filestore = FileStore(self.env['filestore']['path'])
        self.filestore_id = self.env['filestore']['_id']

        self.filetuples = None
        self._processed = []
        self.doc = None
        self._id = None

    def execute(self, base, extract=False):
        import_id = self.start()
        self.emit('started', import_id)

        files = self.scanfiles()
        total = len(files)
        self.emit('count', import_id, total)

        c = 1
        for (src, action) in self.import_all_iter():
            self.emit('progress', import_id, c, total,
                dict(
                    action=action,
                    src=src,
                )
            )
            c += 1

        stats = self.finalize()
        self.emit('finished', import_id, stats)

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
        #drive = create_drive(self.base)
        #partition = create_partition(self.base)
        self.doc = create_import(self.base,
            None, #partition['_id'],
            batch_id=self.env.get('batch_id'),
            machine_id=self.env.get('machine_id'),
        )
        self._id = self.doc['_id']
        self.save()
        #try:
        #    self.db.save(drive)
        #except microfiber.Conflict:
        #    pass
        #try:
        #    self.db.save(partition)
        #except microfiber.Conflict:
        #    pass
        return self._id

    def scanfiles(self):
        """
        Build list of files that will be considered for import.

        After this method has been called, the ``Importer.filetuples`` attribute
        will contain ``(filename,size,mtime)`` tuples for all files being
        considered.  This information is saved into the dmedia/import record to
        provide a rich audio trail and aid in debugging.
        """
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
        """
        Attempt to import *src* into dmedia library.
        """
        fp = safe_open(src, 'rb')
        stat = os.fstat(fp.fileno())
        if stat.st_size == 0:
            log.warning('File size is zero: %r', src)
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
            doc = self.db.get(chash)
            if self.filestore_id not in doc['stored']:
                doc['stored'][self.filestore_id] =  {
                    'copies': 1,
                    'time': time.time(),
                }
                self.db.save(doc)
            return (action, doc)
        except microfiber.NotFound as e:
            pass

        leaf_hashes = b''.join(leaves)
        stored = {
            self.filestore_id: {
                'copies': 1,
            }
        }
        doc = create_file(chash, stat.st_size, leaf_hashes, stored, ext=ext)
        assert doc['_id'] == chash
        doc.update(
            import_id=self._id,
            mtime=stat.st_mtime,
            name=name,
            dir=path.relpath(path.dirname(src), self.base),
        )
        if ext:
            doc['content_type'] = mimetypes.types_map.get('.' + ext)
            doc['media'] = MEDIA_MAP.get(ext)
        if self.extract:
            merge_metadata(src, doc)
        r = self.db.save(doc)
        assert r['id'] == chash
        return (action, doc)

    def import_file(self, src, size):
        """
        Wraps `Importer._import_file()` with error handling and logging.
        """
        self._processed.append(src)
        try:
            (action, doc) = self._import_file(src)
            if action == 'empty':
                entry = src
            else:
                entry = {
                    'src': src,
                    'id': doc['_id'],
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
        """
        Finalize import and save final import record to CouchDB.

        The method will add the ``"time_end"`` key into the import record and
        save it to CouchDB.  There will likely also be being changes in the
        ``"log"`` and ``"stats"`` keys, which will likewise be saved to CouchDB.
        """
        assert len(self.filetuples) == len(self._processed)
        assert list(t[0] for t in self.filetuples) == self._processed
        self.doc['time_end'] = time.time()
        self.save()
        dt = self.doc['time_end'] - self.doc['time']
        log.info('Completed import of %r in %d:%02d',
            self.base, dt / 60, dt % 60
        )
        return self.doc['stats']


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


class ImportManager(CouchManager):
    def __init__(self, env, callback=None):
        super(ImportManager, self).__init__(env, callback)
        self.doc = None
        self._total = 0
        self._completed = 0
        if not isregistered(ImportWorker):
            register(ImportWorker)

    def get_worker_env(self, worker, key, args):
        env = dict(self.env)
        env['batch_id'] = self.doc['_id']
        return env

    def first_worker_starting(self):
        assert self.doc is None
        assert self._workers == {}

        self._count = 0
        self._total_count = 0
        self._bytes = 0
        self._total_bytes = 0

        self.doc = create_batch(self.env.get('machine_id'))
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
        self.emit('ImportStarted', key, import_id)

    def on_scanned(self, key, total_count, total_bytes):
        self._total_count += total_count 
        self._total_bytes += total_bytes

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

    def start_import(self, base, extract=True):
        return self.start_job('ImportWorker', base, base, extract)
