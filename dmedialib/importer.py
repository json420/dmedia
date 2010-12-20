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
from .constants import IMPORT_RECORD
from .workers import Worker
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


def create_import_record(mount):
    return {
        '_id': random_id(),
        'record_type': IMPORT_RECORD,
        'mount': mount,
        'time_start': time.time(),
    }


class Importer(object):
    def __init__(self, base, extract, ctx=None):
        self.base = base
        self.extract = extract
        self.home = path.abspath(os.environ['HOME'])
        self.filestore = FileStore(path.join(self.home, DOTDIR))
        self.metastore = MetaStore(ctx=ctx)

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
        doc = create_import_record(self.base)
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
            'quickid': quickid,
            'import_id': self._import_id,
            'bytes': stat.st_size,
            'mtime': stat.st_mtime,
            'basename': basename,
            'dirname': path.relpath(path.dirname(src), self.base),
            'ext': ext,
        }
        if ext:
            doc['mime'] = mimetypes.types_map.get('.' + ext)
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
        assert s['imported']['count'] + s['skipped']['count'] == len(files)
        return s

    def _import_one(self, d, extract=True):
        try:
            fp = open(d['src'], 'rb')
        except IOError:
            d['action'] = 'ioerror'
            return d
        doc = d['doc']
        stat = os.fstat(fp.fileno())
        quickid = quick_id(fp)
        doc.update({
            'quickid': quickid,
            'mtime': stat.st_mtime,
            'bytes': stat.st_size,
        })
        ids = list(self.metastore.by_quickid(quickid))
        if ids:
            d['action'] = 'skipped_duplicate'
            return d
        (chash, action) = self.filestore.import_file(fp, quickid, doc['ext'])
        doc['_id'] = chash
        if doc['ext']:
            doc['mime'] = mimetypes.types_map.get('.' + doc['ext'])
        d['action'] = action
        if extract:
            merge_metadata(d)
        self.metastore.db.create(d['doc'])
        return d

    def recursive_import(self, base, extensions, common=None, extract=True):
        for d in scanfiles(base, extensions):
            if common:
                d['doc'].update(common)
            yield self._import_one(d, extract)

    def import_files(self, files, common=None, extract=True):
        for d in files:
            if common:
                d['doc'].update(common)
            yield self._import_one(d, extract)


class DummyImporter(object):
    """
    Dummy adapter for testing dbus service.

    Note that DummyImporter.scanfiles() will sleep for 1 second to facilitate
    testing.
    """
    def __init__(self, base):
        self.base = base
        self._files = tuple(
            path.join(self.base, *parts) for parts in [
                ('DCIM', '100EOS5D2', 'MVI_5751.MOV'),
                ('DCIM', '100EOS5D2', 'MVI_5751.THM'),
                ('DCIM', '100EOS5D2', 'MVI_5752.MOV'),
            ]
        )
        self._mov_size = 20202333
        self._thm_size = 27328

    def start(self):
        return '4CXJKLJ3MXAVTNWYEPHTETHV'

    def scanfiles(self):
        time.sleep(1)
        return self._files

    def import_all_iter(self):
        yield (self._files[0], 'imported',
            dict(_id='OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        )
        yield (self._files[1], 'imported',
            dict(_id='F6ATTKI6YVWVRBQQESAZ4DSUXQ4G457A')
        )
        yield (self._files[2], 'skipped',
            dict(_id='OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        )

    def finalize(self):
        return {
            'imported': {
                'count': 2,
                'bytes': 20202333 + 27328,
            },
            'skipped': {
                'count': 1,
                'bytes': 20202333,
            },
        }


class import_files(Worker):
    ctx = None

    def execute(self, base, extract):

        if self.dummy:
            adapter = DummyImporter(base)
        else:
            adapter = Importer(base, extract, ctx=self.ctx)

        import_id = adapter.start()
        self.emit('ImportStarted', base, import_id)

        files = adapter.scanfiles()
        total = len(files)
        self.emit('ImportCount', base, total)

        c = 1
        for (src, action, doc) in adapter.import_all_iter():
            self.emit('ImportProgress', base, c, total,
                dict(
                    action=action,
                    src=src,
                    _id=doc['_id'],
                )
            )
            c += 1

        stats = adapter.finalize()
        self.emit('ImportFinished', base,
            dict(
                imported=stats['imported']['count'],
                imported_bytes=stats['imported']['bytes'],
                skipped=stats['skipped']['count'],
                skipped_bytes=stats['skipped']['bytes'],
            )
        )
