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
from .filestore import FileStore, quick_id, safe_open
from .metastore import MetaStore
from .extractor import merge_metadata

mimetypes.init()


DOTDIR = '.dmedia'


def import_files(q, base, extensions):
    q.put(['ImportStarted', base])

    # Get the file list:
    files = tuple(scanfiles(base, extensions))
    if not files:
        q.put(['ImportFinished', base])
        return

    i = 0
    total = len(files)
    q.put(['ImportProgress', base, 0, total])

    importer = Importer()
    for d in importer.import_files(files):
        i += 1
        q.put(['ImportProgress', base, i, total])

    q.put(['ImportFinished', base])


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


class Importer(object):
    def __init__(self, base, extract=True, ctx=None):
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

    def scanfiles(self):
        if self.__files is None:
            self.__files = tuple(files_iter(self.base))
        return self.__files


    # FIXME: `name` should be renamed to `basename` and we should also add
    # `dirname` which is relative to card mount point
    def import_file(self, src):
        fp = safe_open(src, 'rb')
        quickid = quick_id(fp)
        ids = list(self.metastore.by_quickid(quickid))
        if ids:
            doc = self.metastore.db[ids[0]]
            return ('skipped', doc)
        basename = path.basename(src)
        (root, ext) = normalize_ext(basename)
        (chash, action) = self.filestore.import_file(fp, quickid, ext)
        stat = os.fstat(fp.fileno())
        doc = {
            '_id': chash,
            'quickid': quickid,
            'bytes': stat.st_size,
            'mtime': stat.st_mtime,
            'name': basename,
            'ext': ext,
        }
        if ext:
            doc['mime'] = mimetypes.types_map.get('.' + ext)
        assert self.metastore.db.create(doc) == chash
        return ('imported', doc)

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
