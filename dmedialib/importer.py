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
from .filestore import FileStore, quick_id
from .metastore import MetaStore
from dmedialib.extractor import merge_metadata

mimetypes.init()


DOTDIR = '.dmedia'


def import_files(q, base, extensions):
    def put(kind, **kw):
        kw.update(dict(
            domain='import',
            kind=kind,
            base=base,
        ))
        q.put(kw)

    put('status', status='started')

    # Get the file list:
    files = tuple(scanfiles(base, extensions))
    if not files:
        put('finish')
        return

    i = 0
    count = len(files)
    put('progress',
        current=i,
        total=count,
    )

    importer = Importer()
    for d in importer.import_files(files):
        i += 1
        put('progress',
            current=i,
            total=count,
        )

    put('status', status='finished')


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


class Importer(object):
    def __init__(self, ctx=None):
        self.home = path.abspath(os.environ['HOME'])
        self.filestore = FileStore(path.join(self.home, DOTDIR))
        self.metastore = MetaStore(ctx=ctx)

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
