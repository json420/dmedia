# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#
# media: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `media`.
#
# `media` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `media` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `media`.  If not, see <http://www.gnu.org/licenses/>.

"""
Store media files based on content-hash.
"""

import os
from os import path
import shutil
import tempfile
import hashlib

CHUNK = 2 ** 20  # Read in chunks of 1 MiB


def hash_file(filename, hashfunc=hashlib.sha256):
    """
    Compute the content-hash of the file *filename*.
    """
    fp = open(filename, 'rb')
    h = hashfunc()
    while True:
        chunk = fp.read(CHUNK)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def normalize_ext(name):
    """
    Return extension from *name* normalized to lower-case.

    If *name* has no extension, ``None`` is returned.  For example:

    >>> normalize_ext('IMG_2140.CR2')
    'cr2'
    >>> normalize_ext('test.jpg')
    'jpg'
    >>> normalize_ext('hello_world') is None
    True
    """
    parts = name.split('.', 1)
    if len(parts) == 2:
        return parts[1].lower()


def scanfiles(base, extensions=None):
    """
    Recursively iterate through files in directory *base*.
    """
    try:
        names = sorted(os.listdir(base))
    except StandardError:
        return
    for name in names:
        fullname = path.join(base, name)
        if path.islink(fullname):
            continue
        if path.isfile(fullname):
            if extensions is None or normalize_ext(name) in extensions:
                yield fullname
        elif path.isdir(fullname):
            for f in scanfiles(fullname, extensions):
                yield f
