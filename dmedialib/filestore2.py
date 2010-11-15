# Authors:
#   Jason Gerard DeRose <jderose@jasonderose.org>
#   Akshat Jain <ssj6akshat1234@gmail.com)
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@jasonderose.org>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Store media files based on content-hash.

Security note: this module must be carefully designed to prevent path traversal!
"""

import os
from os import path
from hashlib import sha1 as HASH
from base64 import b32encode, b32decode
import logging
from subprocess import check_call, CalledProcessError


B32LENGTH = 32  # Length of base32-encoded hash
CHUNK = 2 ** 20  # Read in chunks of 1 MiB
QUICK_ID_CHUNK = 2 ** 20  # Amount to read for quick_id()
FALLOCATE = '/usr/bin/fallocate'
TYPE_ERROR = '%s: need a %r; got a %r: %r'  # Standard TypeError message


def safehash(chash):
    """
    Verify that *chash* is valid base32-encoding and correct length.

    A malicious *chash* could cause path traversal or other security gotchas,
    thus this sanity check.  When *chash* is valid, it is returned unchanged:

    >>> safehash('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
    'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

    However, when *chash* does not conform, a ``TypeError`` or ``ValueError`` is
    raised:

    >>> safehash('NWBNVXVK5DQGIOW7MYR4K3KA')
    Traceback (most recent call last):
      ...
    ValueError: len(chash) must be 32; got 24: 'NWBNVXVK5DQGIOW7MYR4K3KA'

    For other protections against path traversal, see `FileStore.join()`.
    """
    if not isinstance(chash, basestring):
        raise TypeError(
            TYPE_ERROR % ('chash', basestring, type(chash), chash)
        )
    try:
        b32decode(chash)
    except TypeError as e:
        raise ValueError('chash: cannot b32decode %r: %s' % (chash, e))
    if len(chash) != B32LENGTH:
        raise ValueError('len(chash) must be %d; got %d: %r' %
            (B32LENGTH, len(chash), chash)
        )
    return chash


def hash_file(filename):
    """
    Compute the content-hash of the file at *filename*.
    """
    fp = open(filename, 'rb')
    h = HASH()
    while True:
        chunk = fp.read(CHUNK)
        if not chunk:
            break
        h.update(chunk)
    return b32encode(h.digest())


def hash_and_copy(src, dst):
    """
    Efficiently copy file from *src* to *dst* while computing content-hash.
    """
    src_fp = open(src, 'rb')
    dst_fp = open(dst, 'wb')
    h = HASH()
    while True:
        chunk = src_fp.read(CHUNK)
        if not chunk:
            break
        dst_fp.write(chunk)
        h.update(chunk)
    return b32encode(h.digest())


def quick_id(filename):
    """
    Compute a quick reasonably unique ID for the file at *filename*.
    """
    h = HASH()
    h.update(str(path.getsize(filename)).encode('utf-8'))
    h.update(open(filename, 'rb').read(QUICK_ID_CHUNK))
    return b32encode(h.digest())


class FileStore(object):
    """
    Arranges files in a special layout according to their content-hash.

    Security note: this class must be carefully designed to prevent path
    traversal!

    As the files are assumed to be read-only and unchanging, moving a file into
    its canonical location must be atomic.  There are 3 scenarios that must be
    considered:

        1. Initial import of file on same disk device as `FileStore` - this is
           the simplest case.  Imported file is hashed and then hard-linked into
           its canonical location.

        2. Initial import - as file will be copied from another disk device,
           requires the use of a temporary file.  When copy completes, file is
           is renamed to its canonical name.  During an initial import, the
           temporary file is named based on the quick_id(), which will be known
           prior to the import.

        3. Download - as download might fail and be resumed, requires a
           canonically named temporary file.  As content-hash is already known
           (file is already in library), the temporary file should be named by
           content-hash.  Once download completes and content is verified, file
           is renamed to its canonical name.

    In scenario (2) and (3), the filesize will be known when the temporary file
    is created, so an attempt is made to preallocate the entire file using the
    ``fallocate`` command.
    """

    def __init__(self, base):
        self.base = path.abspath(base)

    def join(self, *parts):
        """
        Safely join *parts* with base directory to prevent path traversal.

        For example:

        >>> fs = FileStore('/home/name/.dmedia')
        >>> fs.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        '/home/name/.dmedia/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

        However, a ``ValueError`` is raised if *parts* cause a traversal
        outside of the `FileStore` base directory:

        >>> fs.join('../.ssh/id_rsa')
        Traceback (most recent call last):
          ...
        ValueError: parts ('../.ssh/id_rsa',) cause path traversal to '/home/name/.ssh/id_rsa'
        """
        fullpath = path.normpath(path.join(self.base, *parts))
        if fullpath.startswith(self.base):
            return fullpath
        raise ValueError('parts %r cause path traversal to %r' %
            (parts, fullpath)
        )

    @staticmethod
    def relpath(chash, extension=None):
        """
        Relative path components for file with *chash*, ending with *extension*.

        For example:

        >>> fs = FileStore('/foo')
        >>> fs.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        >>> fs.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', extension='txt')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt')
        """
        dname = chash[:2]
        fname = chash[2:]
        if extension:
            return (dname, '.'.join((fname, extension)))
        return (dname, fname)

    def path(self, chash, extension=None):
        """
        Returns path of file with *chash* and *extension*.

        For example:

        >>> fs = FileStore('/foo')
        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'
        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', extension='txt')
        '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'
        """
        return self.join(*self.relpath(chash, extension))

    def tmp(self, chash=None, quickid=None):
        """
        Returns path of temporary file by either *chash* or *quickid*.

        >>> fs = FileStore('/foo')
        >>> fs.tmp(chash='OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE')
        '/foo/downloads/OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE'
        >>> fs.tmp(quickid='GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN')
        '/foo/imports/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN'
        """
        if chash:
            return self.join('downloads', safehash(chash))
        if quickid:
            return self.join('imports', safehash(quickid))
        raise TypeError('must provide either `chash` or `quickid`')

    def allocate_tmp(self, chash=None, quickid=None, size=None):
        tmp = self.tmp(chash, quickid)
        parent = path.dirname(tmp)
        if not path.exists(parent):
            os.makedirs(parent)
        if isinstance(size, int) and size > 0:
            try:
                check_call([FALLOCATE, '-l', str(size), tmp])
            except CalledProcessError as e:
                pass
        return tmp






#fallocate -l 340064241 MVI_6172.MOV
