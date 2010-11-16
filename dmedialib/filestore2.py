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
Store media files in a special layout according to their content hash.

Security note: this module must be carefully designed to prevent path traversal!
Two lines of defense are used:

    * `issafe()` - ensures that a chash is well-formed

    * `FileStore.join()` - used in place of ``path.join()``, detects when
      untrusted portions of path cause a path traversal

Either should fully prevent path traversal but are used together for extra
safety.
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


def issafe(b32):
    """
    Verify that *b32* is valid base32-encoding and correct length.

    A malicious *b32* could cause path traversal or other security gotchas,
    thus this sanity check.  When *b2* is valid, it is returned unchanged:

    >>> issafe('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
    'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

    However, when *b32* does not conform, a ``TypeError`` or ``ValueError`` is
    raised:

    >>> issafe('NWBNVXVK5DQGIOW7MYR4K3KA')
    Traceback (most recent call last):
      ...
    ValueError: len(b32) must be 32; got 24: 'NWBNVXVK5DQGIOW7MYR4K3KA'

    For other protections against path traversal, see `FileStore.join()`.
    """
    if not isinstance(b32, basestring):
        raise TypeError(
            TYPE_ERROR % ('b32', basestring, type(b32), b32)
        )
    try:
        b32decode(b32)
    except TypeError as e:
        raise ValueError('b32: cannot b32decode %r: %s' % (b32, e))
    if len(b32) != B32LENGTH:
        raise ValueError('len(b32) must be %d; got %d: %r' %
            (B32LENGTH, len(b32), b32)
        )
    return b32


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

    @staticmethod
    def relpath(chash, ext=None):
        """
        Relative path components for file with *chash*, ending with *ext*.

        For example:

        >>> FileStore.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')

        Or with the file extension *ext*:

        >>> FileStore.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='txt')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt')

        Also see `FileStore.reltmp()`.
        """
        chash = issafe(chash)
        dname = chash[:2]
        fname = chash[2:]
        if ext:
            return (dname, '.'.join((fname, ext)))
        return (dname, fname)

    @staticmethod
    def reltmp(quickid=None, chash=None, ext=None):
        """
        Relative path components of temporary file.

        Temporary files are created in either an ``'imports'`` or
        ``'downloads'`` sub-directory based on whether you're doing an initial
        import or downloading a file already present in the library.

        For initial imports, provide the *quickid* like this:

        >>> FileStore.reltmp(quickid='GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN', ext='mov')
        ('imports', 'GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN.mov')

        For downloads, the content-hash will already be known, so provide the
        *chash* like this:

        >>> FileStore.reltmp(chash='OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE', ext='mov')
        ('downloads', 'OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE.mov')

        Also see `FileStore.relpath()`.
        """
        if quickid:
            dname = 'imports'
            fname = issafe(quickid)
        elif chash:
            dname = 'downloads'
            fname = issafe(chash)
        else:
            raise TypeError('must provide either `chash` or `quickid`')
        if ext:
            return (dname, '.'.join((fname, ext)))
        return (dname, fname)

    def join(self, *parts):
        """
        Safely join *parts* with base directory to prevent path traversal.

        For security reasons, it's very important that you use this method
        rather than ``path.join()`` directly.  This method will prevent
        directory/path traversal, ``path.join()`` will not.

        For example:

        >>> fs = FileStore('/home/name/.dmedia')
        >>> fs.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        '/home/name/.dmedia/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

        However, a ``ValueError`` is raised if *parts* cause a path traversal
        outside of the `FileStore` base directory:

        >>> fs.join('../.ssh/id_rsa')
        Traceback (most recent call last):
          ...
        ValueError: parts ('../.ssh/id_rsa',) cause path traversal to '/home/name/.ssh/id_rsa'

        Or Likewise if an absolute

        For other protections against path traversal, see `issafe()`.
        """
        fullpath = path.normpath(path.join(self.base, *parts))
        if fullpath.startswith(self.base):
            return fullpath
        raise ValueError('parts %r cause path traversal to %r' %
            (parts, fullpath)
        )

    def create_parent(self, *parts):
        """
        Safely join *parts* with base and create containing directory if needed.

        This method will construct an absolute filename using `FileStore.join()`
        and then create this file's containing directory if it doesn't already
        exist.

        Returns the absolute filename.
        """
        filename = self.join(*parts)
        containing = path.dirname(filename)
        if not path.exists(containing):
            os.makedirs(containing)
        return filename

    def path(self, chash, ext=None):
        """
        Returns path of file with content-hash *chash* and extension *ext*.

        For example:

        >>> fs = FileStore('/foo')
        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

        Or with a file extension:

        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='txt')
        '/foo/NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'
        """
        return self.join(*self.relpath(chash, ext))

    def tmp(self, quickid=None, chash=None, ext=None):
        """
        Returns path of temporary file.

        Temporary files are created in either an ``'imports'`` or a
        ``'downloads'`` sub-directory based on whether you're doing an initial
        import or downloading a file already present in the library.

        For initial imports, provide the *quickid* like this:

        >>> fs = FileStore('/foo')
        >>> fs.tmp(quickid='GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN', ext='mov')
        '/foo/imports/GJ4AQP3BK3DMTXYOLKDK6CW4QIJJGVMN.mov'

        For downloads, the content-hash will already be known, so provide the
        *chash* like this:

        >>> fs.tmp(chash='OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE', ext='mov')
        '/foo/downloads/OMLUWEIPEUNRGYMKAEHG3AEZPVZ5TUQE.mov'

        Also see `FileStore.path()`, `FileStore.allocate_tmp()`.
        """
        return self.join(*self.reltmp(quickid, chash, ext))

    def allocate_tmp(self, quickid=None, chash=None, ext=None, size=None):
        """
        Create parent directory and attempt to preallocate temporary file.

        The temporary filename is constructed by called `FileStore.tmp()`.
        Then the temporary file's containing directory is created if it doesn't
        already exist.

        If *size* is a nonzero ``int``, an attempt is made to make a persistent
        pre-allocation with the ``fallocate`` command, something like this:

            fallocate -l 4284061229 HIGJPQWY4PI7G7IFOB2G4TKY6PMTJSI7.mov

        The temporary filename is returned.
        """
        tmp = self.create_parent(*self.reltmp(quickid, chash, ext))
        if isinstance(size, int) and size > 0 and path.isfile(FALLOCATE):
            try:
                check_call([FALLOCATE, '-l', str(size), tmp])
                assert path.getsize(tmp) == size
            except CalledProcessError as e:
                pass
        return tmp
