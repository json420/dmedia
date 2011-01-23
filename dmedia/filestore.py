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
Store media files in a special layout according to their content hash.

Security note: this module must be carefully designed to prevent path traversal!
Two lines of defense are used:

    * `safe_b32()` and `safe_ext()` validate the (assumed) untrusted *chash*,
      *quickid*, and *ext* values

    * `FileStore.join()` and `FileStore.create_parent()` check the paths they
       create to insure that the path did not traverse outside of the file store
       base directory

Either should fully prevent path traversal but are used together for extra
safety.
"""

import os
from os import path
from hashlib import sha1 as HASH
from base64 import b32encode, b32decode
from string import ascii_lowercase, digits
import logging
from subprocess import check_call, CalledProcessError
from threading import Thread
from Queue import Queue
from .errors import AmbiguousPath, DuplicateFile
from .constants import CHUNK_SIZE, LEAF_SIZE


chars = frozenset(ascii_lowercase + digits)
B32LENGTH = 32  # Length of base32-encoded hash
CHUNK = 2 ** 20  # Read in chunks of 1 MiB
QUICK_ID_CHUNK = 2 ** 20  # Amount to read for quick_id()
FALLOCATE = '/usr/bin/fallocate'
TYPE_ERROR = '%s: need a %r; got a %r: %r'  # Standard TypeError message


def safe_open(filename, mode):
    """
    Only open file if *filename* is an absolute normalized path.

    This is to help protect against path-traversal attacks and to prevent use of
    ambiguous relative paths.

    If *filename* is not an absolute normalized path, `AmbiguousPath` is raised:

    >>> safe_open('/foo/../root', 'rb')
    Traceback (most recent call last):
      ...
    AmbiguousPath: filename '/foo/../root' resolves to '/root'

    Otherwise returns a ``file`` instance created with ``open()``.
    """
    if path.abspath(filename) != filename:
        raise AmbiguousPath(filename=filename, abspath=path.abspath(filename))
    return open(filename, mode)


def safe_ext(ext):
    """
    Verify that extension *ext* contains only lowercase ascii letters, digits.

    A malicious *ext* could cause path traversal or other security gotchas,
    thus this sanity check.  When *wav* is valid, it is returned unchanged:

    >>> safe_ext('ogv')
    'ogv'

    However, when *ext* does not conform, a ``TypeError`` or ``ValueError`` is
    raised:

    >>> safe_ext('/../.ssh')
    Traceback (most recent call last):
      ...
    ValueError: ext: can only contain ascii lowercase, digits; got '/../.ssh'

    Also see `safe_b32()`.
    """
    if not isinstance(ext, basestring):
        raise TypeError(
            TYPE_ERROR % ('ext', basestring, type(ext), ext)
        )
    if not chars.issuperset(ext):
        raise ValueError(
            'ext: can only contain ascii lowercase, digits; got %r' % ext
        )
    return ext


def safe_b32(b32):
    """
    Verify that *b32* is valid base32-encoding and correct length.

    A malicious *b32* could cause path traversal or other security gotchas,
    thus this sanity check.  When *b2* is valid, it is returned unchanged:

    >>> safe_b32('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
    'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

    However, when *b32* does not conform, a ``TypeError`` or ``ValueError`` is
    raised:

    >>> safe_b32('NWBNVXVK5DQGIOW7MYR4K3KA')
    Traceback (most recent call last):
      ...
    ValueError: len(b32) must be 32; got 24: 'NWBNVXVK5DQGIOW7MYR4K3KA'

    Also see `safe_ext()`.
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


class HashList(object):
    """
    Simple hash-list (a 1-deep tree-hash).

    For swarm upload/download, we need to keep the content hashes of the
    individual leaves, a list of which is available via the `HashList.leaves`
    attribute after `HashList.run()` has been called.

    The effective content-hash for the entire file is a hash of the leaf hashes
    concatenated together.  This is handy because it gives us a
    cryptographically strong way to associate individual leaves with the file
    "_id".  This is important because otherwise malicious peers could pollute
    the network with invalid leaves, but victims wouldn't know anything was
    wrong till the entire file was downloaded.  The whole file would fail to
    verify, and worse, the victim would have no way of knowing which leaves were
    invalid.

    In order to maximize IO utilization, the hash is computed in two threads.
    The main thread reads chunks from *src_fp* and puts them into a queue.  The
    2nd thread gets chunks from the queue, updates the hash, and then optionally
    writes the chunk to *dst_fp* if one was provided when the `HashList` was
    created.

    For some background, see:

        https://bugs.launchpad.net/dmedia/+bug/704272

    For more information about hash-lists and tree-hashes, see:

      http://en.wikipedia.org/wiki/Hash_list

      http://en.wikipedia.org/wiki/Tree_hash
    """

    def __init__(self, src_fp, dst_fp=None, leaf_size=LEAF_SIZE):
        if not isinstance(src_fp, file):
            raise TypeError(
                TYPE_ERROR % ('src_fp', file, type(src_fp), src_fp)
            )
        if src_fp.mode != 'rb':
            raise ValueError(
                "src_fp: mode must be 'rb'; got %r" % src_fp.mode
            )
        if dst_fp is not None:
            if not isinstance(dst_fp, file):
                raise TypeError(
                    TYPE_ERROR % ('dst_fp', file, type(dst_fp), dst_fp)
                )
            if dst_fp.mode not in ('wb', 'r+b'):
                raise ValueError(
                    "dst_fp: mode must be 'wb' or 'r+b'; got %r" % dst_fp.mode
                )
        self.src_fp = src_fp
        self.dst_fp = dst_fp
        self.leaf_size = leaf_size
        self.file_size = os.fstat(src_fp.fileno()).st_size
        self.h = HASH()
        self.leaves = []
        self.q = Queue(4)
        self.thread = Thread(target=self.hashing_thread)
        self.thread.daemon = True
        self.__ran = False

    def update(self, chunk):
        """
        Update hash with *chunk*, optionally write to dst_fp.

        This will append the content-hash of *chunk* to ``HashList.leaves`` and
        update the top-hash.

        If the `HashList` was created with a *dst_fp*, *chunk* will be will be
        written to *dst_fp*.

        `HashList.hashing_thread()` calls this method once for each chunk in the
        queue.  This functionality is in its own method simply to make testing
        easier.
        """
        digest = HASH(chunk).digest()
        self.h.update(digest)
        self.leaves.append(b32encode(digest))
        if self.dst_fp is not None:
            self.dst_fp.write(chunk)

    def hashing_thread(self):
        while True:
            chunk = self.q.get()
            if not chunk:
                break
            self.update(chunk)

    def run(self):
        assert self.__ran is False
        self.__ran = True
        self.src_fp.seek(0)  # Make sure we are at beginning of file
        self.thread.start()
        while True:
            chunk = self.src_fp.read(self.leaf_size)
            self.q.put(chunk)
            if not chunk:
                break
        self.thread.join()
        if self.dst_fp is not None:
            os.fchmod(self.dst_fp.fileno(), 0o444)
        return b32encode(self.h.digest())


def hash_file(fp):
    """
    Compute the content-hash of the open file *fp*.
    """
    if not isinstance(fp, file):
        raise TypeError(
            TYPE_ERROR % ('fp', file, type(fp), fp)
        )
    if fp.mode != 'rb':
        raise ValueError("fp: must be opened in mode 'rb'; got %r" % fp.mode)
    fp.seek(0)  # Make sure we are at beginning of file
    h = HASH()
    while True:
        chunk = fp.read(CHUNK)
        if not chunk:
            break
        h.update(chunk)
    return b32encode(h.digest())


def hash_and_copy(src_fp, dst_fp):
    """
    Efficiently copy file *src_fp* to *dst_fp* while computing content-hash.
    """
    if not isinstance(src_fp, file):
        raise TypeError(
            TYPE_ERROR % ('src_fp', file, type(src_fp), src_fp)
        )
    if src_fp.mode != 'rb':
        raise ValueError(
            "src_fp: must be opened in mode 'rb'; got %r" % src_fp.mode
        )
    if not isinstance(dst_fp, file):
        raise TypeError(
            TYPE_ERROR % ('dst_fp', file, type(dst_fp), dst_fp)
        )
    if dst_fp.mode not in ('wb', 'r+b'):
        raise ValueError(
            "dst_fp: must be opened in mode 'wb' or 'r+b'; got %r" % dst_fp.mode
        )
    dst_size = os.fstat(dst_fp.fileno()).st_size
    if dst_fp.mode == 'r+b':
        assert dst_size == os.fstat(src_fp.fileno()).st_size
    else:
        assert dst_size == 0
    src_fp.seek(0)  # Make sure we are at beginning of file
    h = HASH()
    while True:
        chunk = src_fp.read(CHUNK)
        if not chunk:
            break
        dst_fp.write(chunk)
        h.update(chunk)
    os.fchmod(dst_fp.fileno(), 0o444)
    return b32encode(h.digest())


def quick_id(fp):
    """
    Compute a quick reasonably unique ID for the open file *fp*.
    """
    if not isinstance(fp, file):
        raise TypeError(
            TYPE_ERROR % ('fp', file, type(fp), fp)
        )
    if fp.mode != 'rb':
        raise ValueError("fp: must be opened in mode 'rb'; got %r" % fp.mode)
    fp.seek(0)  # Make sure we are at beginning of file
    h = HASH()
    size = os.fstat(fp.fileno()).st_size
    h.update(str(size).encode('utf-8'))
    h.update(fp.read(QUICK_ID_CHUNK))
    return b32encode(h.digest())


class FileStore(object):
    """
    Arranges files in a special layout according to their content-hash.

    Security note: this class must be carefully designed to prevent path
    traversal!

    To create a `FileStore`, you give it the directory that will be its base on
    the filesystem:

    >>> fs = FileStore('/home/user/.dmedia')
    >>> fs.base
    '/home/user/.dmedia'

    You can add files to the store using `FileStore.import_file()`:

    >>> src_fp = open('/my/movie/MVI_5751.MOV', 'rb')  #doctest: +SKIP
    >>> fs.import_file(src_fp, quick_id(src_fp), 'mov')  #doctest: +SKIP
    ('HIGJPQWY4PI7G7IFOB2G4TKY6PMTJSI7', 'copied')

    And when you have the content-hash and extension, you can retrieve the full
    path of the file using `FileStore.path()`:

    >>> fs.path('HIGJPQWY4PI7G7IFOB2G4TKY6PMTJSI7', 'mov')
    '/home/user/.dmedia/HI/GJPQWY4PI7G7IFOB2G4TKY6PMTJSI7.mov'

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
        chash = safe_b32(chash)
        dname = chash[:2]
        fname = chash[2:]
        if ext:
            return (dname, '.'.join((fname, safe_ext(ext))))
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
            fname = safe_b32(quickid)
        elif chash:
            dname = 'downloads'
            fname = safe_b32(chash)
        else:
            raise TypeError('must provide either `chash` or `quickid`')
        if ext:
            return (dname, '.'.join((fname, safe_ext(ext))))
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

        Or Likewise if an absolute path is included in *parts*:

        >>> fs.join('NW', '/etc', 'ssh')
        Traceback (most recent call last):
          ...
        ValueError: parts ('NW', '/etc', 'ssh') cause path traversal to '/etc/ssh'

        Also see `FileStore.create_parent()`.
        """
        fullpath = path.normpath(path.join(self.base, *parts))
        if fullpath.startswith(self.base):
            return fullpath
        raise ValueError('parts %r cause path traversal to %r' %
            (parts, fullpath)
        )

    def create_parent(self, filename):
        """
        Safely create the directory containing *filename*.

        To prevent path traversal attacks, this method will only create
        directories within the `FileStore` base directory.  For example:

        >>> fs = FileStore('/foo')
        >>> fs.create_parent('/bar/my/movie.ogv')
        Traceback (most recent call last):
          ...
        ValueError: Wont create '/bar/my' outside of base '/foo' for file '/bar/my/movie.ogv'

        It also protects against malicious filenames like this:

        >>> fs.create_parent('/foo/my/../../bar/movie.ogv')
        Traceback (most recent call last):
          ...
        ValueError: Wont create '/bar' outside of base '/foo' for file '/foo/my/../../bar/movie.ogv'

        If doesn't already exists, the directory containing *filename* is
        created.  Returns the directory containing *filename*.

        Also see `FileStore.join()`.
        """
        containing = path.dirname(path.abspath(filename))
        if not containing.startswith(self.base):
            raise ValueError('Wont create %r outside of base %r for file %r' %
                (containing, self.base, filename)
            )
        if not path.exists(containing):
            os.makedirs(containing)
        return containing

    def path(self, chash, ext=None, create=False):
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
        filename = self.join(*self.relpath(chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def tmp(self, quickid=None, chash=None, ext=None, create=False):
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
        filename = self.join(*self.reltmp(quickid, chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def allocate_tmp(self, quickid=None, chash=None, ext=None, size=None):
        """
        Create parent directory and attempt to preallocate temporary file.

        The temporary filename is constructed by calling `FileStore.tmp()` with
        ``create=True``, which will create the temporary file's containing
        directory if it doesn't already exist.

        If *size* is a nonzero ``int``, an attempt is made to make a persistent
        pre-allocation with the ``fallocate`` command, something like this:

            fallocate -l 4284061229 HIGJPQWY4PI7G7IFOB2G4TKY6PMTJSI7.mov

        Returns a ``file`` instance opened with ``open()``.
        """
        tmp = self.tmp(quickid, chash, ext, create=True)
        if isinstance(size, int) and size > 0 and path.isfile(FALLOCATE):
            try:
                check_call([FALLOCATE, '-l', str(size), tmp])
                assert path.getsize(tmp) == size
                return open(tmp, 'r+b')
            except CalledProcessError as e:
                pass
        return open(tmp, 'wb')

    def import_file(self, src_fp, quickid, ext=None):
        """
        Atomically copy open file *src_fp* into this file store.

        The method will compute the content-hash of *src_fp* as it copies it to
        a temporary file within this store.  Once the copying is complete, the
        file will be renamed to its canonical location in the store, thus
        ensuring an atomic operation.

        A `DuplicatedFile` exception will be raised if the file already exists
        in this store.

        This method returns a ``(chash, leaves)`` tuple with the content hash
        (top-hash) and a list of the content hashes of the leaves.  See
        `HashList` for details.

        Note that *src_fp* must have been opened in ``'rb'`` mode.

        :param src_fp: A ``file`` instance created with ``open()``
        :param quickid: The quickid computed by ``quick_id()``
        :param ext: The file's extension, e.g., ``'ogv'``
        """
        size = os.fstat(src_fp.fileno()).st_size
        tmp_fp = self.allocate_tmp(quickid=quickid, ext=ext, size=size)
        h = HashList(src_fp, tmp_fp)
        chash = h.run()
        dst = self.path(chash, ext, create=True)
        if path.exists(dst):
            raise DuplicateFile(chash=chash, src=src_fp.name, dst=dst)
        os.rename(tmp_fp.name, dst)
        return (chash, h.leaves)
