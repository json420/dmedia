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
import stat
import tempfile
from hashlib import sha1 as HASH
from base64 import b32encode, b32decode
import json
import re
import logging
from subprocess import check_call, CalledProcessError
from threading import Thread
from Queue import Queue
from .schema import create_store
from .errors import AmbiguousPath, FileStoreTraversal
from .errors import DuplicateFile, IntegrityError
from .constants import LEAF_SIZE, TRANSFERS_DIR, IMPORTS_DIR, TYPE_ERROR, EXT_PAT

B32LENGTH = 32  # Length of base32-encoded hash
QUICK_ID_CHUNK = 2 ** 20  # Amount to read for quick_id()
FALLOCATE = '/usr/bin/fallocate'
EXT_RE = re.compile(EXT_PAT)


def safe_path(pathname):
    """
    Ensure that *pathname* is a normalized absolute path.

    This is to help protect against path-traversal attacks and to prevent use of
    ambiguous relative paths.

    For example, if *pathname* is not a normalized absolute path,
    `AmbiguousPath` is raised:

    >>> safe_path('/foo/../root')
    Traceback (most recent call last):
      ...
    AmbiguousPath: '/foo/../root' resolves to '/root'


    Otherwise *pathname* is returned unchanged:

    >>> safe_path('/foo/bar')
    '/foo/bar'


    Also see `safe_open()`.
    """
    if path.abspath(pathname) != pathname:
        raise AmbiguousPath(pathname=pathname, abspath=path.abspath(pathname))
    return pathname


def safe_open(filename, mode):
    """
    Only open file if *filename* is a normalized absolute path.

    This is to help protect against path-traversal attacks and to prevent use of
    ambiguous relative paths.

    Prior to opening the file, *filename* is checked with `safe_path()`.  If
    it's not an absolute normalized path, `AmbiguousPath` is raised:

    >>> safe_open('/foo/../root', 'rb')
    Traceback (most recent call last):
      ...
    AmbiguousPath: '/foo/../root' resolves to '/root'


    Otherwise returns a ``file`` instance created with ``open()``.
    """
    return open(safe_path(filename), mode)


def safe_ext(ext):
    r"""
    Verify that extension *ext* contains only lowercase ascii letters, digits.

    A malicious *ext* could cause path traversal or other security gotchas,
    thus this sanity check.  When *wav* is valid, it is returned unchanged:

    >>> safe_ext('ogv')
    'ogv'
    >>> safe_ext('tar.gz')
    'tar.gz'

    However, when *ext* does not conform, a ``TypeError`` or ``ValueError`` is
    raised:

    >>> safe_ext('/../.ssh')
    Traceback (most recent call last):
      ...
    ValueError: ext '/../.ssh' does not match pattern '^[a-z0-9]+(\\.[a-z0-9]+)?$'

    Also see `safe_b32()`.
    """
    if not isinstance(ext, basestring):
        raise TypeError(
            TYPE_ERROR % ('ext', basestring, type(ext), ext)
        )
    if not EXT_RE.match(ext):
        raise ValueError(
            'ext %r does not match pattern %r' % (ext, EXT_PAT)
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
        self.leaves.append(digest)
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


def pack_leaves(leaves, digest_bytes=20):
    for (i, leaf) in enumerate(leaves):
        if len(leaf) != digest_bytes:
            raise ValueError('digest_bytes=%d, but len(leaves[%d]) is %d' % (
                    digest_bytes, i, len(leaf)
                )
            )
    return ''.join(leaves)


def unpack_leaves(data, digest_bytes=20):
    if len(data) % digest_bytes != 0:
        raise ValueError(
            'len(data)=%d, not multiple of digest_bytes=%d' % (
                len(data), digest_bytes
            )
        )
    return [
        data[i*digest_bytes : (i+1)*digest_bytes]
        for i in xrange(len(data) / digest_bytes)
    ]


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


def fallocate(size, filename):
    """
    Attempt to efficiently preallocate file *filename* to *size* bytes.

    If the fallocate command is available, it will always at least create an
    empty file (the equivalent of ``touch filename``), even the file-system
    doesn't support pre-allocation.
    """
    if not isinstance(size, (int, long)):
        raise TypeError(
            TYPE_ERROR % ('size', (int, long), type(size), size)
        )
    if size <= 0:
        raise ValueError('size must be >0; got %r' % size)
    filename = safe_path(filename)
    if not path.isfile(FALLOCATE):
        return None
    try:
        check_call([FALLOCATE, '-l', str(size), filename])
        return True
    except CalledProcessError:
        return False



class FileStore(object):
    """
    Arranges files in a special layout according to their content-hash.

    Security note: this class must be carefully designed to prevent path
    traversal!

    To create a `FileStore`, you give it the directory that will be its base on
    the filesystem:

    >>> fs = FileStore('/home/jderose/.dmedia')  #doctest: +SKIP
    >>> fs.base  #doctest: +SKIP
    '/home/jderose/.dmedia'

    If you don't supply *base*, a temporary directory will be created for you:

    >>> fs = FileStore()
    >>> fs.base  #doctest: +ELLIPSIS
    '/tmp/store...'

    You can add files to the store using `FileStore.import_file()`:

    >>> from dmedia.tests import sample_mov  # Sample .MOV file
    >>> src_fp = open(sample_mov, 'rb')
    >>> fs.import_file(src_fp, 'mov')  #doctest: +ELLIPSIS
    ('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', [...])

    And when you have the content-hash and extension, you can retrieve the full
    path of the file using `FileStore.path()`:

    >>> fs.path('HIGJPQWY4PI7G7IFOB2G4TKY6PMTJSI7', 'mov')  #doctest: +ELLIPSIS
    '/tmp/store.../HI/GJPQWY4PI7G7IFOB2G4TKY6PMTJSI7.mov'

    As the files are assumed to be read-only and unchanging, moving a file into
    its canonical location must be atomic.  There are 2 scenarios that must be
    considered:

        1. Imports - we compute the content-hash as we copy the file into the
           `FileStore`, so this requires a randomly-named temporary file.  When
           the copy completes, file is renamed to its canonical name.

        2. Transfers - as uploads/downloads might stop or fail and then be
           resumed, this requires a canonically-named temporary file.  As the
           file content-hash is already known (we have its meta-data in
           CouchDB), the temporary file is named by the content-hash.  Once
           download completes, file is renamed to its canonical name.

    In both scenarios, the file size will be known when the temporary file is
    created, so an attempt is made to preallocate the entire file using the
    `fallocate()` function, which calls the Linux ``fallocate`` command.
    """

    def __init__(self, base=None, machine_id=None):
        if base is None:
            base = tempfile.mkdtemp(prefix='store.')
        self.base = safe_path(base)
        try:
            os.makedirs(self.base)
        except OSError:
            pass
        if not path.isdir(self.base):
            raise ValueError('%s.base not a directory: %r' %
                (self.__class__.__name__, self.base)
            )
        self.record = path.join(self.base, 'store.json')
        try:
            fp = open(self.record, 'rb')
            doc = json.load(fp)
        except IOError:
            fp = open(self.record, 'wb')
            doc = create_store(self.base, machine_id)
            json.dump(doc, fp, sort_keys=True, indent=4)
        fp.close()
        self._doc = doc
        self._id = doc['_id']

    @staticmethod
    def relpath(chash, ext=None):
        """
        Relative path of file with *chash*, ending with *ext*.

        For example:

        >>> FileStore.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')

        Or with the file extension *ext*:

        >>> FileStore.relpath('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov')
        ('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')

        Also see `FileStore.reltmp()`.
        """
        chash = safe_b32(chash)
        dname = chash[:2]
        fname = chash[2:]
        if ext:
            return (dname, '.'.join((fname, safe_ext(ext))))
        return (dname, fname)

    @staticmethod
    def reltemp(chash, ext=None):
        """
        Relative path of temporary file with *chash*, ending with *ext*.

        For example:

        >>> FileStore.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')

        Or with the file extension *ext*:

        >>> FileStore.reltemp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov')
        ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')

        Also see `FileStore.relpath()`.
        """
        chash = safe_b32(chash)
        if ext:
            return (TRANSFERS_DIR, '.'.join([chash, safe_ext(ext)]))
        return (TRANSFERS_DIR, chash)

    def check_path(self, pathname):
        """
        Verify that *pathname* in inside this filestore base directory.
        """
        abspath = path.abspath(pathname)
        if abspath.startswith(self.base + os.sep):
            return abspath
        raise FileStoreTraversal(
            pathname=pathname, base=self.base, abspath=abspath
        )

    def join(self, *parts):
        """
        Safely join *parts* with base directory to prevent path traversal.

        For security reasons, it's very important that you use this method
        rather than ``path.join()`` directly.  This method will prevent path
        traversal attacks, ``path.join()`` will not.

        For example:

        >>> fs = FileStore()
        >>> fs.join('NW', 'BNVXVK5DQGIOW7MYR4K3KA5K22W7NW')  #doctest: +ELLIPSIS
        '/tmp/store.../NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'


        However, a `FileStoreTraversal` is raised if *parts* cause a path
        traversal outside of the `FileStore` base directory:

        >>> fs.join('../ssh')  #doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        FileStoreTraversal: '/tmp/ssh' outside base '/tmp/store...'


        Or Likewise if an absolute path is included in *parts*:

        >>> fs.join('NW', '/etc', 'ssh')  #doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        FileStoreTraversal: '/etc/ssh' outside base '/tmp/store...'


        Also see `FileStore.create_parent()`.
        """
        fullpath = path.join(self.base, *parts)
        return self.check_path(fullpath)

    def create_parent(self, filename):
        """
        Safely create the directory containing *filename*.

        To prevent path traversal attacks, this method will only create
        directories within the `FileStore` base directory.  For example:

        >>> fs = FileStore()
        >>> fs.create_parent('/foo/my.ogv')  #doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        FileStoreTraversal: '/foo/my.ogv' outside base '/tmp/store...'


        It also protects against malicious filenames like this:

        >>> fs.create_parent('/foo/../bar/my.ogv')  #doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        FileStoreTraversal: '/bar/my.ogv' outside base '/tmp/store...'


        If doesn't already exists, the directory containing *filename* is
        created.  Returns the directory containing *filename*.

        Also see `FileStore.join()`.
        """
        filename = self.check_path(filename)
        containing = path.dirname(filename)
        if not path.exists(containing):
            os.makedirs(containing)
        return containing

    def path(self, chash, ext=None, create=False):
        """
        Returns path of file with content-hash *chash* and extension *ext*.

        For example:

        >>> fs = FileStore()
        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')  #doctest: +ELLIPSIS
        '/tmp/store.../NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW'


        Or with a file extension:

        >>> fs.path('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'txt')  #doctest: +ELLIPSIS
        '/tmp/store.../NW/BNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'


        If called with ``create=True``, the parent directory is created with
        `FileStore.create_parent()`.
        """
        filename = self.join(*self.relpath(chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def temp(self, chash, ext=None, create=False):
        """
        Returns path of temporary file with *chash*, ending with *ext*.

        These temporary files are used for file transfers between dmedia peers,
        in which case the content-hash is already known.  For example:

        >>> fs = FileStore()
        >>> fs.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'


        Or with a file extension:

        >>> fs.temp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'txt')  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'


        If called with ``create=True``, the parent directory is created with
        `FileStore.create_parent()`.
        """
        filename = self.join(*self.reltemp(chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def allocate_for_transfer(self, size, chash, ext=None):
        filename = self.temp(chash, ext, create=True)
        fallocate(size, filename)
        try:
            fp = open(filename, 'r+b')
            if os.fstat(fp.fileno()).st_size > size:
                fp.truncate(size)
            return fp
        except IOError:
            return open(filename, 'wb')

    def allocate_for_import(self, size, ext=None):
        imports = self.join(IMPORTS_DIR)
        if not path.exists(imports):
            os.makedirs(imports)
        suffix = ('' if ext is None else '.' + ext)
        (fileno, filename) = tempfile.mkstemp(suffix=suffix, dir=imports)
        fallocate(size, filename)
        return open(filename, 'r+b')

    def finalize_transfer(self, chash, ext=None):
        """
        Move canonically named temporary file to its final canonical location.

        This method will check the content hash of the canonically-named
        temporary file with content hash *chash* and extension *ext*.  If the
        content hash is correct, it will do an ``os.fchmod()`` to set read-only
        permissions, and then rename the file into its canonical location.

        If the content hash is incorrect, `IntegrityError` is raised.  If the
        canonical file already exists, `DuplicateFile` is raised.  Lastly, if
        the temporary does not exist, ``IOError`` is raised.

        This method will typically be used with the BitTorrent downloader or
        similar, in which case the content hash will be known prior to
        downloading.  The downloader will first determine the canonical
        temporary file name, like this:

        >>> fs = FileStore()
        >>> tmp = fs.temp('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov', create=True)
        >>> tmp  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/ZR765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'


        Then the downloader will write to the temporary file as it's being
        downloaded:

        >>> from dmedia.tests import sample_mov  # Sample .MOV file
        >>> src_fp = open(sample_mov, 'rb')
        >>> tmp_fp = open(tmp, 'wb')
        >>> while True:
        ...     chunk = src_fp.read(2**20)  # Read in 1MiB chunks
        ...     if not chunk:
        ...         break
        ...     tmp_fp.write(chunk)
        ...
        >>> tmp_fp.close()


        Finally, the downloader will move the temporary file into its canonical
        location:

        >>> dst = fs.finalize_transfer('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov')
        >>> dst  #doctest: +ELLIPSIS
        '/tmp/store.../ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'


        Note above that this method returns the full path of the canonically
        named file.
        """
        # Open temporary file and check content hash:
        tmp = self.temp(chash, ext)
        tmp_fp = open(tmp, 'rb')
        h = HashList(tmp_fp)
        got = h.run()
        if got != chash:
            raise IntegrityError(got=got, expected=chash, filename=tmp_fp.name)

        # Get canonical name, check for duplicate:
        dst = self.path(chash, ext, create=True)
        if path.exists(dst):
            raise DuplicateFile(chash=chash, src=tmp_fp.name, dst=dst)

        # Set file to read-only and rename into canonical location
        os.fchmod(tmp_fp.fileno(), 0o444)
        os.rename(tmp_fp.name, dst)

        # Return canonical filename:
        return dst

    def tmp_rename(self, tmp_fp, chash, ext=None):
        # Validate tmp_fp:
        if not isinstance(tmp_fp, file):
            raise TypeError(
                TYPE_ERROR % ('tmp_fp', file, type(tmp_fp), tmp_fp)
            )
        if tmp_fp.mode not in ('rb', 'wb', 'r+b'):
            raise ValueError(
                "tmp_fp: mode must be 'rb', 'wb', or 'r+b'; got %r" % tmp_fp.mode
            )
        self.check_path(tmp_fp.name)

        # Get canonical name, check for duplicate:
        dst = self.path(chash, ext, create=True)
        if path.exists(dst):
            raise DuplicateFile(chash=chash, src=tmp_fp.name, dst=dst)

        # Set file to read-only (0444) and rename into canonical location
        os.fchmod(tmp_fp.fileno(), stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        os.rename(tmp_fp.name, dst)

        # Return canonical filename:
        return dst

    def import_file(self, src_fp, ext=None):
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
        :param ext: The file's extension, e.g., ``'ogv'``
        """
        size = os.fstat(src_fp.fileno()).st_size
        tmp_fp = self.allocate_for_import(size, ext=ext)
        h = HashList(src_fp, tmp_fp)
        chash = h.run()
        dst = self.path(chash, ext, create=True)
        if path.exists(dst):
            raise DuplicateFile(chash=chash, src=src_fp.name, dst=dst)
        os.rename(tmp_fp.name, dst)
        return (chash, h.leaves)
