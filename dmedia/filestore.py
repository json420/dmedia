# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Akshat Jain <ssj6akshat1234@gmail.com)
#
# dmedia: distributed media library
# Copyright (C) 2010, 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Store files in a special layout according to their content-hash.

The `FileStore` is the heart of dmedia.  Files are assigned a canonical name
based on the file's content-hash, and are placed in a special layout within the
`FileStore` base directory.

The files in a `FileStore` are read-only... they must be as modifying a file
will change its content-hash.  The only way to modify a file is to copy the
original to a temporary file, modify it, and then place the new file into the
`FileStore`.  This might seem like an unreasonable restriction, but it perfectly
captures the use case dmedia is concerned with... a distributed library of media
files.

On the content-creation side, non-destructive editing is certainly the best
practice, especially in professional use cases.  On the content consumption
side, modifying a file is rather rare.  And the somewhat common use case --
modifying a file for the sake of updating metadata (say, EXIF) -- can instead be
accomplished by updating metadata in the corresponding CouchDB document.

Importantly, without the read-only restriction, it would be impossible to make a
distributed file system whose file operations remain robust and atomic in the
face of arbitrary and prolonged network outages.  True to its CouchDB
foundations, dmedia is designing with the assumption that network connectivity
is the exception rather than the rule.

Please read on for the rationale of some key `FileStore` design decisions...


Design Decision: base32-encoded content-hash
============================================

The `FileStore` layout was designed to allow the canonical filename to be
constructed from the content-hash in the simplest way possible, without
requiring any special decoding or encoding.  For this reason, the content-hash
(as stored in CouchDB) is base32-encoded.

Base32-encoding was chosen because:

    1. It's more compact than base16/hex

    2. It can be used to name files on case *insensitive* filesystems (whereas
       base64-encoding cannot)

Inside the `FileStore`, the first 2 characters of the content-hash are used for
the subdirectory name, and the remaining characters for the filename within that
subdirectory.  For example:

>>> from os import path
>>> chash = 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
>>> path.join('/foo', chash[:2], chash[2:])
'/foo/ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O'


Design Decision: canonical filenames have file extensions
=========================================================

Strictly speaking, there is no technical reason to include a file extension on
the canonical filenames.  However, there are some practical reasons that make
including the file extension worthwhile, despite additional complexity it adds
to the `FileStore` API.

Most importantly, it allows files in a `FileStore` layout to be served with the
correct Content-Type by a vanilla web-server.  A key design goal was to be able
to point, say, Apache at a dmedia `FileStore` directory have a useful dmedia
file server without requiring special Apache plugins for dmedia integration.

It also provides broader software compatibility as many applications and
libraries do rely on the file extension for type determination.  And the file
extension is helpful for developers, as a bit of intelligible information in
canonical filename will make the layout easier to explore, aid debugging.

The current `FileStore` always includes the file extension on the canonical name
when the extension is provided by the calling code.  However, the API is
designed to accommodate `FileStore` implementations that do not include the
file extension.  The API is also designed so that the calling code isn't
required to provide the file extension... say, if the extension was ever removed
from the CouchDB schema.

To accomplish this, files are identified by the content-hash and extension
together, and the extension is optional, defaulting to ``None``.  This is the
typical calling signature:

>>> def canonical(chash, ext=None):
...     pass

For example:

>>> FileStore.relpath('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O')
('ZR', '765XWSF6S7JQHLUI4GCG5BHGPE252O')
>>> FileStore.relpath('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'mov')
('ZR', '765XWSF6S7JQHLUI4GCG5BHGPE252O.mov')


Design Decision: security good, path traversals bad
===================================================

The `FileStore` is probably the most security sensitive part of dmedia in that
untrusted data (content-hash, file extension) is used to construct paths on the
filesystem.  This means that the `FileStore` must be carefully designed to
prevent path traversal attacks (aka directory traversal attacks).

Two lines of defense are used.  First, the content-hash and file extension are
validated with the following functions:

    * `safe_b32()` - validates the content-hash

    * `safe_ext()` - validates the file extension

Second, there are methods that ensure that paths constructed relative to the
`FileStore` base directory cannot be outside of the base directory:

    * `FileStore.check_path()` - ensures that a path is inside the base
       directory

    * `FileStore.join()` - creates a path relative to the base directory,
       ensures resulting path is inside the base directory

    * `FileStore.create_parent()` - creates a file's parent directory only if
       that parent directory is inside the base directory

Each line of defense is designed to fully prevent path traversals, assumes the
other defense doesn't exist or will fail.  Together, they should provide a
strong defense against path traversal attacks.

If you discover any security vulnerability in dmedia, please immediately file a
bug:

    https://bugs.launchpad.net/dmedia/+filebug
"""

import os
from os import path
import stat
import tempfile
from hashlib import sha1
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
from .constants import LEAF_SIZE, TYPE_ERROR, EXT_PAT
from .constants import TRANSFERS_DIR, IMPORTS_DIR, WRITES_DIR

B32LENGTH = 32  # Length of base32-encoded hash
QUICK_ID_CHUNK = 2 ** 20  # Amount to read for quick_id()
FALLOCATE = '/usr/bin/fallocate'
EXT_RE = re.compile(EXT_PAT)
log = logging.getLogger()


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


def tophash_personalization(file_size):
    """
    Personalize the top-hash with *file_size*.

    For example:

    >>> tophash_personalization(3141)
    'dmedia/tophash 3141'

    This is used to cryptographically tie ``doc['bytes']`` to ``doc['_id']``.
    You can't change the leaves or the file size without affecting the top-hash.

    The personalization is designed to be easy to implement in JavaScript.  For
    example, this is the equivalent JavaScript function:

        ::

            function tophash_personalization(file_size) {
                return ['dmedia/tophash', file_size].join(' ');
            }

    When hashing with Skein, this value would be used for the Skein
    personalization parameter.  See PySkein and the Skein specification for
    details:

        http://packages.python.org/pyskein/

        http://www.skein-hash.info/

    When hashing with sha1, the top-hash is calculated like this:

    >>> from hashlib import sha1
    >>> from base64 import b32encode
    >>> pers = tophash_personalization(3141)
    >>> leaves = b'pretend this is the concatenated leaves'
    >>> b32encode(sha1(pers + leaves).digest())  # The top-hash
    'M55ORBTYICEDQ2WUREDYIYYO6VUJ3R6S'

    :param file_size: the file size in bytes (an ``int``)
    """
    return ' '.join(['dmedia/tophash', str(file_size)]).encode('utf-8')


def tophash(file_size):
    """
    Initialize hash for a file that is *file_size* bytes.
    """
    return sha1(tophash_personalization(file_size))


def leafhash_personalization(file_size, leaf_index):
    """
    Personalize the leaf-hash with *file_size* and *leaf_index*.

    For example:

    >>> leafhash_personalization(3141, 0)
    'dmedia/leafhash 3141 0'

    :param file_size: the file size in bytes (an ``int``)
    :param leaf_index: the index of this leaf (an ``int``, starting at zero)
    """
    return ' '.join(
        ['dmedia/leafhash', str(file_size), str(leaf_index)]
    ).encode('utf-8')


def leafhash(file_size, leaf_index):
    """
    Initialize hash for the *leaf_index* leaf in a file of *file_size* bytes.
    """
    return sha1(leafhash_personalization(file_size, leaf_index))


class HashList(object):
    """
    Simple hash-list (a 1-deep tree-hash).

    For swarm upload/download, we need to keep the content hashes of the
    individual leaves, a list of which is available via the ``HashList.leaves``
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
        self.h = tophash(self.file_size)
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
        digest = sha1(chunk).digest()
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
        return b32encode(self.h.digest())


def pack_leaves(leaves, digest_bytes=20):
    """
    Pack leaves together into a ``bytes`` instance for CouchDB attachment.

    :param leaves: a ``list`` containing content-hash of each leaf in the file
        (content-hash is binary digest, not base32-encoded)
    :param digest_bytes: digest size in bytes; default is 20 (160 bits)
    """
    for (i, leaf) in enumerate(leaves):
        if len(leaf) != digest_bytes:
            raise ValueError('digest_bytes=%d, but len(leaves[%d]) is %d' % (
                    digest_bytes, i, len(leaf)
                )
            )
    return ''.join(leaves)


def unpack_leaves(data, digest_bytes=20):
    """
    Unpack binary *data* into a list of leaf digests.

    :param data: a ``bytes`` instance containing the packed leaf digests
    :param digest_bytes: digest size in bytes; default is 20 (160 bits)
    """
    assert isinstance(data, bytes)
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
    h = sha1()
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
    ('TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK', [...])

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

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    ############################################
    # Methods to prevent path traversals attacks
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


    #################################################
    # Methods for working with files in the FileStore
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

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        chash = safe_b32(chash)
        dname = chash[:2]
        fname = chash[2:]
        if ext:
            return (dname, '.'.join((fname, safe_ext(ext))))
        return (dname, fname)

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

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        :param create: if ``True``, create parent directory if it does not
            already exist; default is ``False``
        """
        filename = self.join(*self.relpath(chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def exists(self, chash, ext=None):
        """
        Return ``True`` if a file with *chash* and *ext* exists.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        return path.isfile(self.path(chash, ext))

    def open(self, chash, ext=None):
        """
        Open the file with *chash* and *ext* in ``'rb'`` mode.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        return open(self.path(chash, ext), 'rb')

    def verify(self, chash, ext=None):
        """
        Verify integrity of file with *chash* and *ext*.

        If the file's content-hash does not equal *chash*, an `IntegrityError`
        is raised.

        Otherwise, the open ``file`` is returned after calling ``file.seek(0)``
        to put read position back at the start of the file.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        src_fp = self.open(chash, ext)
        h = HashList(src_fp)
        got = h.run()
        if got != chash:
            raise IntegrityError(got=got, expected=chash, filename=src_fp.name)
        src_fp.seek(0)
        return src_fp

    def remove(self, chash, ext=None):
        """
        Delete file with *chash* and *ext* from underlying filesystem.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        filename = self.path(chash, ext)
        log.info('Deleting file %r from %r', filename, self)
        os.remove(filename)


    ###########################################################
    # Methods for working with temporary files in the FileStore
    @staticmethod
    def reltmp(chash, ext=None):
        """
        Relative path of temporary file with *chash*, ending with *ext*.

        For example:

        >>> FileStore.reltmp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')
        ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')

        Or with the file extension *ext*:

        >>> FileStore.reltmp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', ext='mov')
        ('transfers', 'NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.mov')

        Also see `FileStore.relpath()`.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        chash = safe_b32(chash)
        if ext:
            return (TRANSFERS_DIR, '.'.join([chash, safe_ext(ext)]))
        return (TRANSFERS_DIR, chash)

    def tmp(self, chash, ext=None, create=False):
        """
        Returns path of temporary file with *chash*, ending with *ext*.

        These temporary files are used for file transfers between dmedia peers,
        in which case the content-hash is already known.  For example:

        >>> fs = FileStore()
        >>> fs.tmp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW')  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW'

        Or with a file extension:

        >>> fs.tmp('NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW', 'txt')  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/NWBNVXVK5DQGIOW7MYR4K3KA5K22W7NW.txt'

        If called with ``create=True``, the parent directory is created with
        `FileStore.create_parent()`.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        :param create: if ``True``, create parent directory if it does not
            already exist; default is ``False``
        """
        filename = self.join(*self.reltmp(chash, ext))
        if create:
            self.create_parent(filename)
        return filename

    def allocate_for_transfer(self, size, chash, ext=None):
        """
        Open the canonical temporary file for a transfer (download or upload).

        When transferring files from other dmedia peers, the content-hash is
        already known.  As we must be able to easily resume a download or
        upload, transfers use a stable, canonical temporary filename derived
        from the content-hash and file extension.

        The file *size* is also known, so an attempt is made to efficiently
        pre-allocate the temporary file using `fallocate()`.

        If the temporary file already exists, it means we're resuming a
        transfer.  The file is opened in ``'r+b'`` mode, leaving data in the
        temporary file intact.  It is the responsibility of higher-level code
        to verify the file leaf by leaf in order to determine what portions of
        the file have been transfered, what portions of the file still need to
        be transferred.

        Note that as the temporary file will likely be pre-allocated, higher-
        level code cannot use the size of the temporary file as a means of
        determining how much of the file has been transfered.

        If the temporary does not exist, and cannot be pre-allocated, a new
        empty file is opened in ``'wb'`` mode.  Higher-level code must check
        the mode of the ``file`` instance and act accordingly.

        :param size: file size in bytes (an ``int``)
        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        filename = self.tmp(chash, ext, create=True)
        fallocate(size, filename)
        try:
            fp = open(filename, 'r+b')
            if os.fstat(fp.fileno()).st_size > size:
                fp.truncate(size)
            return fp
        except IOError:
            return open(filename, 'wb')

    def allocate_for_import(self, size, ext=None):
        """
        Open a random temporary file for an import operation.

        When importing a file, the content-hash is computed as the file is
        copied into the `FileStore`.  As the content-hash isn't known when
        allocating the temporary file, a randomly named temporary file is used.

        However, the file *size* is known, so an attempt is made to efficiently
        pre-allocate the temporary file using `fallocate()`.

        The file extension *ext* is optional and serves no other purpose than to
        aid in debugging.  The value of *ext* used here has no effect on the
        ultimate canonical file name.

        :param size: file size in bytes (an ``int``)
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        imports = self.join(IMPORTS_DIR)
        if not path.exists(imports):
            os.makedirs(imports)
        suffix = ('' if ext is None else '.' + ext)
        (fileno, filename) = tempfile.mkstemp(suffix=suffix, dir=imports)
        fallocate(size, filename)
        # FIXME: This probably isn't the best approach, but for now it works:
        tmp_fp = open(filename, 'r+b')
        os.close(fileno)
        return tmp_fp

    def allocate_for_write(self, ext=None):
        """
        Open a random temporary file for a write operation.

        Use this method to allocated a temporary file for cases when the file
        size is not known in advance, eg when transcoding or rendering.

        The file extension *ext* is optional and serves no other purpose than to
        aid in debugging.  The value of *ext* used here has no effect on the
        ultimate canonical file name.

        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        writes = self.join(WRITES_DIR)
        if not path.exists(writes):
            os.makedirs(writes)
        suffix = ('' if ext is None else '.' + ext)
        (fileno, filename) = tempfile.mkstemp(suffix=suffix, dir=writes)
        tmp_fp = open(filename, 'r+b')
        os.close(fileno)
        return tmp_fp

    def tmp_move(self, tmp_fp, chash, ext=None):
        """
        Move temporary file into its canonical location.

        This method will securely and atomically move a temporary file into its
        canonical location.

        For example:

        >>> fs = FileStore()
        >>> tmp_fp = open(fs.join('foo.mov'), 'wb')
        >>> chash = 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        >>> fs.tmp_move(tmp_fp, chash, 'mov')  #doctest: +ELLIPSIS
        '/tmp/store.../ZR/765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'

        Note, however, that this method does *not* verify the content hash of
        the temporary file!  This is by design as many operations will compute
        the content hash as they write to the temporary file.  Other operations
        should use `FileStore.tmp_verify_move()` to verify and move in one step.

        Regardless, the full content hash should have been verified prior to
        calling this method.  To ensure the content is not modified, operations
        must take these steps:

            1. Open *tmp_fp* and keep it open, thereby retaining a lock on the
               file

            2. Compute the full content hash, which can be done as content is
               written to *tmp_fp* (open in mode ``'r+b'`` to resume a transfer,
               but hash of previously transfered leaves must still be verified)

            3. With *tmp_fp* still open, move the temporary file into its
               canonical location using this method.

        As a simple locking mechanism, this method takes an open ``file`` rather
        than a filename, thereby preventing the file from being modified during
        the move.  A ``ValueError`` is raised if *tmp_fp* is already closed.

        For portability reasons, this method requires that *tmp_fp* be opened in
        a binary mode: ``'rb'``, ``'wb'``, or ``'r+b'``.  A ``ValueError`` is
        raised if opened in any other mode.

        For security reasons, this method will only move a temporary file
        located within the ``FileStore.base`` directory or a subdirectory
        thereof.  If an attempt is made to move a file from outside the store,
        `FileStoreTraversal` is raised.  See `FileStore.check_path()`.

        Just prior to moving the file, a call to ``os.fchmod()`` is made to set
        read-only permissions (0444).  After the move, *tmp_fp* is closed.

        If the canonical file already exists, `DuplicateFile` is raised.

        The return value is the absolute path of the canonical file.

        :param tmp_fp: a ``file`` instance created with ``open()``
        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        # Validate tmp_fp:
        if not isinstance(tmp_fp, file):
            raise TypeError(
                TYPE_ERROR % ('tmp_fp', file, type(tmp_fp), tmp_fp)
            )
        if tmp_fp.mode not in ('rb', 'wb', 'r+b'):
            raise ValueError(
                "tmp_fp: mode must be 'rb', 'wb', or 'r+b'; got %r" % tmp_fp.mode
            )
        if tmp_fp.closed:
            raise ValueError('tmp_fp is closed, must be open: %r' % tmp_fp.name)
        self.check_path(tmp_fp.name)

        # Get canonical name, check for duplicate:
        dst = self.path(chash, ext, create=True)
        if path.exists(dst):
            raise DuplicateFile(chash=chash, src=tmp_fp.name, dst=dst)

        # Set file to read-only (0444) and move into canonical location
        log.info('Moving file %r to %r', tmp_fp.name, dst)
        os.fchmod(tmp_fp.fileno(), stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        os.rename(tmp_fp.name, dst)
        tmp_fp.close()

        # Return canonical filename:
        return dst

    def tmp_hash_move(self, tmp_fp, ext=None):
        """
        Hash temporary file, then move into its canonical location.
        """
        h = HashList(tmp_fp)
        chash = h.run()
        self.tmp_move(tmp_fp, chash, ext)
        return (chash, h.leaves)

    def tmp_verify_move(self, chash, ext=None):
        """
        Verify temporary file, then move into its canonical location.

        This method will check the content hash of the canonically-named
        temporary file with content hash *chash* and extension *ext*.  If the
        content hash is correct, this method will then move the temporary file
        into its canonical location using `FileStore.tmp_move()`.

        If the content hash is incorrect, `IntegrityError` is raised.  If the
        canonical file already exists, `DuplicateFile` is raised.  Lastly, if
        the temporary does not exist, ``IOError`` is raised.

        This method will typically be used with the BitTorrent downloader or
        similar, in which case the content hash will be known prior to
        downloading.  The downloader will first determine the canonical
        temporary file name, like this:

        >>> fs = FileStore()
        >>> tmp = fs.tmp('TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK', 'mov', create=True)
        >>> tmp  #doctest: +ELLIPSIS
        '/tmp/store.../transfers/TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK.mov'

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

        >>> dst = fs.tmp_verify_move('TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK', 'mov')
        >>> dst  #doctest: +ELLIPSIS
        '/tmp/store.../TG/X33XXWU3EVHEEY5J7NBOJGKBFXLEBK.mov'

        The return value is the absolute path of the canonical file.

        :param chash: base32-encoded content-hash
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        tmp = self.tmp(chash, ext)
        tmp_fp = open(tmp, 'rb')
        h = HashList(tmp_fp)
        got = h.run()
        if got != chash:
            raise IntegrityError(got=got, expected=chash, filename=tmp_fp.name)
        return self.tmp_move(tmp_fp, chash, ext)

    def import_file(self, src_fp, ext=None):
        """
        Atomically copy open file *src_fp* into this file store.

        The method will compute the content-hash of *src_fp* as it copies it to
        a temporary file within this store.  Once the copying is complete, the
        temporary file will be moved to its canonical location using
        `FileStore.tmp_move()`.

        A `DuplicateFile` exception will be raised if the file already exists
        in this store.

        This method returns a ``(chash, leaves)`` tuple with the content hash
        (top-hash) and a list of the content hashes of the leaves.  See
        `HashList` for details.

        Note that *src_fp* must have been opened in ``'rb'`` mode.

        :param src_fp: a ``file`` instance created with ``open()``
        :param ext: normalized lowercase file extension, eg ``'mov'``
        """
        size = os.fstat(src_fp.fileno()).st_size
        tmp_fp = self.allocate_for_import(size, ext)
        h = HashList(src_fp, tmp_fp)
        log.info('Importing file %r into %r', src_fp.name, self)
        chash = h.run()
        try:
            self.tmp_move(tmp_fp, chash, ext)
        except DuplicateFile as e:
            log.warning('File %r is duplicate of %r', src_fp.name, e.dst)
            raise DuplicateFile(src=src_fp.name, dst=e.dst, tmp=e.src,
                chash=chash, leaves=h.leaves
            )
        return (chash, h.leaves)
