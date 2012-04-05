# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
Upload to and download from remote systems.
"""

from base64 import b64decode, b32encode
from hashlib import sha1
import time
from urllib.parse import urlparse
from http.client import HTTPConnection, HTTPSConnection
import logging

from filestore import FileStore, check_root_hash

from dmedia import __version__, workers
from dmedia import workers


USER_AGENT = 'dmedia {}'.format(__version__)
log = logging.getLogger()
_uploaders = {}
_downloaders = {}

# Note: should probably export each download on the org.freedesktop.Dmedia bus
# at the object path /downloads/FILE_ID

def download_key(file_id, store_id):
    """
    Return key to identify a single instance of a download operation.

    For example:

    >>> download_key('my_file_id', 'my_remote_store_id')
    '/downloads/my_file_id'

    Notice that the *store_id* isn't used in the single instance key.  This is
    because, for now, we only allow a file to be downloaded from one location at
    a time, even if available from multiple locations.  This might change in the
    future.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return '/downloads/' + file_id

# Note: should probably export each upload on the org.freedesktop.Dmedia bus
# at the object path /uploads/FILE_ID/REMOTE_ID

def upload_key(file_id, store_id):
    """
    Return key to identify a single instance of an upload operation.

    For example:

    >>> upload_key('my_file_id', 'my_remote_store_id')
    '/uploads/my_file_id/my_remote_store_id'

    Notice that both *file_id* and *store_id* are used in the single instance
    key.  This is because we allow a file to be uploading to multiple remote
    stores simultaneously.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return '/'.join(['/uploads', file_id, store_id])


def register_uploader(name, backend):
    if not issubclass(backend, TransferBackend):
        raise TypeError(
            'backend must be {!r} subclass; got {!r}'.format(
                TransferBackend, backend
            )
        )
    if name in _uploaders:
        raise ValueError(
            'uploader {!r} exists, cannot register {!r}'.format(name, backend)
        )
    log.info('Registering %r upload backend: %r', name, backend)
    _uploaders[name] = backend


def register_downloader(name, backend):
    if not issubclass(backend, TransferBackend):
        raise TypeError(
            'backend must be {!r} subclass; got {!r}'.format(
                TransferBackend, backend
            )
        )
    if name in _downloaders:
        raise ValueError(
            'downloader {!r} exists, cannot register {!r}'.format(name, backend)
        )
    log.info('Registering %r download backend: %r', name, backend)
    _downloaders[name] = backend


def get_uploader(doc, callback=None):
    name = doc['plugin']
    try:
        klass = _uploaders[name]
    except KeyError as e:
        log.error('no uploader backend for %r', name)
        raise e
    return klass(doc, callback)


def get_downloader(doc, callback=None):
    name = doc['plugin']
    try:
        klass = _downloaders[name]
    except KeyError as e:
        log.error('no downloader backend for %r', name)
        raise e
    return klass(doc, callback)


def bytes_range(start, stop=None):
    """
    Convert from Python slice semantics to an HTTP Range request.

    Python slice semantics are quite natural to deal with, whereas the HTTP
    Range semantics are a touch wacky, so this function will help prevent silly
    errors.

    For example, say we're requesting parts of a 10,000 byte long file.  This
    requests the first 500 bytes:

    >>> bytes_range(0, 500)
    'bytes=0-499'

    This requests the second 500 bytes:

    >>> bytes_range(500, 1000)
    'bytes=500-999'

    All three of these request the final 500 bytes:

    >>> bytes_range(9500, 10000)
    'bytes=9500-9999'
    >>> bytes_range(-500)
    'bytes=-500'
    >>> bytes_range(9500)
    'bytes=9500-'

    For details on HTTP Range header, see:

      http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    if start < 0:
        assert stop is None
        return 'bytes=%d' % start
    end = ('' if stop is None else stop - 1)
    return 'bytes=%d-%s' % (start, end)


def range_request(i, leaf_size, file_size):
    """
    Request leaf *i* in a tree with *leaf_size* from a file *file_size*.

    The function returns the value for a Range request header.  For example,
    say we have a *leaf_size* of 1024 bytes and a *file_size* of 2311 bytes:

    >>> range_request(0, 1024, 2311)
    'bytes=0-1023'
    >>> range_request(1, 1024, 2311)
    'bytes=1024-2047'
    >>> range_request(2, 1024, 2311)
    'bytes=2048-2310'

    Also see the `bytes_range()` function, which this function uses.

    :param i: The leaf to request (zero-index)
    :param leaf_size: Size of leaf in bytes (min 1024 bytes)
    :param file_size: Size of file in bytes (min 1 byte)
    """
    if i < 0:
        raise ValueError('i must be >=0; got %r' % i)
    if leaf_size < 1024:
        raise ValueError('leaf_size must be >=1024; got %r' % leaf_size)
    if file_size < 1:
        raise ValueError('file_size must be >=1; got %r' % file_size)
    start = i * leaf_size
    if start >= file_size:
        raise ValueError(
            'past end of file: i=%r, leaf_size=%r, file_size=%r' % (
                i, leaf_size, file_size
            )
        )
    stop = min(file_size, (i + 1) * leaf_size)
    return bytes_range(start, stop)


def http_conn(url, **options):
    """
    Return (connection, parsed) tuple.

    For example:

    >>> (conn, p) = http_conn('http://foo.s3.amazonaws.com/')

    The returned connection will be either an ``HTTPConnection`` or
    ``HTTPSConnection`` instance based on the *url* scheme.

    The 2nd item in the returned tuple will be *url* parsed with ``urlparse()``.
    """
    t = urlparse(url)
    if t.scheme not in ('http', 'https'):
        raise ValueError(
            'url scheme must be http or https; got {!r}'.format(url)
        )
    if not t.netloc:
        raise ValueError('bad url: {!r}'.format(url))
    klass = (HTTPConnection if t.scheme == 'http' else HTTPSConnection)
    conn = klass(t.netloc, **options)
    return (conn, t)


class TransferBackend(object):
    def __init__(self, store, callback=None):
        if not (callback is None or callable(callback)):
            raise TypeError(
                'callback must be a callable; got {!r}'.format(callback)
            )
        self.store = store
        self.store_id = store.get('_id')
        self.copies = store.get('copies', 0)
        self.use_ext = store.get('use_ext', False)
        self.use_subdir = store.get('use_subdir', False)
        self.callback = callback
        self.setup()

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.store_id)

    def setup(self):
        pass

    def key(self, chash, ext=None):
        """
        Return key or relative URL for file with *chash* and *ext*.

        By default, *chash* is returned:

        >>> b = TransferBackend({})
        >>> b.key('XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY', ext='mov')
        'XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY'

        Optionally, a store can be configured to include the file extension:

        >>> b = TransferBackend({'use_ext': True})
        >>> b.key('XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY', ext='mov')
        'XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY.mov'

        Or configured to use a subdirectory layout like `FileStore`:

        >>> b = TransferBackend({'use_subdir': True})
        >>> b.key('XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY', ext='mov')
        'XT/42VK43OILRGUOY3BKJZTIG7HP4ZBJY'

        Or configured both to have subdirectory and include extension:

        >>> b = TransferBackend({'use_subdir': True, 'use_ext': True})
        >>> b.key('XT42VK43OILRGUOY3BKJZTIG7HP4ZBJY', ext='mov')
        'XT/42VK43OILRGUOY3BKJZTIG7HP4ZBJY.mov'

        """
        key = ('/'.join([chash[:2], chash[2:]]) if self.use_subdir else chash)
        if ext and self.use_ext:
            return '.'.join([key, ext])
        return key

    def progress(self, completed):
        if self.callback is not None:
            self.callback(completed)

    def download(self, doc, leaves, filestore):
        raise NotImplementedError(
            '{}.download()'.format(self.__class__.__name__)
        )

    def upload(self, doc, leaves, filestore):
        raise NotImplementedError(
            '{}.upload()'.format(self.__class__.__name__)
        )


class HTTPBaseBackend(TransferBackend):
    """
    Backend for downloading using HTTP.
    """

    def setup(self):
        self.url = self.store['url']
        (self.conn, t) = http_conn(self.url)
        self.conn.set_debuglevel(1)
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.t = t

    def get(self, urlpath, extra=None):
        headers = {'User-Agent': USER_AGENT}
        if extra:
            headers.update(extra)
        self.conn.request('GET', urlpath, headers=headers)
        response = self.conn.getresponse()
        return response.read()


class HTTPBackend(TransferBackend):
    """
    Backend for downloading using HTTP.
    """

    def setup(self):
        self.url = self.store['url']
        (self.conn, t) = http_conn(self.url)
        #self.conn.set_debuglevel(1)
        self.basepath = (t.path if t.path.endswith('/') else t.path + '/')
        self.t = t

    def download_leaf(self, i):
        headers = {
            'User-Agent': USER_AGENT,
            'Range': range_request(i, LEAF_SIZE, self.file_size),
        }
        self.conn.request('GET', self.path, headers=headers)
        response = self.conn.getresponse()
        return response.read()

    def process_leaf(self, i, expected):
        self.progress(i * LEAF_SIZE)
        for r in xrange(3):
            chunk = self.download_leaf(i)
            got = sha1(chunk).digest()
            if got == expected:
                return chunk
            log.warning('leaf %d expected %r; got %r', i, expected, got)
        raise DownloadFailure(leaf=i, expected=expected, got=got)

    def download(self, doc, leaves, fs):
        chash = doc['_id']
        ext = doc.get('ext')
        self.path = self.basepath + self.key(chash, ext)
        url = ''.join([self.t.scheme, '://', self.t.netloc, self.path])
        log.info('Downloading %r...', url)
        self.file_size = doc['bytes']
        tmp_fp = fs.allocate_for_transfer(self.file_size, chash, ext)
        for (i, leaf) in enumerate(leaves):
            chunk = self.process_leaf(i, leaf)
            tmp_fp.write(chunk)
        tmp_fp.close()
        log.info('Successfully downloaded %r', url)


register_downloader('http', HTTPBackend)


class TransferWorker(workers.CouchWorker):
    def __init__(self, env, q, key, args):
        super().__init__(env, q, key, args)
        self.filestore = FileStore(self.env['filestore']['parentdir'])
        self.filestore_id = self.env['filestore']['_id']

    def on_progress(self, completed):
        self.emit('progress', completed, self.ch.file_size)

    def init_file(self, file_id):
        doc = self.db.get(file_id)
        leaf_hashes = self.db.get_att(file_id, 'leaf_hashes')[1]
        ch = check_root_hash(file_id, doc['bytes'], leaf_hashes, unpack=True)
        self.file_id = file_id
        self.file = doc
        self.ch = ch

    def init_remote(self, remote_id):
        self.remote_id = remote_id
        self.remote = self.db.get(remote_id)

    def execute(self, file_id, remote_id):
        self.init_file(file_id)
        self.init_remote(remote_id)
        self.emit('started')
        self.emit('progress', 0, self.ch.file_size)
        self.transfer()
        self.emit('progress', self.ch.file_size, self.ch.file_size)
        self.emit('finished')

    def transfer(self):
        self.transfer_called = True


class DownloadWorker(TransferWorker):
    def transfer(self):
        self.backend = get_downloader(self.remote, self.on_progress)
        self.backend.download(self.file, self.ch.leaf_hashes, self.filestore)
        self.filestore.tmp_verify_move(self.file_id, self.file.get('ext'))
        self.file['stored'][self.filestore_id] = {
            'copies': 1,
            'time': time.time(),
        }
        self.db.save(self.file)



class UploadWorker(TransferWorker):
    def transfer(self):
        self.backend = get_uploader(self.remote, self.on_progress)
        d = self.backend.upload(self.file, self.ch.leaf_hashes, self.filestore)
        if d:
            d['time'] = time.time()
            if 'copies' not in d:
                d['copies'] = 0
            self.file['stored'][self.remote_id] = d
            self.db.save(self.file)


class TransferManager(workers.Manager):
    def __init__(self, env, callback=None):
        super().__init__(env, callback)
        for klass in (DownloadWorker, UploadWorker):
            if not workers.isregistered(klass):
                workers.register(klass)

    def download(self, file_id, store_id):
        key = download_key(file_id, store_id)
        return self.start_job('DownloadWorker', key, file_id, store_id)

    def upload(self, file_id, store_id):
        key = upload_key(file_id, store_id)
        return self.start_job('UploadWorker', key, file_id, store_id)
