# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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
Download files in chunks using HTTP Range requests.
"""

from os import path
from base64 import b32encode
from urlparse import urlparse
from httplib import HTTPConnection, HTTPSConnection
import logging
import time

import libtorrent
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key

from . import __version__
from .constants import CHUNK_SIZE, TYPE_ERROR
from .errors import DownloadFailure
from .filestore import FileStore, HashList, HASH


USER_AGENT = 'dmedia %s' % __version__
log = logging.getLogger()


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


class Downloader(object):
    def __init__(self, dst_fp, url, leaves, leaf_size, file_size):
        self.dst_fp = dst_fp
        self.url = url
        self.c = urlparse(url)
        if self.c.scheme not in ('http', 'https'):
            raise ValueError('url scheme must be http or https; got %r' % url)
        self.leaves = leaves
        self.leaf_size = leaf_size
        self.file_size = file_size

    def conn(self):
        """
        Return new connection instance.
        """
        klass = (HTTPConnection if self.c.scheme == 'http' else HTTPSConnection)
        conn = klass(self.c.netloc, strict=True)
        conn.set_debuglevel(1)
        return conn

    def download_leaf(self, i):
        conn = self.conn()
        headers = {
            'User-Agent': USER_AGENT,
            'Range': range_request(i, self.leaf_size, self.file_size),
        }
        conn.request('GET', self.url, headers=headers)
        response = conn.getresponse()
        return response.read()

    def process_leaf(self, i, expected):
        for r in xrange(3):
            chunk = self.download_leaf(i)
            got = b32encode(HASH(chunk).digest())
            if got == expected:
                self.dst_fp.write(chunk)
                return chunk
            log.warning('leaf %d expected %r; got %r', i, expected, got)
        raise DownloadFailure(leaf=i, expected=expected, got=got)

    def run(self):
        for (i, chash) in enumerate(self.leaves):
            self.process_leaf(i, chash)


class TorrentDownloader(object):
    def __init__(self, torrent, fs, chash, ext=None):
        if not isinstance(fs, FileStore):
            raise TypeError(
                TYPE_ERROR % ('fs', FileStore, type(fs), fs)
            )
        self.torrent = torrent
        self.fs = fs
        self.chash = chash
        self.ext = ext

    def get_tmp(self):
        tmp = self.fs.tmp(self.chash, self.ext, create=True)
        log.debug('Writting file to %r', tmp)
        return tmp

    def finalize(self):
        dst = self.fs.tmp_verify_move(self.chash, self.ext)
        log.debug('Canonical name is %r', dst)
        return dst

    def run(self):
        log.info('Downloading torrent %r %r', self.chash, self.ext)
        tmp = self.get_tmp()
        session = libtorrent.session()
        session.listen_on(6881, 6891)

        info = libtorrent.torrent_info(
            libtorrent.bdecode(self.torrent)
        )

        torrent = session.add_torrent({
            'ti': info,
            'save_path': path.dirname(tmp),
        })

        while not torrent.is_seed():
            s = torrent.status()
            log.debug('Downloaded %d%%', s.progress * 100)
            time.sleep(2)

        session.remove_torrent(torrent)
        time.sleep(1)

        return self.finalize()


class S3Transfer(object):
    def __init__(self, bucketname, keyid, secret):
        self.bucketname = bucketname
        self.keyid = keyid
        self.secret = secret
        self._bucket = None

    def __repr__(self):
        return '%s(%r, <keyid>, <secret>)' % (
            self.__class__.__name__, self.bucketname
        )

    @staticmethod
    def key(doc):
        """
        Create S3 key for file with *chash* and extension *ext*.

        For example:

        >>> doc = {'_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', 'ext': 'mov'}
        >>> S3Transfer.key(doc)
        'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'
        >>> doc = {'_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'}
        >>> S3Transfer.key(doc)
        'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        """
        ext = doc.get('ext')
        if ext:
            return '.'.join([doc['_id'], ext])
        return doc['_id']

    @property
    def bucket(self):
        if self._bucket is None:
            conn = S3Connection(self.keyid, self.secret)
            self._bucket = conn.get_bucket(self.bucketname)
        return self._bucket

    def upload(self, doc, fs):
        pass

    def download(self, doc, fs):
        pass
