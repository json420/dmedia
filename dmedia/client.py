# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
dmedia HTTP client.
"""

from collections import OrderedDict, namedtuple
import os
from os import path
import time
import logging

from microfiber import CouchBase, build_ssl_context, NotFound
from filestore import LEAF_SIZE, TYPE_ERROR, hash_leaf, reader_iter
from filestore import Leaf, ContentHash, SmartQueue, _start_thread

from .util import get_db
from .units import bytes10
from .metastore import MetaStore, get_dict


log = logging.getLogger()
Slice = namedtuple('Slice', 'start stop')


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
        return 'bytes={}'.format(start)
    end = ('' if stop is None else stop - 1)
    return 'bytes={}-{}'.format(start, end)


def check_slice(ch, start, stop):
    """
    Validate the crap out of a leaf-wise slice of a file.
    """
    # Check `ch` type
    if not isinstance(ch, ContentHash):
        raise TypeError(
            TYPE_ERROR.format('ch', ContentHash, type(ch), ch)
        )
    if not isinstance(ch.leaf_hashes, tuple):
        raise TypeError(
            'ch.leaf_hashes not unpacked for ch.id={}'.format(ch.id)
        )
    if not ch.leaf_hashes:
        raise ValueError('got empty ch.leaf_hashes for ch.id={}'.format(ch.id))

    # Check `start` type:
    if not isinstance(start, int):
        raise TypeError(
            TYPE_ERROR.format('start', int, type(start), start)
        )

    # Check `stop` type:
    count = len(ch.leaf_hashes)
    stop = (count if stop is None else stop)
    if not isinstance(stop, int):
        raise TypeError(
            TYPE_ERROR.format('stop', int, type(stop), stop)
        )

    # Check slice math:
    if not (0 <= start < stop <= count):
        raise ValueError(
            '[{}:{}] invalid slice for {} leaves'.format(start, stop, count)
        )

    # We at most change `stop`, but return them all for clarity:
    return (ch, start, stop)


def range_header(ch, start, stop):
    """
    When needed, convert a leaf-wise slice into a byte-wise HTTP Range header.

    If the slice represents the entire file, None is returned.

    Note: we assume (ch, start, stop) were already validated with
    `check_slice()`.  See `HTTPClient.get_leaves()`.
    """
    count = len(ch.leaf_hashes)
    assert 0 <= start < stop <= count
    if start == 0 and stop == count:
        return None
    start_bytes = start * LEAF_SIZE
    stop_bytes = min(ch.file_size, stop * LEAF_SIZE)
    assert 0 <= start_bytes < stop_bytes <= ch.file_size
    end_bytes = stop_bytes - 1
    return {'Range': 'bytes={}-{}'.format(start_bytes, end_bytes)}


def response_reader(response, queue, start=0):
    try:
        index = start
        while True:
            data = response.read(LEAF_SIZE)
            if not data:
                queue.put(None)
                break
            queue.put(Leaf(index, data))
            index += 1
    except Exception as e:
        queue.put(e)


def response_iter(response, start=0):
    q = SmartQueue(4)
    thread = _start_thread(response_reader, response, q, start)
    while True:
        leaf = q.get()
        if leaf is None:
            break
        yield leaf
    thread.join()  # Make sure reader() terminates


def missing_leaves(ch, tmp_fp):
    assert isinstance(ch.leaf_hashes, tuple)
    assert os.fstat(tmp_fp.fileno()).st_size == ch.file_size
    assert tmp_fp.mode == 'rb+'
    tmp_fp.seek(0)
    for leaf in reader_iter(tmp_fp):
        leaf_hash = ch.leaf_hashes[leaf.index]
        if hash_leaf(leaf.index, leaf.data) != leaf_hash:
            yield (leaf.index, leaf_hash)
    assert leaf.index == len(ch.leaf_hashes) - 1


class DownloadComplete(Exception):
    pass


class Downloader:
    def __init__(self, doc_or_id, ms, fs):
        (self.doc, self.id) = ms.doc_and_id(doc_or_id)
        self.ch = ms.content_hash(self.doc)
        self.tmp_fp = ms.start_download(fs, self.doc)
        self.ms = ms
        self.fs = fs
        if self.tmp_fp.mode != 'xb':
            log.info('Resuming download of %s in %r', self.ch.id, fs)
            self.missing = OrderedDict(missing_leaves(self.ch, self.tmp_fp))
            log.info('Missing %d of %d leaves in partial file %s',
                len(self.missing), len(self.ch.leaf_hashes), self.ch.id
            )
        else:
            self.missing = OrderedDict(enumerate(self.ch.leaf_hashes))

    def write_leaf(self, leaf):
        if hash_leaf(leaf.index, leaf.data) != self.ch.leaf_hashes[leaf.index]:
            log.warning('Got corrupt leaf %s[%d]', self.ch.id, leaf.index)
            return False
        self.tmp_fp.seek(leaf.index * LEAF_SIZE)
        self.tmp_fp.write(leaf.data)
        leaf_hash = self.missing.pop(leaf.index)
        assert leaf_hash == self.ch.leaf_hashes[leaf.index]
        return True

    def next_slice(self):
        if len(self.missing) == 0:
            return None
        first = None
        for i in self.missing:
            if first is None:
                first = i
                last = i
            elif i != last + 1:
                return Slice(first, last + 1)
            else:
                last = i
        return Slice(first, last + 1)

    def download_is_complete(self):
        if self.missing:
            return False
        self.doc = self.ms.finish_download(self.fs, self.doc, self.tmp_fp)
        return True

    def download_from(self, client):
        start = time.monotonic()
        total = 0

        next = self.next_slice()
        while next is not None:
            for leaf in client.iter_leaves(self.ch, next.start, next.stop):
                self.write_leaf(leaf)
                total += len(leaf.data)
            next = self.next_slice()

        delta = time.monotonic() - start
        rate = int(total / delta)
        log.info('**** %s per second from %s', bytes10(rate), client.url)


class HTTPClient(CouchBase):
    def get_leaves(self, ch, start=0, stop=None):
        (ch, start, stop) = check_slice(ch, start, stop)
        log.info('Getting leaves %s[%d:%d] from %s', ch.id, start, stop, self.url)
        return self.request('GET', ('files', ch.id), None,
            headers=range_header(ch, start, stop),
        )

    def iter_leaves(self, ch, start=0, stop=None):
        response = self.get_leaves(ch, start, stop)
        return response_iter(response, start)


def download_one(ms, ssl_context, _id, tmpfs=None):
    try:
        doc = ms.db.get(_id)
    except NotFound:
        log.error('doc for %s NotFound in CouchDB', _id)
        return

    # We can't do anything if there are no local stores:
    local_stores = ms.get_local_stores()
    if tmpfs is not None:
        local_stores.add(tmpfs)
    if len(local_stores) == 0:
        log.warning('No connected FileStore, nothing to download to...')
        return

    # Pointless to download when the file is already local:
    stored = local_stores.intersection(get_dict(doc, 'stored'))
    if stored and tmpfs is None:
        log.error('%s is already local in %r', _id, stored)
        return

    # If a partial download already exists, use that FileStore, otherwise use
    # the FileStore with the most available free space:
    partial = local_stores.intersection(get_dict(doc, 'partial'))
    if partial:
        fs = local_stores.by_id(partial.pop())
    else:
        fs = local_stores.sort_by_avail()[0]
    if tmpfs is not None:
        fs = tmpfs
    log.info('Saving into %r', fs)

    # Could happen occasionally:
    downloader = Downloader(doc, ms, fs)
    if downloader.download_is_complete():
        log.info('Hey, the partial file for %s was already complete', _id)
        return

    # We can't do anything if no peers are available:
    peers = ms.get_peers()
    if not peers:
        log.warning('No peers on local network, cannot download %s', _id)
        return

    # Try downloading from each local peer till we succeed (or give up):
    for (machine_id, info) in peers.items():
        url = info['url']
        client_env = {
            'url': url,
            'ssl': {
                'context': ssl_context,
                'check_hostname': False,
            },
        }
        client = HTTPClient(client_env)
        try:
            downloader.download_from(client)
        except Exception:
            log.exception('Error downloading %s from %s', _id, url)
        if downloader.download_is_complete():
            break


def download_worker(queue, env, ssl_config, tmpfs=None):
    ms = MetaStore(get_db(env))
    ssl_context = build_ssl_context(ssl_config)
    while True:
        _id = queue.get()
        if _id is None:
            break
        try:
            download_one(ms, ssl_context, _id, tmpfs)
        except Exception:
            log.exception('An error occurred when downloading %s', _id)
        if tmpfs is not None:
            for filename in [tmpfs.path(_id), tmpfs.partial_path(_id)]:
                if path.isfile(filename):
                    log.info('Removing %s', filename)
                    os.remove(filename)

            

