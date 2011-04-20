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

import logging
from base64 import b64decode, b32encode

from . import workers
from .workers import CouchWorker, Manager
from .filestore import FileStore, tophash, unpack_leaves
from .errors import TopHashError



log = logging.getLogger()
_uploaders = {}
_downloaders = {}

# Note: should probably export each download on the org.freedesktop.DMedia bus
# at the object path /downloads/FILE_ID

def download_key(file_id, store_id):
    """
    Return key to identify a single instance of a download operation.

    For example:

    >>> download_key('my_file_id', 'my_remote_store_id')
    ('downloads', 'my_file_id')

    Notice that the *store_id* isn't used in the single instance key.  This is
    because, for now, we only allow a file to be downloaded from one location at
    a time, even if available from multiple locations.  This might change in the
    future.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return ('downloads', file_id)

# Note: should probably export each upload on the org.freedesktop.DMedia bus
# at the object path /uploads/FILE_ID/REMOTE_ID

def upload_key(file_id, store_id):
    """
    Return key to identify a single instance of an upload operation.

    For example:

    >>> upload_key('my_file_id', 'my_remote_store_id')
    ('uploads', 'my_file_id', 'my_remote_store_id')

    Notice that both *file_id* and *store_id* are used in the single instance
    key.  This is because we allow a file to be uploading to multiple remote
    stores simultaneously.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return ('uploads', file_id, store_id)


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


class TransferBackend(object):
    def __init__(self, store, callback=None):
        if not (callback is None or callable(callback)):
            raise TypeError(
                'callback must be a callable; got {!r}'.format(callback)
            )
        self.store = store
        self.callback = callback
        self.setup()

    def setup(self):
        pass

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


class HTTPBackend(object):
    pass


class TransferWorker(CouchWorker):
    def __init__(self, env, q, key, args):
        super(TransferWorker, self).__init__(env, q, key, args)
        self.filestore = FileStore(self.env['filestore']['path'])
        self.filestore_id = self.env['filestore']['_id']

    def on_progress(self, completed):
        self.emit('progress', completed, self.file_size)

    def init_file(self, file_id):
        self.file_id = file_id
        self.file = self.db.get(file_id, attachments=True)
        self.file_size = self.file['bytes']
        packed = b64decode(self.file['_attachments']['leaves']['data'])
        h = tophash(self.file_size)
        h.update(packed)
        got = b32encode(h.digest())
        if got != self.file_id:
            raise TopHashError(
                got=got, expected=self.file_id, size=self.file_size
            )
        self.leaves = unpack_leaves(packed)

    def init_remote(self, remote_id):
        self.remote_id = remote_id
        self.remote = self.db[remote_id]

    def execute(self, file_id, remote_id):
        self.init_file(file_id)
        self.init_remote(remote_id)
        self.emit('started')
        self.emit('progress', 0, self.file_size)
        self.transfer()
        self.emit('progress', self.file_size, self.file_size)
        self.emit('finished')

    def transfer(self):
        self.transfer_called = True


class DownloadWorker(TransferWorker):
    def transfer(self):
        self.backend = get_downloader(self.remote, self.on_progress)
        self.backend.download(self.file, self.leaves, self.filestore)


class UploadWorker(TransferWorker):
    def transfer(self):
        self.backend = get_uploader(self.remote, self.on_progress)
        self.backend.upload(self.file, self.leaves, self.filestore)


class TransferManager(Manager):
    def __init__(self, env, callback=None):
        super(TransferManager, self).__init__(env, callback)
        for klass in (DownloadWorker, UploadWorker):
            if not workers.isregistered(klass):
                workers.register(klass)

    def download(self, file_id, store_id):
        key = download_key(file_id, store_id)
        return self.start_job('DownloadWorker', key, file_id, store_id)

    def upload(self, file_id, store_id):
        key = upload_key(file_id, store_id)
        return self.start_job('UploadWorker', key, file_id, store_id)

    def on_started(self, key):
        print('started: {!r}'.format(key))

    def on_finished(self, key):
        print('finished: {!r}'.format(key))

    def on_progress(self, key, completed, total):
        print('progress: {!r}, {!r}, {!r}'.format(key, completed, total))
