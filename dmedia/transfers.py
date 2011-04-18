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


log = logging.getLogger()
_uploaders = {}
_downloaders = {}


def download_key(file_id, store_id):
    """
    Return key to identify a single instance of a download operation.

    For example:

    >>> download_key('my_file_id', 'my_remote_store_id')
    ('download', 'my_file_id')

    Notice that the *store_id* isn't used in the single instance key.  This is
    because, for now, we only allow a file to be downloaded from one location at
    a time, even if available from multiple locations.  This might change in the
    future.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return ('download', file_id)


def upload_key(file_id, store_id):
    """
    Return key to identify a single instance of an upload operation.

    For example:

    >>> upload_key('my_file_id', 'my_remote_store_id')
    ('upload', 'my_file_id', 'my_remote_store_id')

    Notice that both *file_id* and *store_id* are used in the single instance
    key.  This is because we allow a file to be uploading to multiple remote
    stores simultaneously.

    Note that this value isn't used in the dmedia schema or protocol, is only
    an internal implementation detail.
    """
    return ('upload', file_id, store_id)


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
    _downloaders[name] = backend


def get_uploader(doc, callback=None):
    name = doc['plugin']
    klass = _uploaders[name]
    return klass(doc, callback)


def get_downloader(doc, callback=None):
    name = doc['plugin']
    klass = _downloaders[name]
    return klass(doc, callback)


class TransferBackend(object):
    def __init__(self, doc, callback=None):
        if not (callback is None or callable(callback)):
            raise TypeError(
                'callback must be a callable; got {!r}'.format(callback)
            )
        self.doc = doc
        self.callback = callback

    def progress(self, completed):
        if self.callback is not None:
            self.callback(completed)

    def download(self, doc, fs):
        raise NotImplementedError(
            '{}.download()'.format(self.__class__.__name__)
        )

    def upload(self, doc, fs):
        raise NotImplementedError(
            '{}.upload()'.format(self.__class__.__name__)
        )
