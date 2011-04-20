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
Upload to and download from Amazon S3 using ``boto``.

For documentation on ``boto``, see:

    http://code.google.com/p/boto/
"""

import os
import logging

from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key

from dmedia.constants import LEAF_SIZE
from dmedia import transfers


log = logging.getLogger()


class S3Backend(transfers.TransferBackend):
    """
    Backend for uploading to and downloading from Amazon S3 using ``boto``.
    """
    def setup(self):
        self.bucketname = self.store['bucket']
        self.include_ext = self.store.get('include_ext', False)
        self.keyid = os.environ['s3_keyid']  # FIXME
        self.secret = os.environ['s3_secret']  # FIXME
        self._bucket = None

    def key(self, chash, ext=None):
        if ext and self.include_ext:
            return '.'.join([chash, ext])
        return chash

    @property
    def bucket(self):
        """
        Lazily create the ``boto.s3.bucket.Bucket`` instance.
        """
        if self._bucket is None:
            conn = S3Connection(self.keyid, self.secret)
            self._bucket = conn.get_bucket(self.bucketname)
        return self._bucket

    def boto_callback(self, completed, total):
        self.progress(completed)

    def upload(self, doc, leaves, fs):
        """
        Upload the file with *doc* metadata from the filestore *fs*.

        :param doc: the CouchDB document of file to upload (a ``dict``)
        :param fs: a `FileStore` instance from which the file will be read
        """
        chash = doc['_id']
        ext = doc.get('ext')
        key = self.key(chash, ext)
        log.info('Uploading %r to S3 bucket %r...', key, self.bucketname)

        k = Key(self.bucket)
        k.key = key
        headers = {}
        if doc.get('mime'):
            headers['Content-Type'] = doc['mime']
        fp = fs.open(chash, ext)
        k.set_contents_from_file(fp,
            headers=headers,
            cb=self.boto_callback,
            num_cb=max(5, doc['bytes'] / LEAF_SIZE),
            policy='public-read',
        )
        log.info('Uploaded %r to S3 bucket %r', key, self.bucketname)

    def download(self, doc, leaves, fs):
        """
        Download the file with *doc* metadata into the filestore *fs*.

        :param doc: the CouchDB document of file to download (a ``dict``)
        :param fs: a `FileStore` instance into which the file will be written
        """
        chash = doc['_id']
        ext = doc.get('ext')
        key = self.key(chash, ext)
        log.info('Downloading %r from S3 bucket %r...', key, self.bucketname)

        k = self.bucket.get_key(self.key(chash, ext))
        tmp_fp = fs.allocate_for_transfer(doc['size'], chash, ext)
        k.get_file(tmp_fp,
            cb=S3Progress(key, self.bucketname, 'Downloaded'),
        )
        tmp_fp.close()
        fs.tmp_verify_move(chash, ext)
        log.info('Downloaded %r from S3 bucket %r', key, self.bucketname)


transfers.register_uploader('s3', S3Backend)
transfers.register_downloader('s3', S3Backend)
