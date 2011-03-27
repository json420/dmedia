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
Test JavaScript included in dmedia.
"""

from base64 import b16encode, b32encode, b64encode
from hashlib import sha1
import os
import random

from dmedia.webui.js import JSTestCase
from dmedia.webui.util import datafile

MiB = 1 ** 20


class TestUploader(JSTestCase):
    js_files = (
        datafile('sha1.js'),
        datafile('base32.js'),
        datafile('uploader.js'),
        datafile('test_uploader.js'),
    )

    def test_b32encode(self):
        values = []
        for i in xrange(20):
            src = b16encode(os.urandom(15))
            values.append(
                {
                    'src': src,
                    'b32': b32encode(src),
                }
            )
        self.run_js(values=values)

    def test_sha1(self):
        values = []
        for i in xrange(20):
            src = b32encode(os.urandom(30))
            values.append(
                {
                    'src': src,
                    'hex': sha1(src).hexdigest(),
                    'b32': b32encode(sha1(src).digest()),
                }
            )
        self.run_js(values=values)

    def test_quick_id(self):
        values = []
        for i in xrange(10):
            size = random.randint(MiB, 1024 * MiB)
            chunk = b16encode(os.urandom(512))
            quick_id = b32encode(
                sha1(str(size).encode('utf-8') + chunk).digest()
            )
            values.append(
                {
                    'size': size,
                    'chunk': chunk,
                    'quick_id': quick_id,
                }
            )
        self.run_js(values=values)

    def test_uploader(self):
        leaf = 'a' * (16 * 1024)
        chash = b32encode(sha1(leaf).digest())
        self.run_js(leaf=leaf, chash=chash)
