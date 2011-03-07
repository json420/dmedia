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

from base64 import b32encode, b64encode
from hashlib import sha1
import os

from dmedia.js import JSTestCase
from dmedia.ui import datafile, load_datafile


class TestUploader(JSTestCase):
    js_files = (
        datafile('mootools-core.js'),
        datafile('uploader.js'),
        datafile('test_uploader.js'),
    )

    def test_b32encode(self):
        values = []
        for i in xrange(20):
            src = os.urandom(15).encode('hex')
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
                    'b64': b64encode(sha1(src).digest()),
                }
            )
        self.run_js(values=values)

    def test_uploader(self):
        self.run_js()
