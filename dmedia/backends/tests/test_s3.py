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
Unit tests for the `dmedia.backends.s3` module.
"""

from unittest import TestCase

from dmedia.backends import s3


class TestS3Backend(TestCase):
    klass = s3.S3Backend

    def test_init(self):
        inst = self.klass({'_id': 'foo', 'bucket': 'bar'})
        self.assertEqual(inst.bucketname, 'bar')
        self.assertEqual(inst._bucket, None)

    def test_repr(self):
        inst = self.klass({'_id': 'foo', 'bucket': 'bar'})
        self.assertEqual(repr(inst), "S3Backend('foo')")

    def test_key(self):
        # Test with include_ext = False
        inst = self.klass({'_id': 'foo', 'bucket': 'bar'})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext=None),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext='mov'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )

        # Test with include_ext = True
        inst = self.klass({'_id': 'foo', 'bucket': 'bar', 'include_ext': True})
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext=None),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'
        )
        self.assertEqual(
            inst.key('ZR765XWSF6S7JQHLUI4GCG5BHGPE252O', ext='mov'),
            'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O.mov'
        )

    def test_bucket(self):
        inst = self.klass({'_id': 'foo', 'bucket': 'bar'})
        inst._bucket = 'whatever'
        self.assertEqual(inst.bucket, 'whatever')
