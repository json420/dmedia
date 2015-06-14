# dmedia: dmedia hashing protocol and file layout
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
Unit tests for `dmedia.client`.
"""

from unittest import TestCase
import os
from collections import OrderedDict

from filestore import ContentHash, TYPE_ERROR, DIGEST_BYTES

from dmedia import client


class TestFunctions(TestCase):
    def test_check_slice(self):
        ch = ContentHash('foo', None, (1, 2, 3))

        # Test all valid slices
        self.assertEqual(client.check_slice(ch, 0, None), (ch, 0, 3))
        self.assertEqual(client.check_slice(ch, 1, None), (ch, 1, 3))
        self.assertEqual(client.check_slice(ch, 2, None), (ch, 2, 3))
        self.assertEqual(client.check_slice(ch, 0, 1), (ch, 0, 1))
        self.assertEqual(client.check_slice(ch, 0, 2), (ch, 0, 2))
        self.assertEqual(client.check_slice(ch, 1, 2), (ch, 1, 2))
        self.assertEqual(client.check_slice(ch, 0, 3), (ch, 0, 3))
        self.assertEqual(client.check_slice(ch, 1, 3), (ch, 1, 3))
        self.assertEqual(client.check_slice(ch, 2, 3), (ch, 2, 3))

        # ch type
        with self.assertRaises(TypeError) as cm:
            bad = ('foo', None, (1, 2, 3))
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('ch', ContentHash, tuple, bad)
        )

        # ch.leaf_hashes type
        with self.assertRaises(TypeError) as cm:
            bad = ContentHash('foo', None, os.urandom(DIGEST_BYTES))
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            'ch.leaf_hashes not unpacked for ch.id=foo'
        )

        # empty ch.leaf_hashes
        with self.assertRaises(ValueError) as cm:
            bad = ContentHash('foo', None, tuple())
            client.check_slice(bad, 1, None)
        self.assertEqual(
            str(cm.exception),
            'got empty ch.leaf_hashes for ch.id=foo'
        )

        # start type
        with self.assertRaises(TypeError) as cm:
            client.check_slice(ch, 0.0, None)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('start', int, float, 0.0)
        )

        # stop type
        with self.assertRaises(TypeError) as cm:
            client.check_slice(ch, 0, 1.0)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format('stop', int, float, 1.0)
        )

        # start < 0:
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, -1, None)
        self.assertEqual(str(cm.exception), '[-1:3] invalid slice for 3 leaves')
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, -1, 3)
        self.assertEqual(str(cm.exception), '[-1:3] invalid slice for 3 leaves')

        # stop > len(ch.leaf_hashes):
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 1, 4)
        self.assertEqual(str(cm.exception), '[1:4] invalid slice for 3 leaves')

        # start >= stop:
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 1, 1)
        self.assertEqual(str(cm.exception), '[1:1] invalid slice for 3 leaves')
        with self.assertRaises(ValueError) as cm:
            client.check_slice(ch, 2, 1)
        self.assertEqual(str(cm.exception), '[2:1] invalid slice for 3 leaves')


class TestDownloader(TestCase):
    def test_next_slice(self):
        class Dummy(client.Downloader):
            def __init__(self, indexes):
                self.missing = OrderedDict((i, None) for i in indexes)

        # missing is empty
        dl = Dummy([])
        self.assertIsNone(dl.next_slice())

        # all leaves are missing
        dl = Dummy(range(413))
        s = dl.next_slice()
        self.assertIsInstance(s, client.Slice)
        self.assertEqual(s, (0, 413))

        # should chose first contiguous slice:
        dl = Dummy([17, 18, 19, 20, 21, 106, 107, 108, 700, 999])
        s = dl.next_slice()
        self.assertIsInstance(s, client.Slice)
        self.assertEqual(s, (17, 22))

        # even if that slice is only 1 in length:
        dl = Dummy([19, 119, 120, 121, 123])
        s = dl.next_slice()
        self.assertIsInstance(s, client.Slice)
        self.assertEqual(s, (19, 20))

        # even there is only a single missing leaf:
        dl = Dummy([1775])
        s = dl.next_slice()
        self.assertIsInstance(s, client.Slice)
        self.assertEqual(s, (1775, 1776))


class TestHTTPClient(TestCase):        
    def test_get_leaves(self):
        pass

