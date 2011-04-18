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
Unit tests for `dmedia.transfers` module.
"""

from unittest import TestCase
from os import urandom
from base64 import b32encode

from dmedia import transfers



def random_id(blocks=3):
    return b32encode(urandom(5 * blocks))


class TestFunctions(TestCase):
    def setUp(self):
        transfers._uploaders.clear()
        transfers._downloaders.clear()

    def test_download_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.download_key(file_id, store_id),
            ('download', file_id)
        )

    def test_upload_key(self):
        file_id = random_id(4)
        store_id = random_id()
        self.assertEqual(
            transfers.upload_key(file_id, store_id),
            ('upload', file_id, store_id)
        )

    def test_register_uploader(self):
        f = transfers.register_uploader

        class Junk(object):
            pass

        class Okay(transfers.TransferBackend):
            pass

        # Test with wrong subclass
        with self.assertRaises(TypeError) as cm:
            f('foo', Junk)
        self.assertEqual(
            str(cm.exception),
            'backend must be {!r} subclass; got {!r}'.format(
                transfers.TransferBackend, Junk
            )
        )

        # Test when already registered
        transfers._uploaders['foo'] = None
        with self.assertRaises(ValueError) as cm:
            f('foo', Okay)
        self.assertEqual(
            str(cm.exception),
            'uploader {!r} exists, cannot register {!r}'.format('foo', Okay)
        )

        # Test when all good
        self.assertIsNone(f('bar', Okay))
        self.assertEqual(set(transfers._uploaders), set(['foo', 'bar']))
        self.assertIs(transfers._uploaders['bar'], Okay)

    def test_register_downloader(self):
        f = transfers.register_downloader

        class Junk(object):
            pass

        class Okay(transfers.TransferBackend):
            pass

        # Test with wrong subclass
        with self.assertRaises(TypeError) as cm:
            f('foo', Junk)
        self.assertEqual(
            str(cm.exception),
            'backend must be {!r} subclass; got {!r}'.format(
                transfers.TransferBackend, Junk
            )
        )

        # Test when already registered
        transfers._downloaders['foo'] = None
        with self.assertRaises(ValueError) as cm:
            f('foo', Okay)
        self.assertEqual(
            str(cm.exception),
            'downloader {!r} exists, cannot register {!r}'.format('foo', Okay)
        )

        # Test when all good
        self.assertIsNone(f('bar', Okay))
        self.assertEqual(set(transfers._downloaders), set(['foo', 'bar']))
        self.assertIs(transfers._downloaders['bar'], Okay)



class TestTransferBackend(TestCase):
    klass = transfers.TransferBackend

    def test_download(self):
        inst = self.klass({})
        with self.assertRaises(NotImplementedError) as cm:
            inst.download('file doc', 'filestore')
        self.assertEqual(
            str(cm.exception),
            'TransferBackend.download()'
        )

    def test_upload(self):
        inst = self.klass({})
        with self.assertRaises(NotImplementedError) as cm:
            inst.upload('file doc', 'filestore')
        self.assertEqual(
            str(cm.exception),
            'TransferBackend.upload()'
        )
