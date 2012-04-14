# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2012 Novacut Inc
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
Unit tests for `dmedia.service.udisks`.
"""

from unittest import TestCase
import os
from os import path
import json

from microfiber import random_id

from dmedia.tests.base import TempDir
from dmedia.service import udisks


class DummyDevice:
    def __init__(self, d):
        self.__d = d
        self.drive = random_id()
    
    def __getitem__(self, key):
        return self.__d[key]


class TestFunctions(TestCase):
    def test_major_minor(self):
        tmp = TempDir()
        st_dev = os.stat(tmp.dir).st_dev
        major = os.major(st_dev)
        minor = os.minor(st_dev)

        # Test on the tmp dir:
        self.assertEqual(udisks.major_minor(tmp.dir), (major, minor))

        # Test on a subdir
        subdir = tmp.makedirs('subdir')
        self.assertEqual(udisks.major_minor(subdir), (major, minor))

        # Test on a file
        some_file = tmp.touch('some_file')
        self.assertEqual(udisks.major_minor(some_file), (major, minor))

        # Test on a non-existant path
        nope = tmp.join('nope')
        with self.assertRaises(OSError) as cm:
            udisks.major_minor(nope)
        self.assertEqual(cm.exception.errno, 2)

    def test_usable_mount(self):
        self.assertEqual(udisks.usable_mount(['/media/foo']), '/media/foo')
        self.assertEqual(udisks.usable_mount(['/srv/bar']), '/srv/bar')

        self.assertIsNone(udisks.usable_mount(['/media']))
        self.assertIsNone(udisks.usable_mount(['/srv']))
        self.assertIsNone(udisks.usable_mount(['/srv', '/media']))

        self.assertEqual(
            udisks.usable_mount(['/media/foo', '/srv/bar']),
            '/media/foo'
        )
        self.assertEqual(
            udisks.usable_mount(['/srv/bar', '/media/foo']),
            '/srv/bar'
        )
        self.assertEqual(
            udisks.usable_mount(['/', '/home', '/home/user', '/tmp', '/media/foo']),
            '/media/foo'
        )

    def test_partition_info(self):
        d = {
            'DeviceSize': 16130244608, 
            'IdType': 'vfat', 
            'IdVersion': 'FAT32', 
            'IdLabel': 'H4N_SD', 
            'PartitionNumber': 1, 
            'IdUuid': '89E3-CE4D',
        }
        device = DummyDevice(d)
        self.assertEqual(udisks.partition_info(device),
            {
                'mount': None,
                'drive': device.drive,
                'info': {
                    'bytes': 16130244608, 
                    'filesystem': 'vfat', 
                    'filesystem_version': 'FAT32', 
                    'label': 'H4N_SD', 
                    'number': 1, 
                    'size': '16.1 GB', 
                    'uuid': '89E3-CE4D'
                },
            }
        ) 
        self.assertEqual(udisks.partition_info(device, '/media/foo'),
            {
                'mount': '/media/foo',
                'drive': device.drive,
                'info': {
                    'bytes': 16130244608, 
                    'filesystem': 'vfat', 
                    'filesystem_version': 'FAT32', 
                    'label': 'H4N_SD', 
                    'number': 1, 
                    'size': '16.1 GB', 
                    'uuid': '89E3-CE4D'
                },
            }
        )

    def test_drive_info(self):
        d = {
            'DeviceSize': 16134438912, 
            'DriveConnectionInterface': 'usb', 
            'DriveModel': 'CFUDMASD', 
            'DeviceIsSystemInternal': False, 
            'DriveSerial': 'AA0000000009019', 
        }
        self.assertEqual(udisks.drive_info(d),
            {
                'partitions': [],
                'info': {
                    'bytes': 16134438912, 
                    'connection': 'usb', 
                    'model': 'CFUDMASD', 
                    'removable': True, 
                    'serial': 'AA0000000009019', 
                    'size': '16.1 GB', 
                    'text': '16.1 GB Removable Drive',
                },
            }
        )

        d['DeviceIsSystemInternal'] = True 
        self.assertEqual(udisks.drive_info(d),
            {
                'partitions': [],
                'info': {
                    'bytes': 16134438912, 
                    'connection': 'usb', 
                    'model': 'CFUDMASD', 
                    'removable': False, 
                    'serial': 'AA0000000009019', 
                    'size': '16.1 GB', 
                    'text': '16.1 GB Drive',
                },
            }
        )

    def test_get_filestore_id(self):
        tmp = TempDir()

        # Test when file and directory are missing
        self.assertIsNone(udisks.get_filestore_id(tmp.dir))         

        # Test when control dir exists, file missing
        basedir = tmp.makedirs('.dmedia')
        self.assertTrue(path.isdir(basedir))
        self.assertIsNone(udisks.get_filestore_id(tmp.dir))

        # Test when file is empty
        store = tmp.touch('.dmedia', 'store.json')
        self.assertTrue(path.isfile(store))
        self.assertIsNone(udisks.get_filestore_id(tmp.dir))

        # Test when correct
        _id = random_id()
        json.dump({'_id': _id}, open(store, 'w'))
        self.assertEqual(udisks.get_filestore_id(tmp.dir), _id)

        # Test when '_id' is missed from dict:
        json.dump({'id': _id}, open(store, 'w'))
        self.assertIsNone(udisks.get_filestore_id(tmp.dir)) 

