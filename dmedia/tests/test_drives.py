# dmedia: distributed media library
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
Unit tests for `dmedia.drives`.
"""

from unittest import TestCase
import uuid
import os

from dbase32 import db32enc

from dmedia import drives


PARTED_PRINT = """
Model: ATA WDC WD30EZRX-00D (scsi)
Disk /dev/sdd: 2861588MiB
Sector size (logical/physical): 512B/4096B
Partition Table: gpt

Number  Start    End         Size        File system  Name     Flags
 1      1.00MiB  2861587MiB  2861586MiB  ext4         primary
"""


class TestFunctions(TestCase):
    def test_db32_to_uuid(self):
        for i in range(100):
            data = os.urandom(15)
            data_D = data + b'D'
            self.assertEqual(
                drives.db32_to_uuid(db32enc(data)),
                str(uuid.UUID(bytes=data_D))
            )

    def test_uuid_to_db32(self):
        for i in range(100):
            data = os.urandom(16)
            self.assertEqual(
                drives.uuid_to_db32(str(uuid.UUID(bytes=data))),
                db32enc(data[:15])
            )
            
    def test_unfuck(self):
        self.assertIsNone(drives.unfuck(None))
        fucked = 'WDC\\x20WD30EZRX-00DC0B0\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20\\x20'
        self.assertEqual(drives.unfuck(fucked), 'WDC WD30EZRX-00DC0B0')

    def test_parse_drive_size(self):
        self.assertEqual(drives.parse_drive_size(PARTED_PRINT), 2861588)


class TestDrive(TestCase):
    def test_init(self):
        for dev in ('/dev/sda', '/dev/sdz', '/dev/vda', '/dev/vdz'):
            inst = drives.Drive(dev)
            self.assertIs(inst.dev, dev)
        with self.assertRaises(ValueError) as cm:
            drives.Drive('/dev/sda1')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/sda1'"
        )
        with self.assertRaises(ValueError) as cm:
            drives.Drive('/dev/sdA')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/sdA'"
        )
        with self.assertRaises(ValueError) as cm:
            drives.Drive('/dev/sdaa')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/sdaa'"
        )


class TestPartition(TestCase):
    def test_init(self):
        for dev in ('/dev/sda1', '/dev/sda9', '/dev/sdz1', '/dev/sdz9'):
            part = drives.Partition(dev)
            self.assertIs(part.dev, dev)
        for dev in ('/dev/vda1', '/dev/vda9', '/dev/vdz1', '/dev/vdz9'):
            part = drives.Partition(dev)
            self.assertIs(part.dev, dev)
        with self.assertRaises(ValueError) as cm:
            drives.Partition('/dev/sda11')
        self.assertEqual(str(cm.exception),
            "Invalid partition device file: '/dev/sda11'"
        )

