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
import string
import re
import subprocess
from random import SystemRandom

from filestore import FileStore
from dbase32 import db32enc, random_id

import gi
gi.require_version('GUdev', '1.0')
from gi.repository import GUdev

from .base import TempDir
from dmedia import drives


random = SystemRandom()

PARTED_PRINT = """
Model: ATA WDC WD30EZRX-00D (scsi)
Disk /dev/sdd: 2861588MiB
Sector size (logical/physical): 512B/4096B
Partition Table: gpt

Number  Start    End         Size        File system  Name     Flags
 1      1.00MiB  2861587MiB  2861586MiB  ext4         primary
"""

EXPECTED_DRIVE_KEYS = (
    'drive_block_physical',
    'drive_block_logical',
    'drive_alignment_offset',
    'drive_discard_alignment',
    'drive_bytes',
    'drive_size',
    'drive_model',
    'drive_model_id',
    'drive_revision',
    'drive_serial',
    'drive_wwn',
    'drive_vendor',
    'drive_vendor_id',
    'drive_removable',
    'drive_bus',
    'drive_rpm',
)

EXPECTED_PARTITION_KEYS = EXPECTED_DRIVE_KEYS + (
    'partition_scheme',
    'partition_number',
    'partition_bytes',
    'partition_start_bytes',
    'partition_size',
    'partition_size',
    'filesystem_type',
    'filesystem_uuid',
    'filesystem_label',
)


def random_drive_dev():
    base = random.choice(['/dev/sd', '/dev/vd'])
    letter = random.choice(string.ascii_lowercase)
    dev = '{}{}'.format(base, letter)
    assert drives.VALID_DRIVE.match(dev)
    return dev


class TestConstants(TestCase):
    def test_VALID_DRIVE(self):
        self.assertIsInstance(drives.VALID_DRIVE, re._pattern_type)
        for base in ('/dev/sd', '/dev/vd'):
            for letter in string.ascii_lowercase:
                dev = base + letter
                m = drives.VALID_DRIVE.match(dev)
                self.assertIsNotNone(m)
                self.assertEqual(m.group(0), dev)

    def test_VALID_PARTITION(self):
        self.assertIsInstance(drives.VALID_PARTITION, re._pattern_type)
        for base in ('/dev/sd', '/dev/vd'):
            for letter in string.ascii_lowercase:
                for number in range(1, 10):
                    dev = '{}{}{:d}'.format(base, letter, number)
                    m = drives.VALID_PARTITION.match(dev)
                    self.assertIsNotNone(m)
                    self.assertEqual(m.group(0), dev)


def iter_drive_dev_name():
    for start in ('s', 'v'):
        for end in string.ascii_lowercase:
            yield '{}d{}'.format(start, end)
    for i in range(10):
        for j in range(1, 10):
            yield 'nvme{:d}n{:d}'.format(i, j)
    for i in range(10):
        yield 'mmcblk{:d}'.format(i)


def iter_drive_dev():
    for name in iter_drive_dev_name():
        yield '/dev/' + name


class TestFunctions(TestCase):
    def test_drive_dev(self):
        with self.assertRaises(ValueError) as cm:
            drives.check_drive_dev('/dev/sd1')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/sd1'"
        )
        self.assertEqual(drives.check_drive_dev('/dev/sda'), '/dev/sda')

        with self.assertRaises(ValueError) as cm:
            drives.check_drive_dev('/dev/vd1')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/vd1'"
        )
        self.assertEqual(drives.check_drive_dev('/dev/vda'), '/dev/vda')

        with self.assertRaises(ValueError) as cm:
            drives.check_drive_dev('/dev/nvme0n0')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/nvme0n0'"
        )
        self.assertEqual(drives.check_drive_dev('/dev/nvme0n1'), '/dev/nvme0n1')

        with self.assertRaises(ValueError) as cm:
            drives.check_drive_dev('/dev/mmcblka')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/mmcblka'"
        )
        self.assertEqual(drives.check_drive_dev('/dev/mmcblk0'), '/dev/mmcblk0')

        for name in iter_drive_dev_name():
            with self.assertRaises(ValueError) as cm:
                drives.check_drive_dev(name)
            self.assertEqual(str(cm.exception),
                'Invalid drive device file: {!r}'.format(name)
            )
            dev = '/dev/' + name
            self.assertEqual(drives.check_drive_dev(dev), dev)

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

    def test_get_drive_info(self):
        d = drives.Devices()
        for device in d.iter_drives():
            info = drives.get_drive_info(device)
            self.assertEqual(set(info), set(EXPECTED_DRIVE_KEYS))

    def test_get_partition_info(self):
        d = drives.Devices()
        for device in d.iter_partitions():
            info = drives.get_partition_info(device)
            self.assertEqual(set(info), set(EXPECTED_PARTITION_KEYS))

    def test_parse_mounts(self):
        mounts = drives.parse_mounts()
        self.assertIsInstance(mounts, dict)
        for (key, value) in mounts.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, str)


class TestMockable(TestCase):
    def test_init(self):
        inst = drives.Mockable()
        self.assertIs(inst.mocking, False)
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])

        inst = drives.Mockable(mocking=True)
        self.assertIs(inst.mocking, True)
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])

    def test_reset(self):
        inst = drives.Mockable()
        calls = inst.calls
        outputs = inst.outputs
        self.assertIsNone(inst.reset())
        self.assertIs(inst.mocking, False)
        self.assertIs(inst.calls, calls)
        self.assertEqual(inst.calls, [])
        self.assertIs(inst.outputs, outputs)
        self.assertEqual(inst.outputs, [])

        inst = drives.Mockable(mocking=True)
        calls = inst.calls
        outputs = inst.outputs
        inst.calls.extend(
            [('check_call', ['stuff']), ('check_output', ['junk'])]
        )
        inst.outputs.extend([b'foo', b'bar'])
        self.assertIsNone(inst.reset())
        self.assertIs(inst.mocking, False)
        self.assertIs(inst.calls, calls)
        self.assertEqual(inst.calls, [])
        self.assertIs(inst.outputs, outputs)
        self.assertEqual(inst.outputs, [])

        inst = drives.Mockable(mocking=True)
        calls = inst.calls
        outputs = inst.outputs
        inst.calls.extend(
            [('check_call', ['stuff']), ('check_output', ['junk'])]
        )
        inst.outputs.extend([b'foo', b'bar'])
        self.assertIsNone(inst.reset(mocking=True, outputs=[b'aye', b'bee']))
        self.assertIs(inst.mocking, True)
        self.assertIs(inst.calls, calls)
        self.assertEqual(inst.calls, [])
        self.assertIs(inst.outputs, outputs)
        self.assertEqual(inst.outputs, [b'aye', b'bee'])

    def test_check_call(self):
        inst = drives.Mockable()
        self.assertIsNone(inst.check_call(['/bin/true']))
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            inst.check_call(['/bin/false'])
        self.assertEqual(cm.exception.cmd, ['/bin/false'])
        self.assertEqual(cm.exception.returncode, 1)
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])

        inst = drives.Mockable(mocking=True)
        self.assertIsNone(inst.check_call(['/bin/true']))
        self.assertEqual(inst.calls, [
            ('check_call', ['/bin/true']),
        ])
        self.assertEqual(inst.outputs, [])
        self.assertIsNone(inst.check_call(['/bin/false']))
        self.assertEqual(inst.calls, [
            ('check_call', ['/bin/true']),
            ('check_call', ['/bin/false']),
        ])
        self.assertEqual(inst.outputs, [])

    def test_check_output(self):
        inst = drives.Mockable()
        self.assertEqual(inst.check_output(['/bin/echo', 'foobar']), b'foobar\n')
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            inst.check_output(['/bin/false', 'stuff'])
        self.assertEqual(cm.exception.cmd, ['/bin/false', 'stuff'])
        self.assertEqual(cm.exception.returncode, 1)
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.outputs, [])

        inst.reset(mocking=True, outputs=[b'foo', b'bar'])
        self.assertEqual(inst.check_output(['/bin/echo', 'stuff']), b'foo')
        self.assertEqual(inst.calls, [
            ('check_output', ['/bin/echo', 'stuff']),
        ])
        self.assertEqual(inst.outputs, [b'bar'])
        self.assertEqual(inst.check_output(['/bin/false', 'stuff']), b'bar')
        self.assertEqual(inst.calls, [
            ('check_output', ['/bin/echo', 'stuff']),
            ('check_output', ['/bin/false', 'stuff']),
        ])
        self.assertEqual(inst.outputs, [])


class TestDrive(TestCase):
    def test_init(self):
        for dev in ('/dev/sda', '/dev/sdz', '/dev/vda', '/dev/vdz'):
            inst = drives.Drive(dev)
            self.assertIs(inst.dev, dev)
            self.assertIs(inst.mocking, False)
            inst = drives.Drive(dev, mocking=True)
            self.assertIs(inst.dev, dev)
            self.assertIs(inst.mocking, True)
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

    def test_get_partition(self):
        for base in ('/dev/sd', '/dev/vd'):
            for letter in string.ascii_lowercase:
                dev = base + letter
                for number in range(1, 10):
                    # mocking=True
                    inst = drives.Drive(dev)
                    part = inst.get_partition(number)
                    self.assertIsInstance(part, drives.Partition)
                    self.assertEqual(part.dev, '{}{:d}'.format(dev, number))
                    self.assertIs(part.mocking, False)
                    # mocking=False
                    inst = drives.Drive(dev, mocking=True)
                    part = inst.get_partition(number)
                    self.assertIsInstance(part, drives.Partition)
                    self.assertEqual(part.dev, '{}{:d}'.format(dev, number))
                    self.assertIs(part.mocking, True)

    def test_rereadpt(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        self.assertIsNone(inst.rereadpt())
        self.assertEqual(inst.calls, [
            ('check_call', ['blockdev', '--rereadpt', dev]),
        ])

    def test_zero(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        self.assertIsNone(inst.zero())
        self.assertEqual(inst.calls, [
            ('check_call', ['dd', 'if=/dev/zero', 'of={}'.format(dev), 'bs=4M', 'count=1', 'oflag=sync']),
        ])

    def test_parted(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        self.assertEqual(inst.parted(),
            ['parted', '-s', dev, 'unit', 'MiB']
        )
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.parted('print'),
            ['parted', '-s', dev, 'unit', 'MiB', 'print']
        )
        self.assertEqual(inst.calls, [])
        self.assertEqual(inst.parted('mklabel', 'gpt'),
            ['parted', '-s', dev, 'unit', 'MiB', 'mklabel', 'gpt']
        )
        self.assertEqual(inst.calls, [])

    def test_mklabel(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        self.assertIsNone(inst.mklabel())
        self.assertEqual(inst.calls, [
            ('check_call',  ['parted', '-s', dev, 'unit', 'MiB', 'mklabel', 'gpt']),
        ])

    def test_print(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev)
        marker = random_id()
        inst.reset(mocking=True, outputs=[marker.encode('utf-8')])
        self.assertEqual(inst.print(), marker)
        self.assertEqual(inst.calls, [
            ('check_output',  ['parted', '-s', dev, 'unit', 'MiB', 'print']),
        ])
        self.assertEqual(inst.outputs, [])

    def test_init_partition_table(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev)
        inst.reset(mocking=True, outputs=[PARTED_PRINT.encode('utf-8')])
        self.assertIsNone(inst.init_partition_table())
        self.assertEqual(inst.calls, [
            ('check_call', ['blockdev', '--rereadpt', dev]),
            ('check_call', ['dd', 'if=/dev/zero', 'of={}'.format(dev), 'bs=4M', 'count=1', 'oflag=sync']),
            ('check_call', ['blockdev', '--rereadpt', dev]),
            ('check_call',  ['parted', '-s', dev, 'unit', 'MiB', 'mklabel', 'gpt']),
            ('check_output',  ['parted', '-s', dev, 'unit', 'MiB', 'print']),
        ])
        self.assertEqual(inst.outputs, [])
        self.assertEqual(inst.size, 2861588)
        self.assertEqual(inst.index, 0)
        self.assertEqual(inst.start, 1)
        self.assertEqual(inst.stop, 2861587)

    def test_mkpart(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        inst.start = 1
        inst.stop = 2861587
        self.assertIsNone(inst.mkpart(1, 2861587))
        self.assertEqual(inst.calls, [
            ('check_call', ['parted', '-s', dev, 'unit', 'MiB', 'mkpart', 'primary', 'ext2', '1', '2861587']),
        ])

    def test_remaining(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        inst.start = 1
        inst.stop = 2861587
        self.assertEqual(inst.remaining, 2861586)
        inst.start = 12345
        self.assertEqual(inst.remaining, 2849242)
        inst.stop = 1861587
        self.assertEqual(inst.remaining, 1849242)
        self.assertEqual(inst.calls, [])

    def test_add_partition(self):
        dev = random_drive_dev()
        inst = drives.Drive(dev, mocking=True)
        inst.index = 0
        inst.start = 1
        inst.stop = 2861587

        part = inst.add_partition(123456)
        self.assertIsInstance(part, drives.Partition)
        self.assertIs(part.mocking, True)
        self.assertEqual(part.dev, '{}{:d}'.format(dev, 1))
        self.assertEqual(part.calls, [])
        self.assertEqual(inst.index, 1)
        self.assertEqual(inst.calls, [
            ('check_call', ['parted', '-s', dev, 'unit', 'MiB', 'mkpart', 'primary', 'ext2', '1', '123457']),
        ])
        self.assertEqual(inst.remaining, 2738130)

        part = inst.add_partition(2738130)
        self.assertIsInstance(part, drives.Partition)
        self.assertIs(part.mocking, True)
        self.assertEqual(part.dev, '{}{:d}'.format(dev, 2))
        self.assertEqual(part.calls, [])
        self.assertEqual(inst.index, 2)
        self.assertEqual(inst.calls, [
            ('check_call', ['parted', '-s', dev, 'unit', 'MiB', 'mkpart', 'primary', 'ext2', '1', '123457']),
            ('check_call', ['parted', '-s', dev, 'unit', 'MiB', 'mkpart', 'primary', 'ext2', '123457', '2861587']),
        ])
        self.assertEqual(inst.remaining, 0)


class TestPartition(TestCase):
    def test_init(self):
        for drive_dev in iter_drive_dev():
            for index in range(1, 10):
                dev = drives.get_partition_dev(drive_dev, index)
                p = drives.Partition(drive_dev, index)
                self.assertIs(p.mocking, False)
                self.assertIs(p.drive_dev, drive_dev)
                self.assertIs(p.index, index)
                self.assertEqual(p.dev, dev)
                p = drives.Partition(drive_dev, index, mocking=True)
                self.assertIs(p.mocking, True)
                self.assertIs(p.drive_dev, drive_dev)
                self.assertIs(p.index, index)
                self.assertEqual(p.dev, dev)

        p = drives.Partition('/dev/sdb', 1)
        self.assertEqual(p.dev, '/dev/sdb1')

        p = drives.Partition('/dev/vda', 1)
        self.assertEqual(p.dev, '/dev/vda1')

        p = drives.Partition('/dev/nvme0n1', 1)
        self.assertEqual(p.dev, '/dev/nvme0n1p1')

        p = drives.Partition('/dev/mmcblk0', 1)
        self.assertEqual(p.dev, '/dev/mmcblk0p1')

    def test_mkfs_ext4(self):
        label = 'xvstyjkhbqxocf8g'
        _id = 'ROKFRSNKQCEF6BFEWK3X5MRY'
        uuid = 'c562cc66-91ba-56c1-a18b-ec41e14f1f44'
        p = drives.Partition('/dev/sdb', 1, mocking=True)
        self.assertIsNone(p.mkfs_ext4(label, _id))
        self.assertEqual(p.calls, [
            (
                'check_call',
                ['mkfs.ext4', '/dev/sdb1', '-L', label, '-U', uuid, '-m', '0']
            ),
        ])

        for drive_dev in iter_drive_dev():
            for index in range(1, 10):
                p = drives.Partition(drive_dev, index, mocking=True)
                dev = drives.get_partition_dev(drive_dev, index)
                label = random_id(5)
                _id = random_id()
                uuid = drives.db32_to_uuid(_id)
                self.assertIsNone(p.mkfs_ext4(label, _id))
                self.assertEqual(p.calls, [
                    (
                        'check_call',
                        ['mkfs.ext4', dev, '-L', label, '-U', uuid, '-m', '0']
                    ),
                ])

    def test_create_filestore(self):
        for drive_dev in iter_drive_dev():
            for index in range(1, 10):
                p = drives.Partition(drive_dev, index, mocking=True)
                dev = drives.get_partition_dev(drive_dev, index)
                tmp = TempDir()
                _id = random_id()
                serial = random_id(10)
                uuid = drives.db32_to_uuid(_id)
                label = random_id(5)
                kw = {
                    'drive_serial': serial,
                    'filesystem_type': 'ext4',
                    'filesystem_uuid': uuid,
                    'filesystem_label': label,
                }
                doc = p.create_filestore(tmp.dir, _id, 1, **kw)
                self.assertIsInstance(doc, dict)
                self.assertEqual(set(doc), set([
                    '_id',
                    'time',
                    'type',
                    'plugin',
                    'copies',
                    'drive_serial',
                    'filesystem_type',
                    'filesystem_uuid',
                    'filesystem_label',
                ]))
                self.assertEqual(doc, {
                    '_id': _id,
                    'time': doc['time'],
                    'type': 'dmedia/store',
                    'plugin': 'filestore',
                    'copies': 1,
                    'drive_serial': serial,
                    'filesystem_type': 'ext4',
                    'filesystem_uuid': uuid,
                    'filesystem_label': label,
                })
                fs = FileStore(tmp.dir, _id)
                self.assertEqual(fs.doc, doc)
                del fs
                self.assertEqual(p.calls, [
                    ('check_call', ['mount', dev, tmp.dir]),
                    ('check_call', ['chmod', '0777', tmp.dir]),
                    ('check_call', ['umount', dev]),
                ])


class TestDeviceNotFound(TestCase):
    def test_init(self):
        dev = '/dev/{}'.format(random_id())
        inst = drives.DeviceNotFound(dev)
        self.assertIs(inst.dev, dev)
        self.assertEqual(str(inst), 'No such device: {!r}'.format(dev))


class TestDevices(TestCase):
    def test_init(self):
        inst = drives.Devices()
        self.assertIsInstance(inst.udev_client, GUdev.Client)

        marker = random_id()

        class Subclass(drives.Devices):
            def get_udev_client(self):
                return marker

        inst = Subclass()
        self.assertIs(inst.udev_client, marker)

    def test_get_device(self):
        inst = drives.Devices()
        dev = '/dev/nopenopenope'
        with self.assertRaises(drives.DeviceNotFound) as cm:
            inst.get_device(dev)
        self.assertIs(cm.exception.dev, dev)
        self.assertEqual(str(cm.exception),
            "No such device: '/dev/nopenopenope'"
        )

    def test_get_drive_info(self):
        inst = drives.Devices()
        for device in inst.iter_drives():
            dev = device.get_device_file()
            self.assertEqual(
                inst.get_drive_info(dev),
                drives.get_drive_info(device),
            )
        with self.assertRaises(ValueError) as cm:
            inst.get_drive_info('/dev/sdaa')
        self.assertEqual(str(cm.exception),
            "Invalid drive device file: '/dev/sdaa'"
        )

    def test_get_partition_info(self):
        inst = drives.Devices()
        for device in inst.iter_partitions():
            dev = device.get_device_file()
            self.assertEqual(
                inst.get_partition_info(dev),
                drives.get_partition_info(device),
            )
        with self.assertRaises(ValueError) as cm:
            inst.get_partition_info('/dev/sda11')
        self.assertEqual(str(cm.exception),
            "Invalid partition device file: '/dev/sda11'"
        )

    def test_iter_drives(self):
        inst = drives.Devices()
        for drive in inst.iter_drives():
            self.assertIsInstance(drive, GUdev.Device)
            self.assertEqual(drive.get_devtype(), 'disk')
            self.assertTrue(drives.VALID_DRIVE.match(drive.get_device_file()))

    def test_iter_partitions(self):
        inst = drives.Devices()
        for drive in inst.iter_partitions():
            self.assertIsInstance(drive, GUdev.Device)
            self.assertEqual(drive.get_devtype(), 'partition')
            self.assertTrue(drives.VALID_PARTITION.match(drive.get_device_file()))

    def test_get_parentdir_info(self):
        inst = drives.Devices()
        info = inst.get_parentdir_info('/')
        self.assertIsInstance(info, dict)
        if info != {}:
            self.assertEqual(set(info), set(EXPECTED_PARTITION_KEYS))

    def test_get_info(self):
        inst = drives.Devices()
        info = inst.get_info()
        self.assertIsInstance(info, dict)
        self.assertEqual(set(info), set(['drives', 'partitions']))
