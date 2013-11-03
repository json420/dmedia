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


def random_partition_dev():
    drive_dev = random_drive_dev()
    number = random.randint(1, 9)
    dev = '{}{:d}'.format(drive_dev, number)
    assert drives.VALID_PARTITION.match(dev)
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
                    self.assertEqual(m.group(1), base + letter)
                    self.assertEqual(m.group(2), str(number))


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
            m = drives.VALID_PARTITION.match(device.get_device_file())
            self.assertIsNotNone(m)
            drive_device = d.get_device(m.group(1))
            sub = dict(
                (key, info[key]) for key in EXPECTED_DRIVE_KEYS
            )
            self.assertEqual(sub, drives.get_drive_info(drive_device))

    def test_parse_mounts(self):
        mounts = drives.parse_mounts()
        self.assertIsInstance(mounts, dict)
        for (key, value) in mounts.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, str)


class TestMockable(TestCase):
    def test_init(self):
        m = drives.Mockable()
        self.assertIs(m.mocking, False)
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])

        m = drives.Mockable(mocking=True)
        self.assertIs(m.mocking, True)
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])

    def test_reset(self):
        m = drives.Mockable()
        calls = m.calls
        outputs = m.outputs
        self.assertIsNone(m.reset())
        self.assertIs(m.mocking, False)
        self.assertIs(m.calls, calls)
        self.assertEqual(m.calls, [])
        self.assertIs(m.outputs, outputs)
        self.assertEqual(m.outputs, [])

        m = drives.Mockable(mocking=True)
        calls = m.calls
        outputs = m.outputs
        m.calls.extend(
            [('check_call', ['stuff']), ('check_output', ['junk'])]
        )
        m.outputs.extend([b'foo', b'bar'])
        self.assertIsNone(m.reset())
        self.assertIs(m.mocking, False)
        self.assertIs(m.calls, calls)
        self.assertEqual(m.calls, [])
        self.assertIs(m.outputs, outputs)
        self.assertEqual(m.outputs, [])

        m = drives.Mockable(mocking=True)
        calls = m.calls
        outputs = m.outputs
        m.calls.extend(
            [('check_call', ['stuff']), ('check_output', ['junk'])]
        )
        m.outputs.extend([b'foo', b'bar'])
        self.assertIsNone(m.reset(mocking=True, outputs=[b'aye', b'bee']))
        self.assertIs(m.mocking, True)
        self.assertIs(m.calls, calls)
        self.assertEqual(m.calls, [])
        self.assertIs(m.outputs, outputs)
        self.assertEqual(m.outputs, [b'aye', b'bee'])

    def test_check_call(self):
        m = drives.Mockable()
        self.assertIsNone(m.check_call(['/bin/true']))
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            m.check_call(['/bin/false'])
        self.assertEqual(cm.exception.cmd, ['/bin/false'])
        self.assertEqual(cm.exception.returncode, 1)
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])

        m = drives.Mockable(mocking=True)
        self.assertIsNone(m.check_call(['/bin/true']))
        self.assertEqual(m.calls, [
            ('check_call', ['/bin/true']),
        ])
        self.assertEqual(m.outputs, [])
        self.assertIsNone(m.check_call(['/bin/false']))
        self.assertEqual(m.calls, [
            ('check_call', ['/bin/true']),
            ('check_call', ['/bin/false']),
        ])
        self.assertEqual(m.outputs, [])

    def test_check_output(self):
        m = drives.Mockable()
        self.assertEqual(m.check_output(['/bin/echo', 'foobar']), b'foobar\n')
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            m.check_output(['/bin/false', 'stuff'])
        self.assertEqual(cm.exception.cmd, ['/bin/false', 'stuff'])
        self.assertEqual(cm.exception.returncode, 1)
        self.assertEqual(m.calls, [])
        self.assertEqual(m.outputs, [])

        m.reset(mocking=True, outputs=[b'foo', b'bar'])
        self.assertEqual(m.check_output(['/bin/echo', 'stuff']), b'foo')
        self.assertEqual(m.calls, [
            ('check_output', ['/bin/echo', 'stuff']),
        ])
        self.assertEqual(m.outputs, [b'bar'])
        self.assertEqual(m.check_output(['/bin/false', 'stuff']), b'bar')
        self.assertEqual(m.calls, [
            ('check_output', ['/bin/echo', 'stuff']),
            ('check_output', ['/bin/false', 'stuff']),
        ])
        self.assertEqual(m.outputs, [])


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
        for base in ('/dev/sd', '/dev/vd'):
            for letter in string.ascii_lowercase:
                for number in range(1, 10):
                    dev = '{}{}{:d}'.format(base, letter, number)
                    inst = drives.Partition(dev)
                    self.assertIs(inst.dev, dev)
                    self.assertIs(inst.mocking, False)
                    inst = drives.Partition(dev, mocking=True)
                    self.assertIs(inst.dev, dev)
                    self.assertIs(inst.mocking, True)

        with self.assertRaises(ValueError) as cm:
            drives.Partition('/dev/sda11')
        self.assertEqual(str(cm.exception),
            "Invalid partition device file: '/dev/sda11'"
        )

        with self.assertRaises(ValueError) as cm:
            drives.Partition('/dev/sda0')
        self.assertEqual(str(cm.exception),
            "Invalid partition device file: '/dev/sda0'"
        )

    def test_mkfs_ext4(self):
        dev = random_partition_dev()
        inst = drives.Partition(dev, mocking=True)
        label = random_id(5)
        store_id = random_id()
        ext4_uuid = drives.db32_to_uuid(store_id)
        self.assertIsNone(inst.mkfs_ext4(label, store_id))
        self.assertEqual(inst.calls, [
            ('check_call', ['mkfs.ext4', dev, '-L', label, '-U', ext4_uuid, '-m', '0']),
        ])

    def test_create_filestore(self):
        dev = random_partition_dev()
        inst = drives.Partition(dev, mocking=True)
        tmp = TempDir()
        store_id = random_id()
        ext4_uuid = drives.db32_to_uuid(store_id)
        label = random_id(5)
        serial = random_id(10)
        kw = {
            'drive_serial': serial,
            'filesystem_type': 'ext4',
            'filesystem_uuid': ext4_uuid,
            'filesystem_label': label,
        }
        doc = inst.create_filestore(tmp.dir, store_id, 1, **kw)
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
            '_id': store_id,
            'time': doc['time'],
            'type': 'dmedia/store',
            'plugin': 'filestore',
            'copies': 1,
            'drive_serial': serial,
            'filesystem_type': 'ext4',
            'filesystem_uuid': ext4_uuid,
            'filesystem_label': label,
        })
        fs = FileStore(tmp.dir, store_id)
        self.assertEqual(fs.doc, doc)
        del fs
        self.assertEqual(inst.calls, [
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
        d = drives.Devices()
        self.assertIsInstance(d.udev_client, GUdev.Client)

        marker = random_id()

        class Subclass(drives.Devices):
            def get_udev_client(self):
                return marker

        d = Subclass()
        self.assertIs(d.udev_client, marker)

    def test_get_device(self):
        d = drives.Devices()
        dev = '/dev/nopenopenope'
        with self.assertRaises(drives.DeviceNotFound) as cm:
            d.get_device(dev)
        self.assertIs(cm.exception.dev, dev)
        self.assertEqual(str(cm.exception),
            "No such device: '/dev/nopenopenope'"
        )

    def test_iter_drives(self):
        d = drives.Devices()
        for drive in d.iter_drives():
            self.assertIsInstance(drive, GUdev.Device)
            self.assertEqual(drive.get_devtype(), 'disk')
            self.assertTrue(drives.VALID_DRIVE.match(drive.get_device_file()))

    def test_iter_partitions(self):
        d = drives.Devices()
        for drive in d.iter_partitions():
            self.assertIsInstance(drive, GUdev.Device)
            self.assertEqual(drive.get_devtype(), 'partition')
            self.assertTrue(drives.VALID_PARTITION.match(drive.get_device_file()))

    def test_get_info(self):
        d = drives.Devices()
        info = d.get_info()
        self.assertIsInstance(info, dict)
        self.assertEqual(set(info), set(['drives', 'partitions']))
