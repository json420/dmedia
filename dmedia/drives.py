# dmedia: distributed media library
# Copyright (C) 2013 Novacut Inc
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
Get drive and partition info using udev.
"""

from uuid import UUID
import subprocess
import re
import time
import os
from os import path

from gi.repository import GUdev
from filestore import FileStore, _dumps
from dbase32 import db32dec, db32enc

from .units import bytes10


VALID_DRIVE = re.compile('^/dev/[sv]d[a-z]$')
VALID_PARTITION = re.compile('^(/dev/[sv]d[a-z])([1-9])$')


def check_drive_dev(dev):
    if not VALID_DRIVE.match(dev):
        raise ValueError('Invalid drive device file: {!r}'.format(dev))
    return dev


def check_partition_dev(dev):
    if not VALID_PARTITION.match(dev):
        raise ValueError('Invalid partition device file: {!r}'.format(dev))
    return dev


def db32_to_uuid(store_id):
    """
    Convert a 120-bit Dbase32 encoded ID into a 128-bit UUID.

    This is used to create a stable mapping between Dmedia FileStore IDs and
    ext4 UUIDs.

    For example:

    >>> db32_to_uuid('333333333333333333333333')
    '00000000-0000-0000-0000-000000000044'
    >>> db32_to_uuid('YYYYYYYYYYYYYYYYYYYYYYYY')
    'ffffffff-ffff-ffff-ffff-ffffffffff44'

    """
    assert len(store_id) == 24
    uuid_bytes = db32dec(store_id) + b'D'
    assert len(uuid_bytes) == 16
    return str(UUID(bytes=uuid_bytes))


def uuid_to_db32(uuid_hex):
    """
    Convert a 128-bit UUID into a 120-bit Dbase32 encoded ID.

    This is used to create a stable mapping between Dmedia FileStore IDs and
    ext4 UUIDs.

    For example:

    >>> uuid_to_db32('00000000-0000-0000-0000-000000000044')
    '333333333333333333333333'

    >>> uuid_to_db32('ffffffff-ffff-ffff-ffff-ffffffffff44')
    'YYYYYYYYYYYYYYYYYYYYYYYY'

    """
    uuid_bytes = UUID(hex=uuid_hex).bytes
    assert len(uuid_bytes) == 16
    return db32enc(uuid_bytes[:15])


def unfuck(string):
    if string is None:
        return
    return string.replace('\\x20', ' ').strip()


def get_drive_info(device):
    physical = device.get_sysfs_attr_as_uint64('queue/physical_block_size')
    logical = device.get_sysfs_attr_as_uint64('queue/logical_block_size')
    drive_sectors = device.get_sysfs_attr_as_uint64('size')
    drive_bytes = drive_sectors * logical
    return {
        'drive_block_physical': physical,
        'drive_block_logical': logical,
        'drive_alignment_offset': device.get_sysfs_attr_as_int('alignment_offset'),
        'drive_discard_alignment': device.get_sysfs_attr_as_int('discard_alignment'),
        'drive_bytes': drive_bytes,
        'drive_size': bytes10(drive_bytes),
        'drive_model': unfuck(device.get_property('ID_MODEL_ENC')),
        'drive_model_id': device.get_property('ID_MODEL_ID'),
        'drive_revision': device.get_property('ID_REVISION'),
        'drive_serial': device.get_property('ID_SERIAL_SHORT'),
        'drive_wwn': device.get_property('ID_WWN_WITH_EXTENSION'),
        'drive_vendor': unfuck(device.get_property('ID_VENDOR_ENC')),
        'drive_vendor_id': device.get_property('ID_VENDOR_ID'),
        'drive_removable': bool(device.get_sysfs_attr_as_int('removable')),
        'drive_bus': device.get_property('ID_BUS'),
        'drive_rpm': device.get_property('ID_ATA_ROTATION_RATE_RPM'),
    }


def get_partition_info(device):
    physical = device.get_sysfs_attr_as_uint64('../queue/physical_block_size')
    logical = device.get_sysfs_attr_as_uint64('../queue/logical_block_size')
    drive_sectors = device.get_sysfs_attr_as_uint64('../size')
    part_sectors = device.get_sysfs_attr_as_uint64('size')
    part_start_sector = device.get_sysfs_attr_as_uint64('start')
    drive_bytes = drive_sectors * logical
    part_bytes = part_sectors * logical
    return {
        'drive_block_physical': physical,
        'drive_block_logical': logical,
        'drive_alignment_offset': device.get_sysfs_attr_as_int('../alignment_offset'),
        'drive_discard_alignment': device.get_sysfs_attr_as_int('../discard_alignment'),
        'drive_bytes': drive_bytes,
        'drive_size': bytes10(drive_bytes),
        'drive_model': unfuck(device.get_property('ID_MODEL_ENC')),
        'drive_model_id': device.get_property('ID_MODEL_ID'),
        'drive_revision': device.get_property('ID_REVISION'),
        'drive_serial': device.get_property('ID_SERIAL_SHORT'),
        'drive_wwn': device.get_property('ID_WWN_WITH_EXTENSION'),
        'drive_vendor': unfuck(device.get_property('ID_VENDOR_ENC')),
        'drive_vendor_id': device.get_property('ID_VENDOR_ID'),
        'drive_removable': bool(device.get_sysfs_attr_as_int('../removable')),
        'drive_bus': device.get_property('ID_BUS'),
        'drive_rpm': device.get_property('ID_ATA_ROTATION_RATE_RPM'),

        'partition_scheme': device.get_property('ID_PART_ENTRY_SCHEME'),
        'partition_number': device.get_property_as_int('ID_PART_ENTRY_NUMBER'),
        'partition_bytes': part_bytes,
        'partition_start_bytes': part_start_sector * logical,
        'partition_size': bytes10(part_bytes),

        'filesystem_type': device.get_property('ID_FS_TYPE'),
        'filesystem_uuid': device.get_property('ID_FS_UUID'),
        'filesystem_label': unfuck(device.get_property('ID_FS_LABEL_ENC')),
    }


def parse_drive_size(text):
    regex = re.compile('Disk /dev/\w+: (\d+)MiB')
    for line in text.splitlines():
        match = regex.match(line)
        if match:
            return int(match.group(1))
    raise ValueError('Could not find disk size with unit=MiB')


def parse_mounts(procdir='/proc'):
    text = open(path.join(procdir, 'mounts'), 'r').read()
    mounts = {}
    for line in text.splitlines():
        (dev, mount, type_, options, dump, pass_) = line.split()
        mounts[mount.replace('\\040', ' ')] = dev
    return mounts


class Mockable:
    """
    Mock calls to `subprocess.check_call()`, `subprocess.check_output()`.
    """

    def __init__(self, mocking=False):
        assert isinstance(mocking, bool)
        self.mocking = mocking
        self.calls = []
        self.outputs = []

    def reset(self, mocking=False, outputs=None):
        assert isinstance(mocking, bool)
        self.mocking = mocking
        self.calls.clear()
        self.outputs.clear()
        if outputs:
            assert mocking is True
            for value in outputs:
                assert isinstance(value, bytes)
                self.outputs.append(value)

    def check_call(self, cmd):
        assert isinstance(cmd, list)
        if self.mocking:
            self.calls.append(('check_call', cmd))
        else:
            subprocess.check_call(cmd)

    def check_output(self, cmd):
        assert isinstance(cmd, list)
        if self.mocking:
            self.calls.append(('check_output', cmd))
            return self.outputs.pop(0)
        else:
            return subprocess.check_output(cmd)


class Drive(Mockable):
    def __init__(self, dev, mocking=False):
        super().__init__(mocking)
        self.dev = check_drive_dev(dev)

    def get_partition(self, index):
        assert isinstance(index, int)
        assert index >= 1
        return Partition('{}{}'.format(self.dev, index), mocking=self.mocking)

    def rereadpt(self):
        self.check_call(['blockdev', '--rereadpt', self.dev])

    def zero(self):
        self.check_call(['dd',
            'if=/dev/zero',
            'of={}'.format(self.dev),
            'bs=4M',
            'count=1',
            'oflag=sync',
        ])

    def parted(self, *args):
        """
        Helper for building parted commands with the shared initial args.
        """
        cmd = ['parted', '-s', self.dev, 'unit', 'MiB']
        cmd.extend(args)
        return cmd

    def mklabel(self):
        self.check_call(self.parted('mklabel', 'gpt'))

    def print(self):
        cmd = self.parted('print')
        return self.check_output(cmd).decode('utf-8')

    def init_partition_table(self):
        self.rereadpt()  # Make sure existing partitions aren't mounted
        self.zero()
        time.sleep(1)
        self.rereadpt()
        self.mklabel()
        self.size = parse_drive_size(self.print())
        self.index = 0
        self.start = 1
        self.stop = self.size - 1
        assert self.start < self.stop

    def mkpart(self, start, stop):
        assert isinstance(start, int)
        assert isinstance(stop, int)
        assert 1 <= start < stop <= self.stop
        cmd = self.parted('mkpart', 'primary', 'ext2', str(start), str(stop))
        self.check_call(cmd)

    @property
    def remaining(self):
        return self.stop - self.start

    def add_partition(self, size):
        assert isinstance(size, int)
        assert 1 <= size <= self.remaining
        start = self.start
        self.start += size
        self.mkpart(start, self.start)
        self.index += 1
        return self.get_partition(self.index)

    def provision(self, label, store_id):
        self.init_partition_table()
        partition = self.add_partition(self.remaining)
        partition.mkfs_ext4(label, store_id)
        time.sleep(1)
        return partition


class Partition(Mockable):
    def __init__(self, dev, mocking=False):
        super().__init__(mocking)
        self.dev = check_partition_dev(dev)

    def mkfs_ext4(self, label, store_id):
        cmd = ['mkfs.ext4', self.dev,
            '-L', label,
            '-U', db32_to_uuid(store_id),
            '-m', '0',  # 0% reserved blocks
        ]
        self.check_call(cmd)

    def create_filestore(self, mount, store_id=None, copies=1, **kw):
        fs = None
        self.check_call(['mount', self.dev, mount])
        try:
            fs = FileStore.create(mount, store_id, copies, **kw)
            self.check_call(['chmod', '0777', mount])
            return fs.doc
        finally:
            del fs
            self.check_call(['umount', self.dev])


class DeviceNotFound(Exception):
    def __init__(self, dev):
        self.dev = dev
        super().__init__('No such device: {!r}'.format(dev))


class Devices:
    """
    Gather disk and partition info using udev.
    """

    def __init__(self):
        self.udev_client = self.get_udev_client()

    def get_udev_client(self):
        """
        Making this easy to override for mocking purposes.
        """
        return GUdev.Client.new(['block'])

    def get_device(self, dev):
        """
        Get a device object by its dev path (eg, ``'/dev/sda'``).
        """
        device = self.udev_client.query_by_device_file(dev)
        if device is None:
            raise DeviceNotFound(dev)
        return device

    def get_drive_info(self, dev):
        device = self.get_device(check_drive_dev(dev))
        return get_drive_info(device)

    def get_partition_info(self, dev):
        device = self.get_device(check_partition_dev(dev))
        return get_partition_info(device)

    def iter_drives(self):
        for device in self.udev_client.query_by_subsystem('block'):
            if device.get_devtype() != 'disk':
                continue
            if VALID_DRIVE.match(device.get_device_file()):
                yield device

    def iter_partitions(self):
        for device in self.udev_client.query_by_subsystem('block'):
            if device.get_devtype() != 'partition':
                continue
            if VALID_PARTITION.match(device.get_device_file()):
                yield device

    def get_parentdir_info(self, parentdir):
        assert path.abspath(parentdir) == parentdir
        mounts = parse_mounts()
        mountdir = parentdir
        while True:
            if mountdir in mounts:
                try:
                    device = self.get_device(mounts[mountdir])
                    return get_partition_info(device)
                except DeviceNotFound:
                    pass
            if mountdir == '/':
                return {}
            mountdir = path.dirname(mountdir)

    def get_info(self):
        return {
            'drives': dict(
                (drive.get_device_file(), get_drive_info(drive))
                for drive in self.iter_drives()
            ),
            'partitions': dict(
                (partition.get_device_file(), get_drive_info(partition))
                for partition in self.iter_partitions()
            ),
        }


if __name__ == '__main__':
    d = Devices()
    print(_dumps(d.get_info()))
    print(_dumps(parse_mounts()))
    print(_dumps(d.get_parentdir_info('/home/jderose/Videos')))
