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
from subprocess import check_call, check_output
import re
import time
import tempfile
import os

from gi.repository import GUdev
from filestore import FileStore
from dbase32 import db32dec, db32enc

from .units import bytes10


udev_client = GUdev.Client.new(['block'])
VALID_DRIVE = re.compile('^/dev/[sv]d[a-z]$')
VALID_PARTITION = re.compile('^/dev/[sv]d[a-z][1-9]$')


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


def get_device(dev):
    device = udev_client.query_by_device_file(dev)
    if device is None:
        raise Exception('No such device: {!r}'.format(dev))  
    return device


def get_drive_info(device):
    physical = device.get_sysfs_attr_as_uint64('queue/physical_block_size')
    logical = device.get_sysfs_attr_as_uint64('queue/logical_block_size')
    drive_sectors = device.get_sysfs_attr_as_uint64('size')
    drive_bytes = drive_sectors * logical
    return {
        'drive_block_physical': physical,
        'drive_block_logical': logical,
        'drive_bytes': drive_bytes,
        'drive_size': bytes10(drive_bytes),
        'drive_model': device.get_property('ID_MODEL'),
        'drive_model_id': device.get_property('ID_MODEL_ID'),
        'drive_revision': device.get_property('ID_REVISION'),
        'drive_serial': device.get_property('ID_SERIAL_SHORT'),
        'drive_wwn': device.get_property('ID_WWN_WITH_EXTENSION'),
        'drive_vendor': device.get_property('ID_VENDOR'),
        'drive_removable': bool(device.get_sysfs_attr_as_int('removable')),
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
        'drive_removable': bool(device.get_sysfs_attr_as_int('../removable')),
        'drive_bus': device.get_property('ID_BUS'),

        'partition_scheme': device.get_property('ID_PART_ENTRY_SCHEME'),
        'partition_number': device.get_property_as_int('ID_PART_ENTRY_NUMBER'),
        'partition_bytes': part_bytes,
        'partition_size': bytes10(part_bytes),
        'partition_start_bytes': part_start_sector * logical,

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


class Drive:
    def __init__(self, dev):
        if not VALID_DRIVE.match(dev):
            raise ValueError('Invalid drive device file: {!r}'.format(dev))
        self.dev = dev

    def get_partition(self, index):
        assert isinstance(index, int)
        assert index >= 1
        return Partition('{}{}'.format(self.dev, index))

    def rereadpt(self):
        check_call(['blockdev', '--rereadpt', self.dev])

    def zero(self):
        check_call(['dd',
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
        check_call(self.parted('mklabel', 'gpt'))

    def print(self):
        cmd = self.parted('print')
        return check_output(cmd).decode('utf-8')

    def init_partition_table(self):
        self.rereadpt()  # Make sure existing partitions aren't mounted
        self.zero()
        time.sleep(2)
        self.rereadpt()
        self.mklabel()

        text = self.print()
        print(text)
        self.size = parse_drive_size(text)
        self.index = 0
        self.start = 1
        self.stop = self.size - 1
        assert self.start < self.stop

    @property
    def remaining(self):
        return self.stop - self.start

    def mkpart(self, start, stop):
        assert isinstance(start, int)
        assert isinstance(stop, int)
        assert 1 <= start < stop <= self.stop
        cmd = self.parted('mkpart', 'primary', 'ext2', str(start), str(stop))
        check_call(cmd)

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
        time.sleep(2)
        doc = partition.create_filestore(store_id)
        return doc


class Partition:
    def __init__(self, dev):
        if not VALID_PARTITION.match(dev):
            raise ValueError('Invalid partition device file: {!r}'.format(dev))
        self.dev = dev

    def get_info(self):
        return get_partition_info(get_device(self.dev))

    def mkfs_ext4(self, label, store_id):
        cmd = ['mkfs.ext4', self.dev,
            '-L', label,
            '-U', db32_to_uuid(store_id),
            '-m', '0',  # 0% reserved blocks
        ]
        check_call(cmd)

    def create_filestore(self, store_id, copies=1):
        kw = self.get_info()
        tmpdir = tempfile.mkdtemp(prefix='dmedia.')
        fs = None
        check_call(['mount', self.dev, tmpdir])
        try:
            fs = FileStore.create(tmpdir, store_id, 1, **kw)
            check_call(['chmod', '0777', tmpdir])
            return fs.doc
        finally:
            del fs
            check_call(['umount', tmpdir])
            os.rmdir(tmpdir)

