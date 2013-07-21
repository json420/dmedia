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
MiB = 1024**2


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


def get_blkid_info(dev):
    text = check_output(['blkid', '-o', 'export', dev]).decode('utf-8')
    return dict(line.split('=') for line in text.splitlines())


def get_partition_info(device):
    physical = device.get_sysfs_attr_as_uint64('../queue/physical_block_size')
    logical = device.get_sysfs_attr_as_uint64('../queue/logical_block_size')
    drive_sectors = device.get_sysfs_attr_as_uint64('../size')
    part_sectors = device.get_sysfs_attr_as_uint64('size')
    part_start_sector = device.get_sysfs_attr_as_uint64('start')
    drive_bytes = drive_sectors * logical
    part_bytes = part_sectors * logical
    blkid_info = get_blkid_info(device.get_device_file())
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

        'partition_scheme': device.get_property('ID_PART_ENTRY_SCHEME'),
        'partition_number': device.get_property_as_int('ID_PART_ENTRY_NUMBER'),
        'partition_bytes': part_bytes,
        'partition_size': bytes10(part_bytes),
        'partition_start_bytes': part_start_sector * logical,

        # udev replaces spaces in LABEL with understore, so use blkid directly:
        # 'filesystem_type': device.get_property('ID_FS_TYPE'),
        # 'filesystem_uuid': device.get_property('ID_FS_UUID'),
        # 'filesystem_label': device.get_property('ID_FS_LABEL'),
        'filesystem_type': blkid_info['TYPE'],
        'filesystem_uuid': blkid_info['UUID'],
        'filesystem_label': blkid_info['LABEL'],
    }


def parse_disk_size(text):
    regex = re.compile('Disk /dev/\w+: (\d+)MiB')
    for line in text.splitlines():
        match = regex.match(line)
        if match:
            return int(match.group(1))
    raise ValueError('Could not find disk size with unit=MiB')


def parse_sector_size(text):
    regex = re.compile('Sector size \(logical/physical\): (\d+)B/(\d+)B')
    for line in text.splitlines():
        match = regex.match(line)
        if match:
            return tuple(int(match.group(i)) for i in [1, 2])
    raise ValueError('Could not find sector size')


class Drive:
    def __init__(self, dev):
        assert dev.startswith('/dev/sd') or dev.startswith('/dev/vd')
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
        cmd = ['parted', '-s', self.dev]
        cmd.extend(args)
        return cmd

    def mklabel(self):
        check_call(self.parted('mklabel', 'gpt'))

    def print_MiB(self):
        cmd = self.parted('unit', 'MiB', 'print')
        return check_output(cmd).decode('utf-8')

    def init_partition_table(self):
        self.rereadpt()
        self.zero()
        time.sleep(2)
        self.rereadpt()
        self.mklabel()

        self.index = 0
        text = self.print_MiB()
        self.size_MiB = parse_disk_size(text)
        (self.logical, self.physical) = parse_sector_size(text)
        self.sectors_per_MiB = MiB // self.logical
        self.start_MiB = 1
        self.stop_MiB = self.size_MiB - 1

        assert self.logical in (512, 4096)
        assert self.physical in (512, 4096)
        assert self.logical <= self.physical
        assert self.sectors_per_MiB in (2048, 256)

        print(self.print_MiB())

    @property
    def remaining_MiB(self):
        return self.stop_MiB - self.start_MiB

    def mkpart(self, start_MiB, stop_MiB):
        assert isinstance(start_MiB, int)
        assert isinstance(stop_MiB, int)
        assert 1 <= start_MiB < stop_MiB <= self.stop_MiB

        assert self.sectors_per_MiB in (2048, 256)
        start = start_MiB * self.sectors_per_MiB
        end = stop_MiB * self.sectors_per_MiB - 1
        assert start % 2 == 0  # start should always be an even sector number
        assert end % 2 == 1  # end should always be an odd sector number

        cmd = self.parted(
            'unit', 's', 'mkpart', 'primary', 'ext2', str(start), str(end)
        )
        check_call(cmd)

    def add_partition(self, size_MiB):
        assert isinstance(size_MiB, int)
        assert 1 <= size_MiB <= self.remaining_MiB
        start_MiB = self.start_MiB
        self.start_MiB += size_MiB
        self.mkpart(start_MiB, self.start_MiB)
        self.index += 1
        return self.get_partition(self.index)

    def provision(self, label, store_id):
        self.init_partition_table()
        partition = self.add_partition(self.remaining_MiB)
        partition.mkfs_ext4(label, store_id)
        doc = partition.create_filestore(store_id)
        return doc


class Partition:
    def __init__(self, dev):
        assert dev.startswith('/dev/sd') or dev.startswith('/dev/vd')
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
