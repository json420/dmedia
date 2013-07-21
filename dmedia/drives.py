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

from gi.repository import GUdev
from dbase32 import db32dec, db32enc

from .units import bytes10


udev_client = GUdev.Client.new(['block'])


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

        'filesystem_type': device.get_property('ID_FS_TYPE'),
        'filesystem_uuid': device.get_property('ID_FS_UUID'),
        'filesystem_label': device.get_property('ID_FS_LABEL'),
    }

