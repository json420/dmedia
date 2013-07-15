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

from gi.repository import GUdev


udev_client = GUdev.Client.new(['block'])


def get_drive_info(name):
    sysfs_path = '/sys/block/{}'.format(name)
    device = udev_client.query_by_sysfs_path(sysfs_path)
    sector_size = device.get_sysfs_attr_as_uint64('queue/hw_sector_size')
    sectors = device.get_sysfs_attr_as_uint64('size')
    return {
        'bytes': sectors * sector_size,
        'model': device.get_property('ID_MODEL'),
        'revision': device.get_property('ID_REVISION'),
        'serial': device.get_property('ID_SERIAL_SHORT'),
        'wwn': device.get_property('ID_WWN_WITH_EXTENSION'),
        'removeable': bool(device.get_sysfs_attr_as_int('removeable')),
    }
