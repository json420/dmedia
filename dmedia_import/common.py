#!/usr/bin/env python

# Authors:
#   David Green <david4dev@gmail.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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

def device_type(base):
    dev = Popen(
        [
            "df",
            base
        ],
        stdout=PIPE
    ).communicate()[0].split(
        "\n"
    )[1].split(
        ' '
    )[0]

    includes = lambda string: dev.find(string) > -1

    if includes('sr'):
        return 'optical'
    if includes('raw1394') or includes('dv1394'):
        return 'firewire'
    if includes('mmcblk'):
        return 'card'
    if includes('sda') or includes('hda'):
        return 'local'

    #default guess
    return 'usb'


def get_icon(device_type):
    if device_type == 'firewire':
        return 'notification-device-firewire'

    #default icon
    return 'notification-device-usb'
