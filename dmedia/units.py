# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
Display file sizes according to Ubuntu Units Policy.
"""

import math
from gettext import ngettext


BYTES10 = (
    'bytes',
    'kB',
    'MB',
    'GB',
    'TB',
    'PB',
    'EB',
    'ZB',
    'YB',
)

def bytes10(size):
    """
    Return *size* bytes to 3 significant digits in SI base-10 units.

    For example:

    >>> bytes10(1000)
    '1 kB'
    >>> bytes10(29481537)
    '29.5 MB'
    >>> bytes10(392012353)
    '392 MB'

    For additional details, see:

        https://wiki.ubuntu.com/UnitsPolicy
    """
    if size is None:
        return None
    if size < 0:
        raise ValueError('size must be >= 0; got {!r}'.format(size))
    if size == 0:
        return '0 bytes'
    if size == 1:
        return '1 byte'
    i = min(int(math.floor(math.log(size, 1000))), len(BYTES10) - 1)
    s = (size / (1000 ** i) if i > 0 else size)
    return (
        '{:.3g} {}'.format(s, BYTES10[i])
    )


def minsec(seconds):
    """
    Format *seconds* as a M:SS string with minutes and seconds.
    
    For example:
    
    >>> minsec(123)
    '2:03'
    
    """
    return '{:d}:{:02d}'.format(seconds // 60, seconds % 60)


def file_count(count):
    return ngettext(
        '{count:,d} file',
        '{count:,d} files',
        count
    ).format(count=count)


def count_and_size(count, size):
    return ngettext(
        '{count} file, {size}',
        '{count} files, {size}',
        count
    ).format(count=count, size=bytes10(size))
