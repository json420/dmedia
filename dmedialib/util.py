# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
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

"""
Misc. utility functions and classes.
"""

from math import log, floor


UNITS_BASE10 = (
    'bytes',
    'kB',
    'MB',
    'GB',
    'TB',
    'PB',
    # For now, we're capping at 999 PB
    #'EB',
    #'ZB',
    #'YB',
)


def units_base10(size):
    """
    Return *size* bytes with 3 significant digits and SI base-10 units.

    For example:

    >>> units_base10(29481537)
    '29.5 MB'
    >>> units_base10(392012353)
    '392 MB'

    For additional details, see:

        https://wiki.ubuntu.com/UnitsPolicy
    """
    if size < 0:
        raise ValueError('size must be greater than zero; got %r' % size)
    if size >= 10 ** 18:
        raise ValueError('size must be smaller than 10**18; got %r' % size)
    if size == 0:
        return '0 bytes'
    if size == 1:
        return '1 byte'
    i = int(floor(log(size, 1000)))
    s = (size / (1000.0 ** i) if i > 0 else size)
    return (
        '%.*g %s' % (3, s, UNITS_BASE10[i])
    )
