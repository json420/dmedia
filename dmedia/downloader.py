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
Download files in chunks using HTTP Range requests.
"""


def bytes_range(start, stop=None):
    """
    Convert from Python slice semantics to an HTTP Range request.

    Python slice semantics are quite natural to deal with, whereas the HTTP
    Range semantics are a touch wacky, so this function will help prevent silly
    errors.

    For example, say we're requesting parts of a 10,000 byte long file.  This
    requests the first 500 bytes:

    >>> bytes_range(0, 500)
    'bytes=0-499'

    This requests the second 500 bytes:

    >>> bytes_range(500, 1000)
    'bytes=500-999'

    All three of these request the final 500 bytes:

    >>> bytes_range(9500, 10000)
    'bytes=9500-9999'
    >>> bytes_range(-500)
    'bytes=-500'
    >>> bytes_range(9500)
    'bytes=9500-'

    For details on HTTP Range header, see:

      http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    """
    if start < 0:
        assert stop is None
        return 'bytes=%d' % start
    end = ('' if stop is None else stop - 1)
    return 'bytes=%d-%s' % (start, end)


def range_request(i, leaf_size, file_size):
    """
    Request the *i*-th leaf in a tree with *leaf_size* from a file *file_size*.

    The function returns the value for a Range request header.  For example,
    say we have a *leaf_size* of 1024 bytes and a *file_size* of 2311 bytes:

    >>> range_request(0, 1024, 2311)
    'bytes=0-1023'
    >>> range_request(1, 1024, 2311)
    'bytes=1024-2047'
    >>> range_request(2, 1024, 2311)
    'bytes=2048-2310'

    Also see the `bytes_range()` function, which this function uses.

    :param i: The leaf to request (zero-index)
    :param leaf_size: Size of leaf in bytes (min 1024 bytes)
    :param file_size: Size of file in bytes (min 1 byte)
    """
    if i < 0:
        raise ValueError('i must be >=0; got %r' % i)
    if leaf_size < 1024:
        raise ValueError('leaf_size must be >=1024; got %r' % leaf_size)
    if file_size < 1:
        raise ValueError('file_size must be >=1; got %r' % file_size)
    start = i * leaf_size
    if start >= file_size:
        raise ValueError(
            'past end of file: i=%r, leaf_size=%r, file_size=%r' % (
                i, leaf_size, file_size
            )
        )
    stop = min(file_size, (i + 1) * leaf_size)
    return bytes_range(start, stop)
