# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
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
CouchDB schema all defined in one place.

This might move, but for now it's handy to have this all in one file simply to
help the design process.


Design Decision: base32-encoded document IDs
============================================

dmedia utilizes both random (abstract) IDs and intrinsic IDs (derived from file
content-hash).  Both kinds of IDs will always be a multiple of 40-bits (5-bytes)
and will be base32-encoded.  Base32-encoding was chosen because:

    1. It's more compact than base16/hex

    2. It can be used in URLs with no escaping

    3. Importantly, it can be used to name files on case *insensitive*
       filesystems

At its core, dmedia is a simple layered filesystem, so being able to create a
filename directly from a document ID is an important design consideration.


Random IDs
----------

Random IDs are 120-bit random numbers, base32-encoded.  They're much like a
Version 4 (random) UUID, except dmedia random IDs have no reserved bits.  For
example:

>>> from dmedia.schema import random_id
>>> random_id()  #doctest: +SKIP
'NZXXMYLDOV2F6ZTUO5PWM5DX'


Intrinsic IDs
-------------

Files in the dmedia library are uniquely identified by their content-hash.
dmedia *is* a distributed filesystem, but a quite simple one in that it only
stores intrinsically-named, read-only files.

The content-hash is computed as a hash-list (a 1 deep tree-hash).  Currently
this is done using the sha1 hash function with an 8 MiB leaf size, but both the
hash function and leaf size are designed to be configurable to allow for future
migration.

The content-hashes of the individual leaves are stored in the "leaves"
attachment in the CouchDB document.  This allows for file integrity checks with
8 MiB granularity, and provides the basis for cryptographically robust swarm
upload and download.

The base32-encoded sha1 hash is 32-characters long.  For example:

>>> from dmedia.filestore import HashList
>>> from dmedia.tests.helpers import sample_mov  # Sample .MOV file
>>> src_fp = open(sample_mov, 'rb')
>>> hashlist = HashList(src_fp)
>>> hashlist.run()
'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'


After calling `HashList.run()`, the binary digests of the leaf content-hashes
is available via the ``leaves`` attribute (which is a ``list``):

>>> from base64 import b32encode
>>> for d in hashlist.leaves:
...     print(repr(b32encode(d)))
...
'IXJTSUCYYFECGSG6JIB2R77CAJVJK4W3'
'MA3IAHUOKXR4TRG7CWAPOO7U4WCV5WJ4'
'FHF7KDMAGNYOVNYSYT6ZYWQLUOCTUADI'


The overall file content-hash (aka the top-hash) is a hash of the leaf hashes.
Note that this matches what was returned by `HashList.run()`:

>>> from hashlib import sha1
>>> b32encode(sha1(''.join(hashlist.leaves)).digest())
'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O'


In the near future dmedia will very likely migrate to using a 200-bit Skein-512
hash.  See:

    http://packages.python.org/pyskein/



Design Decision: Unix timestamps
================================

All timestamps in the core dmedia schema are ``int`` or ``float`` values
expressing the time in seconds since the epoch, UTC.  This was chosen because:

    1. It avoids the eternal mess of storing times in local-time

    2. All useful comparisons (including deltas) can be quickly done without
       first converting from a calendar representation to Unix time


"""

from __future__ import print_function

import os
from base64 import b32encode


def random_id(random=None):
    """
    Returns a 120-bit base32-encoded random ID.

    The ID will be 24-characters long, URL and filesystem safe.  For example:

    >>> random_id()  #doctest: +SKIP
    'OVRHK3TUOUQCWIDMNFXGC4TP'


    Optionally you can provide the 15-byte random seed yourself:

    >>> random_id(random='abcdefghijklmno'.encode('utf-8'))
    'MFRGGZDFMZTWQ2LKNNWG23TP'


    :param random: optionally provide 15-byte random seed; when not provided,
        seed is created by calling ``os.urandom(15)``
    """
    random = (os.urandom(15) if random is None else random)
    assert len(random) % 5 == 0
    return b32encode(random)
