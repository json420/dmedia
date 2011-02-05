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
       filesystems (whereas base64-encoding cannot)

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
are available via the ``leaves`` attribute (which is a ``list``):

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



Design Decision: mime-like record types
=======================================

FIXME: This needs to be discussed with the desktopcouch people, but for now I'm
documenting it here to clarify the idea.  --jderose 2011-02-04

The desktopcouch convention is for each document to have a "record_type"
attribute containing the URL of a webpage describing the schema.  For example:

>>> doc = {
...     'record_type': 'http://www.freedesktop.org/wiki/Specifications/desktopcouch/contact',
... }


Although the "record_type" convention addresses an important need (standardizing
CouchDB schema for cross-application use), it has some practical problems that
have been encountered in dmedia:

    1. URLs are too long and awkward if you need to frequently compare the
       record_type with constant values (eg, in map functions, in client-side
       JavaScript, etc)

    2. URLs aren't the greatest long-term stable identifier, and there is a
       tendency for the wiki documentation to lag behind the code

    3. The "record_type" convention is fairly convenient when used from the
       desktopcouch Python API, but gets a bit awkward when used from
       JavaScript or otherwise making direct HTTP requests

So currently dmedia is using something similar in spirit, but a lot simpler in
practice.  For example:

>>> doc = {
...     'type': 'dmedia/file',
... }


The "type" is a namespace ("dmedia"), then a forward slash, then a sub-type
("file"), similar to mime types.  The current dmedia record types include:

    dmedia/file
        Each file has a corresponding CouchDB document, and this is its type

    dmedia/machine
        One for each unique machine (computer/phone/etc) that is a peer in a
        dmedia library

    dmedia/store
        One for each "place" files can be stored - used for both FileStore on
        dmedia peers and for services (like UbuntuOne or Amazon S3)

    dmedia/import
        One is created each time an SD/CF card is imported

    dmedia/batch
        One is created for each batch of imports (imports done in parallel with
        multiple card readers)

For additional information on desktopcouch and record types, see:

    http://www.freedesktop.org/wiki/Specifications/desktopcouch

    https://launchpad.net/desktopcouch



Design Decision: Unix timestamps
================================

All timestamps in the core dmedia schema are ``int`` or ``float`` values
expressing the time in seconds since the epoch, UTC.  This was chosen because:

    1. It avoids the eternal mess of storing times in local-time

    2. All useful comparisons (including deltas) can be quickly done without
       first converting from a calendar representation to Unix time

All dmedia records have a "time" attribute, which is the timestamp of when the
document was first added to CouchDB.  This allows a unified Zeitgeist-style
chronological view across all dmedia records regardless of record type.  For
example:

>>> doc = {
...     '_id': 'MZZG2ZDSOQVSW2TEMVZG643F',
...     'type': 'dmedia/batch',
...     'time': 1234567890,
... }



Design Decision: schema extensibility
=====================================

As the goal is to make dmedia suitable for use by a wide range of applications,
there is a special attribute namespace reserved for application-specific schema.

Attributes starting with "x_" are reserved for extensibility.  The dmedia schema
will never include attributes starting with "x_".  Additionally, the special "x"
attribute is a dictionary that allows groups of related attributes to be placed
under a single extension namespace.  For example:

>>> doc = {
...     '_id': 'GS5HPKZRK7DRXOECOYYXRUTUP26H3ECY',
...     'type': 'dmedia/file',
...     'time': 1234567890,
...     'x': {
...         'pitivi': {
...             'foo': 'PiTiVi-specific foo',
...             'bar': 'PiTiVi-specific bar',
...         },
...     },
...     'x_baz': 'other misc attribute not in dmedia schema',
... }


An important consequence of this extensibility is that when modifying documents,
applications must always losslessly round-trip any attributes they don't know
about.
"""

from __future__ import print_function

import os
from base64 import b32encode, b32decode
from .constants import TYPE_ERROR


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


def isbase32(value, key='_id'):
    if not isinstance(value, basestring):
        raise TypeError(TYPE_ERROR % (key, basestring, type(value), value))
    decoded = b32decode(value)
    if len(decoded) % 5 != 0:
        raise ValueError(
            'len(b32decode(%s)) not multiple of 5: %r' % (key, value)
        )
    return value


def istime(value, key='time'):
    """
    Test that *value* is a Unix timestamp.

    Timestamps must:

        1. be ``int`` or ``float`` instances

        2. be non-negative (must be >= 0)

    For example:

    >>> istime(-3, key='time_end')
    Traceback (most recent call last):
      ...
    ValueError: time_end must be >= 0; got -3

    """
    if not isinstance(value, (int, float)):
        raise TypeError(TYPE_ERROR % (key, (int, float), type(value), value))
    if value < 0:
        raise ValueError(
            '%s must be >= 0; got %r' % (key, value)
        )
    return value


def isdmedia(doc):
    if not isinstance(doc, dict):
        raise TypeError(TYPE_ERROR % ('doc', dict, type(doc), doc))
    required = set(['_id', 'type', 'time'])
    if not required.issubset(doc):
        raise ValueError(
            'doc missing required keys: %r' % sorted(required - set(doc))
        )
    isbase32(doc['_id'])
    istime(doc['time'])
    return doc
