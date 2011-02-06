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
CouchDB schema precisely defined with tests and documentation.

This module contains a number of test functions that precisely define the
conventions of the dmedia CouchDB schema.  These functions are used in the
dmedia test suite, and are available for 3rd-party apps to do the same.

The ``check_foo()`` functions verify that a document (or specific document
attribute) conforms with the schema conventions.  If the value conforms,
``None`` is returned.  If the value does *not* conform, an exception is raised
(typically a ``TypeError`` or ``ValueError``) with a detailed error message
that should allow you to pinpoint the exact problem.

Either way, the ``check_foo()`` functions will never alter the values being
tested.


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

>>> from dmedia.util import random_id
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

from base64 import b32decode
import re
from .constants import TYPE_ERROR


def check_int(value, label):
    if not isinstance(value, int):
        raise TypeError(TYPE_ERROR % (label, int, type(value), value))


def check_atleast(value, minvalue, label):
    if value < minvalue:
        raise ValueError(
            '%s must be >= %r; got %r' % (label, minvalue, value)
        )


def check_lowercase(value, label):
    if not value.islower():
        raise ValueError(
            "%s must be lowercase; got %r" % (label, value)
        )


def check_nonempty(value, label):
    if len(value) == 0:
        raise ValueError('%s cannot be empty' % label)


def check_base32(value, label='_id'):
    """
    Verify that *value* is a valid dmedia document ID.

    Document IDs must:

        1. be ``str`` or ``unicode`` instances

        2. be valid base32 encoding

        3. decode to data that is a multiple of 5-bytes (40-bits ) in length

    For example, a conforming value:

    >>> check_base32('MZZG2ZDSOQVSW2TEMVZG643F')


    And an invalid value:

    >>> check_base32('MFQWCYLBMFQWCYI=')
    Traceback (most recent call last):
      ...
    ValueError: len(b32decode(_id)) not multiple of 5: 'MFQWCYLBMFQWCYI='

    """
    if not isinstance(value, basestring):
        raise TypeError(TYPE_ERROR % (label, basestring, type(value), value))
    try:
        decoded = b32decode(value)
    except TypeError as e:
        raise ValueError('%s: invalid base32: %s; got %r' % (label, e, value))
    if len(decoded) % 5 != 0:
        raise ValueError(
            'len(b32decode(%s)) not multiple of 5: %r' % (label, value)
        )


def check_type(value, label='type'):
    """
    Verify that *doc* has a valid dmedia record type.

    Record types must:

        1. be ``str`` or ``unicode`` instances

        2. be lowercase

        3. start with 'dmedia/'

        4. be of the form 'dmedia/foo', where *foo* is a valid Python identifier

    For example, a conforming value:

    >>> check_type('dmedia/file')


    And an invalid value:

    >>> check_type('dmedia/foo/bar')
    Traceback (most recent call last):
      ...
    ValueError: type must contain only one '/'; got 'dmedia/foo/bar'

    """
    if not isinstance(value, basestring):
        raise TypeError(TYPE_ERROR % (label, basestring, type(value), value))
    check_lowercase(value, label)
    if not value.startswith('dmedia/'):
        raise ValueError(
            "%s must start with 'dmedia/'; got %r" % (label, value)
        )
    parts = value.split('/')
    if len(parts) != 2:
        raise ValueError(
            "%s must contain only one '/'; got %r" % (label, value)
        )


def check_time(value, label='time'):
    """
    Verify that *value* is a Unix timestamp.

    Timestamps must:

        1. be ``int`` or ``float`` instances

        2. be non-negative (must be >= 0)

    For example, a conforming value:

    >>> check_time(1234567890, label='time_end')


    And an invalid value:

    >>> check_time(-1234567890, label='time_end')
    Traceback (most recent call last):
      ...
    ValueError: time_end must be >= 0; got -1234567890

    """
    if not isinstance(value, (int, float)):
        raise TypeError(TYPE_ERROR % (label, (int, float), type(value), value))
    if value < 0:
        raise ValueError(
            '%s must be >= 0; got %r' % (label, value)
        )


def check_required(d, required, label='doc'):
    """
    Check that dictionary *d* contains all the keys in *required*.

    For example, a conforming value:

    >>> check_required(dict(foo=1, bar=2, baz=3), ['foo', 'bar'], 'var_name')


    And an invalid value:

    >>> check_required(dict(foo=1, car=2, baz=3), ['foo', 'bar'], 'var_name')
    Traceback (most recent call last):
      ...
    ValueError: var_name missing keys: ['bar']
    """
    if not isinstance(d, dict):
        raise TypeError(TYPE_ERROR % (label, dict, type(d), d))
    required = frozenset(required)
    if not required.issubset(d):
        missing = sorted(required - set(d))
        raise ValueError(
            '%s missing keys: %r' % (label, missing)
        )


def check_dmedia(doc):
    """
    Verify that *doc* is a valid dmedia document.

    This verifies that *doc* has the common schema requirements that all dmedia
    documents should have.  The *doc* must:

        1. have '_id' that passes `check_base32()`

        2. have 'type' that passes `check_type()`

        3. have 'time' that passes `check_time()`

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ... }
    ...
    >>> check_dmedia(doc)


    And an invalid value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'kind': 'dmedia/file',
    ...     'timestamp': 1234567890,
    ... }
    ...
    >>> check_dmedia(doc)
    Traceback (most recent call last):
      ...
    ValueError: doc missing keys: ['time', 'type']

    """
    check_required(doc, ['_id', 'type', 'time'])
    check_base32(doc['_id'])
    check_type(doc['type'])
    check_time(doc['time'])


def check_stored(stored, label='stored'):
    """
    Verify that *stored* is valid for a 'dmedia/file' record.

    To be valid, *stored* must:

        1. be a non-empty ``dict``

        2. have keys that are document IDs according to `check_base32()`

        3. have values that are themselves ``dict`` instances

        4. values must have 'copies' that is an ``int`` > 0

        5. values must have 'time' that conforms with `check_time()`

    For example, a conforming value:

    >>> stored = {
    ...     'MZZG2ZDSOQVSW2TEMVZG643F': {
    ...         'copies': 2,
    ...         'time': 1234567890,
    ...     },
    ... }
    ...
    >>> check_stored(stored)


    And an invalid value:

    >>> stored = {
    ...     'MZZG2ZDSOQVSW2TEMVZG643F': {
    ...         'number': 2,
    ...         'time': 1234567890,
    ...     },
    ... }
    ...
    >>> check_stored(stored)
    Traceback (most recent call last):
      ...
    ValueError: stored['MZZG2ZDSOQVSW2TEMVZG643F'] missing keys: ['copies']


    Also see `check_dmedia_file()`.
    """

    if not isinstance(stored, dict):
        raise TypeError(TYPE_ERROR % (label, dict, type(stored), stored))
    check_nonempty(stored, label)

    for (key, value) in stored.iteritems():
        check_base32(key, '<key in %s>' % label)

        l2 = '%s[%r]' % (label, key)  # eg "stored['OVRHK3TUOUQCWIDMNFXGC4TP']"

        check_required(value, ['copies', 'time'], l2)

        # Check 'copies':
        copies = value['copies']
        l3 = l2 + "['copies']"
        if not isinstance(copies, int):
            raise TypeError(
                TYPE_ERROR % (l3, int, type(copies), copies)
            )
        if copies < 1:
            raise ValueError('%s must be >= 1; got %r' % (l3, copies))

        # Check 'time':
        check_time(value['time'], l2 + "['time']")


def check_ext(value, label='ext'):
    """
    Verify that *value* is a file extension suitable for 'dmedia/file' records.

    The extension *value* can be ``None``, or otherwise *value* must:

        1. be a non-empty ``str`` or ``unicode`` instance

        2. be lowercase

        3. neither start nor end with a period

        4. contain only letters, numbers, and at most on internal period

    For example, some conforming values:

    >>> check_ext(None)
    >>> check_ext('mov')
    >>> check_ext('tar.gz')


    And an invalid value:

    >>> check_ext('.mov')
    Traceback (most recent call last):
      ...
    ValueError: ext cannot start with a period; got '.mov'

    """
    if value is None:
        return
    if not isinstance(value, basestring):
        raise TypeError(TYPE_ERROR % (label, basestring, type(value), value))
    check_nonempty(value, label)
    check_lowercase(value, label)
    if value.startswith('.'):
        raise ValueError(
            '%s cannot start with a period; got %r' % (label, value)
        )
    if value.endswith('.'):
        raise ValueError(
            '%s cannot end with a period; got %r' % (label, value)
        )
    if not re.match('^[a-z0-9]+(\.[a-z0-9]+)?$', value):
        raise ValueError(
            '%s: only letters, numbers, period allowed; got %r' % (label, value)
        )


def check_dmedia_file(doc):
    """
    Verify that *doc* is a valid 'dmedia/file' record type.

    To be a valid 'dmedia/file' record, *doc* must:

        1. conform with `check_dmedia()`

        2. have 'type' equal to 'dmedia/file'

        3. have 'bytes' that is an ``int`` greater than zero

        4. have 'ext' that conforms with `check_ext()`

        5. have 'stored' that is a ``dict`` conforming with `check_stored()`

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'bytes': 20202333,
    ...     'ext': 'mov',
    ...     'stored': {
    ...         'MZZG2ZDSOQVSW2TEMVZG643F': {
    ...             'copies': 2,
    ...             'time': 1234567890,
    ...         },
    ...     },
    ... }
    ...
    >>> check_dmedia_file(doc)


    And an invalid value:

    >>> doc = {
    ...     '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'bytes': 20202333,
    ...     'ext': 'mov',
    ...     'stored': {
    ...         'MZZG2ZDSOQVSW2TEMVZG643F': {
    ...             'number': 2,  # Changed!
    ...             'time': 1234567890,
    ...         },
    ...     },
    ... }
    ...
    >>> check_dmedia_file(doc)
    Traceback (most recent call last):
      ...
    ValueError: stored['MZZG2ZDSOQVSW2TEMVZG643F'] missing keys: ['copies']

    """
    check_dmedia(doc)
    check_required(doc, ['bytes', 'ext', 'stored'])

    # Check type:
    if doc['type'] != 'dmedia/file':
        raise ValueError(
            "doc['type'] must be 'dmedia/file'; got %(type)r" % doc
        )

    # Check 'bytes':
    b = doc['bytes']
    check_int(b, 'bytes')
    check_atleast(b, 1, 'bytes')

    # Check 'ext':
    check_ext(doc['ext'])

    # Check 'stored'
    check_stored(doc['stored'])


def check_dmedia_store(doc):
    """
    Verify that *doc* is a valid 'dmedia/store' type document.

    To be a valid 'dmedia/store' record, *doc* must:

        1. conform with `check_dmedia()`

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'plugin': 'filestore',
    ...     'default_copies': 2,
    ... }
    ...
    >>> check_dmedia_store(doc)


    And an invalid value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'dispatch': 'filestore',
    ...     'default_copies': 2,
    ... }
    ...
    >>> check_dmedia_store(doc)
    Traceback (most recent call last):
      ...
    ValueError: doc missing keys: ['plugin']

    """
    check_dmedia(doc)
    check_required(doc, ['plugin', 'default_copies'])
