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
Test-driven definition of dmedia CouchDB schema.

The goal of this module is to:

   1. Unambiguously define the CouchDB schema via a series of test functions

   2. Provide exceedingly helpful error messages when values do not conform
      with the schema

For example:

>>> good = {
...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
...     'ver': 0,
...     'type': 'dmedia/foo',
...     'time': 1234567890,
... }
...
>>> check_dmedia(good)  # Returns None
>>> bad = {
...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
...     'ver': 0,
...     'kind': 'dmedia/foo',
...     'timestamp': 1234567890,
... }
...
>>> check_dmedia(bad)
Traceback (most recent call last):
  ...
ValueError: doc missing keys: ['time', 'type']


These test functions are used in the dmedia test suite, and 3rd-party apps would
be well served by doing the same.  Please read on for the rationale of some key
dmedia schema design decisions...



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
>>> from dmedia.tests import sample_mov  # Sample .MOV file
>>> src_fp = open(sample_mov, 'rb')
>>> hashlist = HashList(src_fp)
>>> hashlist.run()
'TGX33XXWU3EVHEEY5J7NBOJGKBFXLEBK'


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


The current dmedia record types include:

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
...     'ver': 0,
...     'type': 'dmedia/batch',
...     'time': 1234567890,
... }



Design Decision: schema extensibility
=====================================

As the goal is to make dmedia suitable for use by a wide range of applications,
there is a special attribute namespace reserved for application-specific schema.

Attributes starting with ``"x_"`` are reserved for extensibility.  The dmedia
schema will never include attributes starting with ``"x_"``.  Additionally, the
special ``"x"`` attribute is a dictionary that allows groups of related
attributes to be placed under a single extension namespace.  For example:

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


desktopcouch has an ``"application_annotations"`` attribute that is used exactly
the same way as the dmedia ``"x"`` attribute.  Likewise, dmedia will honor the
``"application_annotations"`` convention and never store any of its own schema
under this key.

An important consequence of this extensibility is that when modifying documents,
applications must always losslessly round-trip any attributes they don't know
about.



Design Decision: transparently distributed
==========================================

When a new job is queued, a document like this is created:

>>> doc = {
...     '_id': '2QBD5MP2AADUBCBBVCTTYTLV',
...     'type': 'dmedia/job',
...     'time': 1234567000,
...     'status': 'waiting',
...     'job': {'task': 'transcode', 'id': 'QOVCOHXGV657A4GEBXTJCXJOYB6VM6NB'},
... }


When execution of the job starts, the document is updated like this:

>>> doc = {
...     '_id': '2QBD5MP2AADUBCBBVCTTYTLV',
...     'type': 'dmedia/job',
...     'time': 1234567000,
...     'time_start': 1234568000,
...     'status': 'executing',
...     'job': {'task': 'transcode', 'id': 'QOVCOHXGV657A4GEBXTJCXJOYB6VM6NB'},
... }


When the job is completed, the document is updated like this:

>>> doc = {
...     '_id': '2QBD5MP2AADUBCBBVCTTYTLV',
...     'type': 'dmedia/job',
...     'time': 1234567000,
...     'time_start': 1234568000,
...     'time_end': 1234569000,
...     'status': 'complete',
...     'job': {'task': 'transcode', 'id': 'QOVCOHXGV657A4GEBXTJCXJOYB6VM6NB'},
...     'result': {'id': 'AQUUT2Y2PY2DRE5NJH5K43HIHQXSMRFL'},
... }

"""

from __future__ import print_function

import os
from hashlib import sha1
from base64 import b32encode, b32decode, b64encode
import re
import time

from .constants import TYPE_ERROR, EXT_PAT

# Some private helper functions that don't directly define any schema.
#
# If this seems unnecessary or even a bit un-Pythonic (where's my duck typing?),
# keep in mind that the goal of this module is to:
#
#   1. Unambiguously define the schema
#
#   2. Provide exceedingly helpful error messages when values do not conform
#      with the schema
#
# That is all.


# FIXME: These 6 functions are a step toward making the checks more concise and
# the error messages consistent and even more helpful.
def _label(path):
    """
    Create a helpful debugging label to indicate the attribute in question.

    For example:

    >>> _label([])
    'doc'
    >>> _label(['log'])
    "doc['log']"
    >>> _label(['log', 'considered', 2, 'src'])
    "doc['log']['considered'][2]['src']"


    See also `_value()`.
    """
    return 'doc' + ''.join('[{!r}]'.format(key) for key in path)


def _value(doc, path):
    """
    Retrieve value from *doc* by traversing *path*.

    For example:

    >>> doc = {'log': {'considered': [None, None, {'src': 'hello'}, None]}}
    >>> _value(doc, [])
    {'log': {'considered': [None, None, {'src': 'hello'}, None]}}
    >>> _value(doc, ['log'])
    {'considered': [None, None, {'src': 'hello'}, None]}
    >>> _value(doc, ['log', 'considered', 2, 'src'])
    'hello'


    Or if you try to retrieve something that doesn't exist:

    >>> _value(doc, ['log', 'considered', 7])
    Traceback (most recent call last):
      ...
    ValueError: doc['log']['considered'][7] does not exists


    Or if a key/index is missing higher up in the path:

    >>> _value(doc, ['dog', 'considered', 7])
    Traceback (most recent call last):
      ...
    ValueError: doc['dog'] does not exists


    See also `_label()`.
    """
    value = doc
    p = []
    for key in path:
        p.append(key)
        try:
            value = value[key]
        except (KeyError, IndexError):
            raise ValueError(
                '{} does not exists'.format(_label(p))
            )
    return value


def _exists(doc, path):
    """
    Return ``True`` if the end of *path* exists.

    For example:

    >>> doc = {'foo': {'hello': 'world'}, 'bar': ['hello', 'naughty', 'nurse']}
    >>> _exists(doc, ['foo', 'hello'])
    True
    >>> _exists(doc, ['foo', 'sup'])
    False
    >>> _exists(doc, ['bar', 2])
    True
    >>> _exists(doc, ['bar', 3])
    False


    Or if a key/index is missing higher up the path:

    >>> _exists(doc, ['stuff', 'junk'])
    Traceback (most recent call last):
      ...
    ValueError: doc['stuff'] does not exists


    See also `_check_if_exists()`.
    """
    if len(path) == 0:
        return True
    base = _value(doc, path[:-1])
    key = path[-1]
    try:
        value = base[key]
        return True
    except (KeyError, IndexError):
        return False


def _check(doc, path, *checks):
    value = _value(doc, path)
    label = _label(path)
    for c in checks:
        if isinstance(c, tuple):
            (c, args) = (c[0], c[1:])
        else:
            args = tuple()
        if c(value, label, *args) is True:
            break


def _check_if_exists(doc, path, *checks):
    """
    Run *checks* only if value at *path* exists.

    For example:

    >>> doc = {'name': 17}
    >>> _check_if_exists(doc, ['dir'], _check_str)
    >>> _check_if_exists(doc, ['name'], _check_str)
    Traceback (most recent call last):
      ...
    TypeError: doc['name']: need a <type 'basestring'>; got a <type 'int'>: 17


    See also `_exists()` and `_check()`.
    """
    if _exists(doc, path):
        _check(doc, path, *checks)


def _can_be_none(value, label):
    """
    Stop execution of check if *value* is ``None``.

    `_check()` will abort upon a check function returning ``True``.

    For example, here a ``TypeError`` is raised:

    >>> doc = {'ext': None}
    >>> _check(doc, ['ext'], _check_str)
    Traceback (most recent call last):
      ...
    TypeError: doc['ext']: need a <type 'basestring'>; got a <type 'NoneType'>: None


    But here it is not:

    >>> _check(doc, ['ext'], _can_be_none, _check_str)

    """
    if value is None:
        return True

# /FIXME


def _check_dict(value, label):
    """
    Verify that *value* is a ``dict`` instance.

    For example:

    >>> _check_dict(['foo', 'bar'], 'doc')
    Traceback (most recent call last):
      ...
    TypeError: doc: need a <type 'dict'>; got a <type 'list'>: ['foo', 'bar']

    """
    if not isinstance(value, dict):
        raise TypeError(TYPE_ERROR % (label, dict, type(value), value))

def _check_str(value, label):
    """
    Verify that *value* is a ``basestring`` instance.

    Or a ``str`` instance one dmedia is running under Python3.

    For example:

    >>> _check_str(17, 'import_id')
    Traceback (most recent call last):
      ...
    TypeError: import_id: need a <type 'basestring'>; got a <type 'int'>: 17

    """
    if not isinstance(value, basestring):
        raise TypeError(TYPE_ERROR % (label, basestring, type(value), value))

def _check_int(value, label):
    """
    Verify that *value* is an ``int`` instance.

    For example:

    >>> _check_int(18.0, 'bytes')
    Traceback (most recent call last):
      ...
    TypeError: bytes: need a <type 'int'>; got a <type 'float'>: 18.0

    """
    if not isinstance(value, int):
        raise TypeError(TYPE_ERROR % (label, int, type(value), value))

def _check_int_float(value, label):
    """
    Verify that *value* is an ``int`` or ``float`` instance.

    For example:

    >>> _check_int_float('18', 'time')
    Traceback (most recent call last):
      ...
    TypeError: time: need a (<type 'int'>, <type 'float'>); got a <type 'str'>: '18'

    """
    if not isinstance(value, (int, float)):
        raise TypeError(TYPE_ERROR % (label, (int, float), type(value), value))

def _check_at_least(value, minvalue, label):
    """
    Verify that *value* is greater than or equal to *minvalue*.

    For example:

    >>> _check_at_least(0, 1, 'bytes')
    Traceback (most recent call last):
      ...
    ValueError: bytes must be >= 1; got 0

    """
    if value < minvalue:
        raise ValueError(
            '%s must be >= %r; got %r' % (label, minvalue, value)
        )

def _check_lowercase(value, label):
    """
    Verify that *value* is lowercase.

    For example:

    >>> _check_lowercase('MOV', 'ext')
    Traceback (most recent call last):
      ...
    ValueError: ext must be lowercase; got 'MOV'

    """
    if not value.islower():
        raise ValueError(
            "%s must be lowercase; got %r" % (label, value)
        )

def _check_identifier(value, label):
    """
    Verify that *value* is a lowercase Python identifier not starting with "_"

    For example:

    >>> _check_identifier('hello_world', 'msg')
    >>> _check_identifier('hello-world', 'msg')
    Traceback (most recent call last):
      ...
    ValueError: msg: 'hello-world' does not match '^[a-z][_a-z0-9]*$'

    """
    pat = '^[a-z][_a-z0-9]*$'
    if not re.match(pat, value):
        raise ValueError(
            '%s: %r does not match %r' % (label, value, pat)
        )

def _check_nonempty(value, label):
    """
    Verify that *value* is not empty (ie len() > 0).

    For example:

    >>> _check_nonempty({}, 'stored')
    Traceback (most recent call last):
      ...
    ValueError: stored cannot be empty; got {}

    """
    if len(value) == 0:
        raise ValueError('%s cannot be empty; got %r' % (label, value))

def _check_required(d, required, label='doc'):
    """
    Check that dictionary *d* contains all the keys in *required*.

    For example:

    >>> _check_required(dict(foo=1, bar=2, baz=3), ['foo', 'bar'], 'var_name')
    >>> _check_required(dict(foo=1, car=2, baz=3), ['foo', 'bar'], 'var_name')
    Traceback (most recent call last):
      ...
    ValueError: var_name missing keys: ['bar']

    """
    _check_dict(d, label)
    required = frozenset(required)
    if not required.issubset(d):
        missing = sorted(required - set(d))
        raise ValueError(
            '%s missing keys: %r' % (label, missing)
        )


# The schema defining functions:

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
    _check_str(value, label)
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
    _check_str(value, label)
    _check_lowercase(value, label)
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
    _check_int_float(value, label)
    _check_at_least(value, 0, label)


def check_dmedia(doc):
    """
    Verify that *doc* is a valid dmedia document.

    This verifies that *doc* has the common schema requirements that all dmedia
    documents should have.  The *doc* must:

        1. have '_id' that passes `check_base32()`

        2. have a 'ver' equal to ``0``

        3. have 'type' that passes `check_type()`

        4. have 'time' that passes `check_time()`

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ... }
    ...
    >>> check_dmedia(doc)


    And an invalid value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'kind': 'dmedia/file',
    ...     'timestamp': 1234567890,
    ... }
    ...
    >>> check_dmedia(doc)
    Traceback (most recent call last):
      ...
    ValueError: doc missing keys: ['time', 'type']

    """
    _check_required(doc, ['_id', 'ver', 'type', 'time'])
    check_base32(doc['_id'])
    _check_int(doc['ver'], 'ver')
    if doc['ver'] != 0:
        raise ValueError(
            "doc['ver'] must be 0; got {!r}".format(doc['ver'])
        )
    check_type(doc['type'])
    check_time(doc['time'])


def check_stored(stored, label='stored'):
    """
    Verify that *stored* is valid for a 'dmedia/file' record.

    To be valid, *stored* must:

        1. be a non-empty ``dict``

        2. have keys that are document IDs according to `check_base32()`

        3. have values that are themselves ``dict`` instances

        4. values must have 'copies' that is an ``int`` >= 0

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

    _check_dict(stored, label)
    _check_nonempty(stored, label)

    for (key, value) in stored.iteritems():
        check_base32(key, '<key in %s>' % label)

        l2 = '%s[%r]' % (label, key)  # eg "stored['OVRHK3TUOUQCWIDMNFXGC4TP']"

        _check_required(value, ['copies', 'time'], l2)

        # Check 'copies':
        copies = value['copies']
        l3 = l2 + "['copies']"
        _check_int(copies, l3)
        _check_at_least(copies, 0, l3)

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
    _check_str(value, label)
    _check_nonempty(value, label)
    _check_lowercase(value, label)
    if value.startswith('.'):
        raise ValueError(
            '%s cannot start with a period; got %r' % (label, value)
        )
    if value.endswith('.'):
        raise ValueError(
            '%s cannot end with a period; got %r' % (label, value)
        )
    if not re.match(EXT_PAT, value):
        raise ValueError(
            '%s: %r does not match %r' % (label, value, EXT_PAT)
        )


def check_origin(value, label='origin', strict=False):
    """
    Verify that *value* is an 'origin' suitable for 'dmedia/file' records.

    To be a valid origin, *value* must:

        1. be a non-empty ``str`` or ``unicode`` instance

        2. be lowercase

        3. be a valid Python identifier not starting with "_"

        4. if called with strict=True, must be either 'user', 'download',
           'paid', 'proxy', 'cache', or 'render'

    For example, some conforming values:

    >>> check_origin('hello_world2')
    >>> check_origin('user')


    And an invalid value:

    >>> check_origin('User')
    Traceback (most recent call last):
      ...
    ValueError: origin must be lowercase; got 'User'

    """
    _check_str(value, label)
    _check_nonempty(value, label)
    _check_lowercase(value, label)
    _check_identifier(value, label)
    if not strict:
        return
    allowed = ['user', 'download', 'paid', 'proxy', 'cache', 'render']
    if value not in allowed:
        raise ValueError('%s: %r not in %r' % (label, value, allowed))


def check_dmedia_file(doc):
    """
    Verify that *doc* is a valid 'dmedia/file' record type.

    To be a valid 'dmedia/file' record, *doc* must:

        1. conform with `check_dmedia()`

        2. have 'type' equal to 'dmedia/file'

        3. have 'bytes' that is an ``int`` >= 1

        4. have 'ext' that conforms with `check_ext()`

        5. have 'origin' that conforms with `check_origin()` with strict=True

        6. have 'stored' that is a ``dict`` conforming with `check_stored()`

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'bytes': 20202333,
    ...     'ext': 'mov',
    ...     'origin': 'user',
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
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'bytes': 20202333,
    ...     'ext': 'mov',
    ...     'origin': 'user',
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
    _check_required(doc, ['bytes', 'ext', 'origin', 'stored'])

    # Check type:
    if doc['type'] != 'dmedia/file':
        raise ValueError(
            "doc['type'] must be 'dmedia/file'; got %(type)r" % doc
        )

    # Check 'bytes':
    b = doc['bytes']
    _check_int(b, 'bytes')
    _check_at_least(b, 1, 'bytes')

    # Check 'ext':
    check_ext(doc['ext'])

    # Check 'origin':
    check_origin(doc['origin'], strict=True)

    # Check 'stored'
    check_stored(doc['stored'])

    check_dmedia_file_optional(doc)


def check_dmedia_file_optional(doc):
    """
    Check the optional attributes in a 'dmedia/file' document.
    """
    _check_dict(doc, 'doc')

    # mime like 'video/quicktime'
    if doc.get('mime') is not None:
        mime = doc['mime']
        _check_str(mime, 'mime')

    # media like 'video'
    if doc.get('media') is not None:
        media = doc['media']
        _check_str(media, 'media')

    # mtime like 1234567890
    if 'mtime' in doc:
        check_time(doc['mtime'], 'mtime')

    # atime like 1234567890
    if 'atime' in doc:
        check_time(doc['atime'], 'atime')

    # name like 'MVI_5899.MOV'
    if 'name' in doc:
        _check_str(doc['name'], 'name')

    # dir like 'DCIM/100EOS5D2'
    if 'dir' in doc:
        _check_str(doc['dir'], 'dir')

    # 'meta' like {'iso': 800}
    if 'meta' in doc:
        _check_dict(doc['meta'], 'meta')

    # 'user' like {'title': 'cool sunset'}
    if 'user' in doc:
        _check_dict(doc['user'], 'user')

    # 'tags' like {'uds-n': {'start': 3, 'end': 17}}
    if 'tags' in doc:
        _check_dict(doc['tags'], 'tags')


def check_dmedia_store(doc):
    """
    Verify that *doc* is a valid 'dmedia/store' type document.

    To be a valid 'dmedia/store' record, *doc* must:

        1. conform with `check_dmedia()`

        2. have 'plugin' that equal to 'filestore', 'removable_filestore',
           'ubuntuone', or 's3'

        3. have 'copies' that is an ``int`` >= 1

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'plugin': 'filestore',
    ...     'copies': 2,
    ... }
    ...
    >>> check_dmedia_store(doc)


    And an invalid value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'dispatch': 'filestore',
    ...     'copies': 2,
    ... }
    ...
    >>> check_dmedia_store(doc)
    Traceback (most recent call last):
      ...
    ValueError: doc missing keys: ['plugin']

    """
    check_dmedia(doc)
    _check_required(doc, ['plugin', 'copies'])

    # Test plugin
    key = 'plugin'
    p = doc[key]
    _check_str(p, key)
    plugins = ['filestore', 'removable_filestore', 'ubuntuone', 's3']
    if p not in plugins:
        raise ValueError(
            '%s %r not in %r' % (key, p, plugins)
        )

    # Test copies
    key = 'copies'
    dc = doc[key]
    _check_int(dc, key)
    _check_at_least(dc, 1, key)


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


# FIXME: There is current a recursize import issue with filestore, but FileStore
# shouldn't deal with the store.json file anyway, should not import
# `schema.create_store()`
def tophash_personalization(file_size):
    return ' '.join(['dmedia/tophash', str(file_size)]).encode('utf-8')


def tophash(file_size, leaves):
    """
    Initialize hash for a file that is *file_size* bytes.
    """
    h = sha1(tophash_personalization(file_size))
    h.update(leaves)
    return b32encode(h.digest())


def create_file(file_size, leaves, store, copies=0, ext=None, origin='user'):
    """
    Create a minimal 'dmedia/file' document.

    :param file_size: an ``int``, the file size in bytes, eg ``20202333``
    :param leaves: a ``list`` containing the content hash of each leaf
    :param store: the ID of the store where this file is initially stored, eg
        ``'Y4J3WQCMKV5GHATOCZZBHF4Y'``
    :param copies: an ``int`` to represent the durability of the file on this
        store; default is ``0``
    :param ext: the file extension, eg ``'mov'``; default is ``None``
    :param origin: the file's origin (for durability/reclamation purposes);
        default is ``'user'``
    """
    ts = time.time()
    packed = b''.join(leaves)
    return {
        '_id': tophash(file_size, packed),
        '_attachments': {
            'leaves': {
                'data': b64encode(packed),
                'content_type': 'application/octet-stream',
            }
        },
        'ver': 0,
        'type': 'dmedia/file',
        'time': ts,
        'bytes': file_size,
        'ext': ext,
        'origin': origin,
        'stored': {
            store: {
                'copies': copies,
                'time': ts,
            }
        }
    }


def create_store(base, machine_id, copies=1):
    """
    Create a 'dmedia/store' document.
    """
    return {
        '_id': random_id(),
        'ver': 0,
        'type': 'dmedia/store',
        'time': time.time(),
        'plugin': 'filestore',
        'copies': copies,
        'path': base,
        'machine_id': machine_id,
    }


def create_batch(machine_id=None):
    """
    Create initial 'dmedia/batch' accounting document.
    """
    return {
        '_id': random_id(),
        'ver': 0,
        'type': 'dmedia/batch',
        'time': time.time(),
        'machine_id': machine_id,
        'imports': [],
        'errors': [],
        'stats': {
            'considered': {'count': 0, 'bytes': 0},
            'imported': {'count': 0, 'bytes': 0},
            'skipped': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
            'error': {'count': 0, 'bytes': 0},
        }
    }


def create_import(base, batch_id=None, machine_id=None):
    """
    Create initial 'dmedia/import' accounting document.
    """
    return {
        '_id': random_id(),
        'ver': 0,
        'type': 'dmedia/import',
        'time': time.time(),
        'batch_id': batch_id,
        'machine_id': machine_id,
        'base': base,
        'log': {
            'imported': [],
            'skipped': [],
            'empty': [],
            'error': [],
        },
        'stats': {
            'imported': {'count': 0, 'bytes': 0},
            'skipped': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
            'error': {'count': 0, 'bytes': 0},
        }
    }
