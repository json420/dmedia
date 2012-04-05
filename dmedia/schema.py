# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
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
...     'kind': 'dmedia/foo',  # Changed!
...     'time': 1234567890,
... }
...
>>> check_dmedia(bad)
Traceback (most recent call last):
  ...
ValueError: doc['type'] does not exist


These test functions are used in the dmedia test suite, and 3rd-party apps would
be well served by doing the same.



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

from base64 import b32encode, b64encode
import re
import time
import socket

from filestore import DIGEST_B32LEN, B32ALPHABET, TYPE_ERROR
from microfiber import random_id, RANDOM_B32LEN


# schema-compatibility version:
VER = 0

# versioned primary database name:
DB_NAME = 'dmedia-{}'.format(VER)

# Pattern that doc['ext'] must match for dmedia/file
EXT_PAT = '^[a-z0-9]+(\.[a-z0-9]+)?$'


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
    ValueError: doc['log']['considered'][7] does not exist

    Or if a key/index is missing higher up in the path:

    >>> _value(doc, ['dog', 'considered', 7])
    Traceback (most recent call last):
      ...
    ValueError: doc['dog'] does not exist

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
                '{} does not exist'.format(_label(p))
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
    ValueError: doc['stuff'] does not exist

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


def _check(doc, path, allowed, *checks):
    """
    Run a series of *checks* on the value in *doc* addressed by *path*.

    For example:

    >>> doc = {'foo': [None, {'bar': 'aye'}, None]}
    >>> _check(doc, ['foo', 1, 'bar'], str,
    ...     (_is_in, 'bee', 'sea'),
    ... )
    ...
    Traceback (most recent call last):
      ...
    ValueError: doc['foo'][1]['bar'] value 'aye' not in ('bee', 'sea')

    Or if a value is missing:

    >>> _check(doc, ['foo', 3], str,
    ...     (_equals, 'hello'),
    ... )
    ...
    Traceback (most recent call last):
      ...
    ValueError: doc['foo'][3] does not exist

    See also `_check_if_exists()`.
    """
    value = _value(doc, path)
    label = _label(path)
    if not (allowed is None or isinstance(value, allowed)):
        raise TypeError(TYPE_ERROR.format(label, allowed, type(value), value))
    if value is None:
        return
    for c in checks:
        if isinstance(c, tuple):
            (c, args) = (c[0], c[1:])
        else:
            args = tuple()
        c(value, label, *args)


def _check_if_exists(doc, path, allowed, *checks):
    """
    Run *checks* only if value at *path* exists.

    For example:

    >>> doc = {'name': 17}
    >>> _check_if_exists(doc, ['dir'], str)
    >>> _check_if_exists(doc, ['name'], str)
    Traceback (most recent call last):
      ...
    TypeError: doc['name']: need a <class 'str'>; got a <class 'int'>: 17


    See also `_check()` and `_exists()`.
    """
    if _exists(doc, path):
        _check(doc, path, allowed, *checks)


def _at_least(value, label, minvalue):
    """
    Verify that *value* is greater than or equal to *minvalue*.

    For example:

    >>> _at_least(0, "doc['bytes']", 1)
    Traceback (most recent call last):
      ...
    ValueError: doc['bytes'] must be >= 1; got 0

    """
    if value < minvalue:
        raise ValueError(
            '%s must be >= %r; got %r' % (label, minvalue, value)
        )


def _lowercase(value, label):
    """
    Verify that *value* is lowercase.

    For example:

    >>> _lowercase('MOV', "doc['ext']")
    Traceback (most recent call last):
      ...
    ValueError: doc['ext'] must be lowercase; got 'MOV'

    """
    if not value.islower():
        raise ValueError(
            "{} must be lowercase; got {!r}".format(label, value)
        )


def _matches(value, label, pattern):
    """
    Verify that *value* matches regex *pattern*.

    For example:

    >>> _matches('hello_world', "doc['plugin']", '^[a-z][_a-z0-9]*$')
    >>> _matches('hello-world', "doc['plugin']", '^[a-z][_a-z0-9]*$')
    Traceback (most recent call last):
      ...
    ValueError: doc['plugin']: 'hello-world' does not match '^[a-z][_a-z0-9]*$'

    """
    if not re.match(pattern, value):
        raise ValueError(
            '{}: {!r} does not match {!r}'.format(label, value, pattern)
        )


def _nonempty(value, label):
    """
    Verify that *value* is not empty (ie len() > 0).

    For example:

    >>> _nonempty({}, 'stored')
    Traceback (most recent call last):
      ...
    ValueError: stored cannot be empty; got {}

    """
    if len(value) == 0:
        raise ValueError('%s cannot be empty; got %r' % (label, value))


def _is_in(value, label, *possible):
    """
    Check that *value* is one of *possible*.

    For example:

    >>> _is_in('foo', "doc['media']", 'video', 'audio', 'image')
    Traceback (most recent call last):
      ...
    ValueError: doc['media'] value 'foo' not in ('video', 'audio', 'image')

    """
    if value not in possible:
        raise ValueError(
            '{} value {!r} not in {!r}'.format(label, value, possible)
        )


def _equals(value, label, expected):
    """
    Check that *value* equals *expected*.

    For example:

    >>> _equals('file', "doc['type']", 'dmedia/file')
    Traceback (most recent call last):
      ...
    ValueError: doc['type'] must equal 'dmedia/file'; got 'file'

    """
    if value != expected:
        raise ValueError(
            '{} must equal {!r}; got {!r}'.format(label, expected, value)
        )


def _any_id(value, label):
    """
    Verify that *value* is a base32-encoded ID.
    """
    if not isinstance(value, str):
        raise TypeError(
            TYPE_ERROR.format(label, str, type(value), value)
        )
    if len(value) % 8 != 0:
        raise ValueError(
            '{}: length of ID ({}) not multiple of 8: {!r}'.format(
                    label, len(value), value)
        )
    if not set(value).issubset(B32ALPHABET):
        raise ValueError(
            '{}: ID not subset of B32ALPHABET: {!r}'.format(
                    label, value)
        )


def _random_id(value, label):
    """
    Verify that *value* is a 120-bit base32-encoded random ID.

    For example, the number ``'1'`` is not a valid base32 character:

    >>> _random_id('1OTXJHVEXTKNXZHCMHDVF276', "doc['_id']")
    Traceback (most recent call last):
      ...
    ValueError: doc['_id']: random ID not subset of B32ALPHABET: '1OTXJHVEXTKNXZHCMHDVF276'

    """
    if not isinstance(value, str):
        raise TypeError(
            TYPE_ERROR.format(label, str, type(value), value)
        )
    if len(value) != RANDOM_B32LEN:
        raise ValueError(
            '{}: random ID must be {} characters, got {}: {!r}'.format(
                    label, RANDOM_B32LEN, len(value), value)
        )
    if not set(value).issubset(B32ALPHABET):
        raise ValueError(
            '{}: random ID not subset of B32ALPHABET: {!r}'.format(
                    label, value)
        )


def _intrinsic_id(value, label):
    """
    Verify that *value* is a 240-bit base32-encoded intrinsic ID.

    For example:

    >>> _intrinsic_id('QE7POGENSF67FGKN2TD3FH4E', "doc['_id']")
    Traceback (most recent call last):
      ...
    ValueError: doc['_id']: intrinsic ID must be 48 characters, got 24: 'QE7POGENSF67FGKN2TD3FH4E'

    """
    if not isinstance(value, str):
        raise TypeError(
            TYPE_ERROR.format(label, str, type(value), value)
        )
    if len(value) != DIGEST_B32LEN:
        raise ValueError(
            '{}: intrinsic ID must be {} characters, got {}: {!r}'.format(
                    label, DIGEST_B32LEN, len(value), value)
        )
    if not set(value).issubset(B32ALPHABET):
        raise ValueError(
            '{}: intrinsic ID not subset of B32ALPHABET: {!r}'.format(
                    label, value)
        )


##################################
# The schema validation functions:

def check_dmedia(doc):
    """
    Verify that *doc* is a valid dmedia document.

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'type': 'dmedia/foo',
    ...     'time': 1234567890,
    ... }
    ...
    >>> check_dmedia(doc)

    """
    _check(doc, [], dict)
    _check(doc, ['_id'], str,
        _any_id,
    )
    _check(doc, ['ver'], int,
        (_equals, VER),
    )
    _check(doc, ['type'], str,
        (_matches, 'dmedia/[a-z]+$'),
    )
    _check(doc, ['time'], (int, float),
        (_at_least, 0),
    )


def check_file(doc):
    """
    Verify that *doc* is a valid "dmedia/file" document.

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'ROHNRBKS6T4YETP5JHEGQ3OLSBDBWRCKR2BKILJOA3CP7QZW',
    ...     '_attachments': {
    ...         'leaf_hashes': {
    ...             'data': 'v7t381LIyKsBCUYhkGreXx2qKTyyMfMD2eHWWp/L',
    ...             'content_type': 'application/octet-stream',
    ...         },
    ...     },
    ...     'ver': 0,
    ...     'type': 'dmedia/file',
    ...     'time': 1234567890,
    ...     'atime': 1318399431,
    ...     'bytes': 20202333,
    ...     'origin': 'user',
    ...     'stored': {
    ...         'MZZG2ZDSOQVSW2TEMVZG643F': {
    ...             'copies': 2,
    ...             'mtime': 1234567890,
    ...             'plugin': 'filestore',
    ...         },
    ...     },
    ...     'partial': {},
    ...     'corrupt': {},
    ... }
    ...
    >>> check_file(doc)

    """
    check_dmedia(doc)
    _check(doc, ['_id'], str,
        _intrinsic_id,
    )
    _check(doc, ['type'], str,
        (_equals, 'dmedia/file'),
    )

    _check(doc, ['_attachments', 'leaf_hashes'], dict,
        _nonempty,
    )
    _check(doc, ['_attachments', 'leaf_hashes', 'content_type'], str,
        (_equals, 'application/octet-stream'),
    )
    _check(doc, ['bytes'], int,
        (_at_least, 1),
    )
    _check(doc, ['origin'], str,
        _lowercase,
        (_is_in, 'user', 'paid', 'download', 'proxy', 'render', 'cache'),
    )
    _check(doc, ['atime'], (int, float),
        (_at_least, 0),
    )

    _check(doc, ['stored'], dict)
    for key in doc['stored']:
        _check(doc, ['stored', key], dict)
        _check(doc, ['stored', key, 'copies'], int,
            (_at_least, 0),
        )
        _check(doc, ['stored', key, 'mtime'], (int, float),
            (_at_least, 0),
        )
        _check_if_exists(doc, ['stored', key, 'verified'], (int, float),
            (_at_least, 0),
        )

    _check(doc, ['partial'], dict)
    for key in doc['partial']:
        _check(doc, ['partial', key], dict)
        _check(doc, ['partial', store, 'time'], (int, float),
            (_at_least, 0),
        )
        _check(doc, ['partial', store, 'mtime'], (int, float),
            (_at_least, 0),
        )

    _check(doc, ['corrupt'], dict)
    for key in doc['corrupt']:
        _check(doc, ['corrupt', key], dict)
        _check(doc, ['corrupt', store, 'time'], (int, float),
            (_at_least, 0),
        )
        _check(doc, ['corrupt', store, 'mtime'], (int, float),
            (_at_least, 0),
        )

    # 'ext' like 'mov'
    _check_if_exists(doc, ['ext'], str,
        (_matches, EXT_PAT),
    )

    # 'content_type' like 'video/quicktime'
    _check_if_exists(doc, ['content_type'], str)

    # proxy_of
    if doc['origin'] == 'proxy':
        _check(doc, ['proxy_of'], str,
            _intrinsic_id,
        )

    check_file_optional(doc)


def check_file_optional(doc):

    # 'content_encoding' like 'gzip'
    _check_if_exists(doc, ['content_encoding'], str,
        (_is_in, 'gzip', 'deflate'),
    )

    # 'media' like 'video'
    _check_if_exists(doc, ['media'], str,
        (_is_in, 'video', 'audio', 'image'),
    )

    # 'ctime' like 1234567890
    _check_if_exists(doc, ['ctime'], (int, float),
        (_at_least, 0),
    )

    # name like 'MVI_5899.MOV'
    _check_if_exists(doc, ['name'], str)

    # dir like 'DCIM/100EOS5D2'
    # FIXME: Should save this as a list so path is portable
    _check_if_exists(doc, ['dir'], str)

    # 'meta' like {'iso': 800}
    _check_if_exists(doc, ['meta'], dict)

    # 'user' like {'title': 'cool sunset'}
    _check_if_exists(doc, ['user'], dict)

    # 'tags' like {'burp': {'start': 6, 'end': 73}}
    _check_if_exists(doc, ['tags'], dict)


def check_store(doc):
    """
    Verify that *doc* is a valid "dmedia/store" document.

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'NZXXMYLDOV2F6ZTUO5PWM5DX',
    ...     'ver': 0,
    ...     'type': 'dmedia/store',
    ...     'time': 1234567890,
    ...     'plugin': 'filestore',
    ...     'copies': 1,
    ... }
    ...
    >>> check_store(doc)

    """
    check_dmedia(doc)
    _check(doc, ['_id'], str,
        _random_id,
    )
    _check(doc, ['type'], str,
        (_equals, 'dmedia/store'),
    )
    _check(doc, ['plugin'], str,
        (_is_in, 'filestore', 'ubuntuone', 's3'),
    )
    _check(doc, ['copies'], int,
        (_at_least, 0),
    )


#######################################################
# Functions for creating specific types of dmedia docs:

def create_file(_id, file_size, leaf_hashes, stored, origin='user'):
    """
    Create a minimal 'dmedia/file' document.
    """
    timestamp = time.time()
    return {
        '_id': _id,
        '_attachments': {
            'leaf_hashes': {
                'data': b64encode(leaf_hashes).decode('utf-8'),
                'content_type': 'application/octet-stream',
            }
        },
        'ver': VER,
        'type': 'dmedia/file',
        'time': timestamp,
        'atime': timestamp,
        'bytes': file_size,
        'origin': origin,
        'stored': stored,
        'partial': {},
        'corrupt': {},
    }


def create_project_file(_id, file_size, leaf_hashes, origin='user'):
    """
    Create a minimal 'dmedia/file' document.
    """
    return {
        '_id': _id,
        '_attachments': {
            'leaf_hashes': {
                'data': b64encode(leaf_hashes).decode('utf-8'),
                'content_type': 'application/octet-stream',
            }
        },
        'ver': VER,
        'type': 'dmedia/file',
        'time': time.time(),
        'bytes': file_size,
        'origin': origin,
        'tags': {},
    }


def create_machine():
    """
    Create a 'dmedia/machine' document.
    """
    return {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/machine',
        'time': time.time(),
        'hostname': socket.gethostname(),
    }


def create_filestore(copies=1):
    """
    Create a 'dmedia/store' doc for a FileStore.
    """
    return {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/store',
        'time': time.time(),
        'plugin': 'filestore',
        'copies': copies,
    }


def create_s3_store(bucket, copies=2, use_ext=True):
    """
    Create a 'dmedia/store' document.
    """
    return {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/store',
        'time': time.time(),
        'plugin': 's3',
        'bucket': bucket,
        'copies': copies,
        'use_ext': use_ext,
    }


def create_batch(machine_id=None):
    """
    Create initial 'dmedia/batch' accounting document.
    """
    return {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/batch',
        'time': time.time(),
        'machine_id': machine_id,
        'imports': {},
        'stats': {
            'total': {'count': 0, 'bytes': 0},
            'new': {'count': 0, 'bytes': 0},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
        },
    }


def create_import(basedir, machine_id, **kw):
    """
    Create initial 'dmedia/import' accounting document.
    """
    doc = {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/import',
        'time': time.time(),
        'basedir': basedir,
        'machine_id': machine_id,
        'files': {},
        'stats': {
            'total': {'count': 0, 'bytes': 0},
            'new': {'count': 0, 'bytes': 0},
            'duplicate': {'count': 0, 'bytes': 0},
            'empty': {'count': 0, 'bytes': 0},
        },
    }
    doc.update(kw)
    return doc


def check_project(doc):
    """
    Verify that *doc* is a valid novacut/project document.

    For example, a conforming value:

    >>> doc = {
    ...     '_id': 'HB6YSCKAY27KIWUTWKGKCTNI',
    ...     'ver': 0,
    ...     'type': 'dmedia/project',
    ...     'time': 1234567890,
    ...     'db_name': 'dmedia-0-hb6ysckay27kiwutwkgkctni',
    ...     'title': 'UDS-P',
    ... }
    ...
    >>> check_project(doc)

    """
    check_dmedia(doc)
    _check(doc, ['_id'], str,
        _random_id,
    )
    _check(doc, ['type'], str,
        (_equals, 'dmedia/project'),
    )
    _check(doc, ['db_name'], str,
        (_equals, project_db_name(doc['_id'])),
    )
    _check(doc, ['title'], str),


def project_db_name(_id):
    """
    Return the CouchDB database name for the project with *_id*.

    For example:

    >>> project_db_name('HB6YSCKAY27KIWUTWKGKCTNI')
    'dmedia-0-hb6ysckay27kiwutwkgkctni'

    """
    return '-'.join(['dmedia', str(VER), _id.lower()])


def create_project(title=''):
    _id = random_id()
    ts = time.time()
    return {
        '_id': _id,
        'ver': VER,
        'type': 'dmedia/project',
        'time': ts,
        'atime': ts,
        'db_name': project_db_name(_id),
        'title': title,
    }


RE_KEY_STRIP = re.compile(r'[-\s_.,]+')

def create_tag(tag):
    value = tag.strip()
    return {
        '_id': random_id(),
        'ver': VER,
        'type': 'dmedia/tag',
        'time': time.time(),
        'value': value,
        'key': RE_KEY_STRIP.sub('', value).lower(),
    }
