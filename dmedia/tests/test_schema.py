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
Unit tests for `dmedia.schema` module.
"""

from unittest import TestCase
from base64 import b32encode, b32decode
from copy import deepcopy
import time
from .helpers import raises, TempDir
from dmedia.constants import TYPE_ERROR
from dmedia.schema import random_id
from dmedia import schema


class test_functions(TestCase):
    def test_check_base32(self):
        f = schema.check_base32

        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('_id', basestring, int, 17)
        )
        e = raises(TypeError, f, True, label='import_id')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('import_id', basestring, bool, True)
        )

        # Test with invalid base32 encoding:
        bad = 'MZzG2ZDSOQVSW2TEMVZG643F'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            '_id: invalid base32: Non-base32 digit found; got %r' % bad
        )
        bad = 'MZZG2ZDSOQVSW2TEMVZG643F='
        e = raises(ValueError, f, bad, label='import_id')
        self.assertEqual(
            str(e),
            'import_id: invalid base32: Incorrect padding; got %r' % bad
        )

        for n in xrange(5, 26):
            b32 = b32encode('a' * n)
            if n % 5 == 0:
                self.assertEqual(f(b32), None)
            else:
                e = raises(ValueError, f, b32, label='foo')
                self.assertEqual(
                    str(e),
                    'len(b32decode(foo)) not multiple of 5: %r' % b32
                )

        self.assertEqual(f('MZZG2ZDSOQVSW2TEMVZG643F'), None)

    def test_check_type(self):
        f = schema.check_type

        # Test with wrong type
        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('type', basestring, int, 17)
        )

        # Test with wrong case
        e = raises(ValueError, f, 'Dmedia/Foo')
        self.assertEqual(
            str(e),
             "type must be lowercase; got 'Dmedia/Foo'"
        )

        # Test with wrong prefix
        e = raises(ValueError, f, 'foo/bar')
        self.assertEqual(
            str(e),
             "type must start with 'dmedia/'; got 'foo/bar'"
        )

        # Test with multiple slashes
        e = raises(ValueError, f, 'dmedia/foo/bar')
        self.assertEqual(
            str(e),
             "type must contain only one '/'; got 'dmedia/foo/bar'"
        )

        # Test with good values
        self.assertEqual(f('dmedia/foo'), None)
        self.assertEqual(f('dmedia/machine'), None)

    def test_check_time(self):
        f = schema.check_time

        # Test with wrong type
        bad = '123456789'
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time', (int, float), str, bad)
        )
        bad = u'123456789.18'
        e = raises(TypeError, f, bad, label='time_end')
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('time_end', (int, float), unicode, bad)
        )

        # Test with negative value
        bad = -1234567890
        e = raises(ValueError, f, bad, label='mtime')
        self.assertEqual(
            str(e),
            'mtime must be >= 0; got %r' % bad
        )
        bad = -1234567890.18
        e = raises(ValueError, f, bad, label='foo')
        self.assertEqual(
            str(e),
            'foo must be >= 0; got %r' % bad
        )

        # Test with good values
        self.assertEqual(f(1234567890), None)
        self.assertEqual(f(1234567890.18), None)
        self.assertEqual(f(0), None)
        self.assertEqual(f(0.0), None)

    def test_check_dmedia(self):
        f = schema.check_dmedia

        bad = [
            ('_id', 'MZZG2ZDSOQVSW2TEMVZG643F'),
            ('type', 'dmedia/foo'),
            ('time', 1234567890),
        ]
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('doc', dict, list, bad)
        )

        good = {
            '_id': 'MZZG2ZDSOQVSW2TEMVZG643F',
            'ver': 0,
            'type': 'dmedia/foo',
            'time': 1234567890,
            'foo': 'bar',
        }
        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # check with bad ver:
        bad = deepcopy(good)
        bad['ver'] = 0.0
        e = raises(TypeError, f, bad)
        self.assertEqual(str(e), TYPE_ERROR % ('ver', int, float, 0.0))
        bad['ver'] = 1
        e = raises(ValueError, f, bad)
        self.assertEqual(str(e), "doc['ver'] must be 0; got 1")

        for key in ['_id', 'ver', 'type', 'time']:
            bad = deepcopy(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % [key]
            )
        for keys in (['_id', 'type'], ['_id', 'time'], ['time', 'type']):
            bad = deepcopy(good)
            for key in keys:
                del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % keys
            )
        bad = {'foo': 'bar'}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'doc missing keys: %r' % ['_id', 'time', 'type', 'ver']
        )

    def test_check_stored(self):
        f = schema.check_stored

        good = {
            'MZZG2ZDSOQVSW2TEMVZG643F': {
                'copies': 2,
                'time': 1234567890,
            },
            'NZXXMYLDOV2F6ZTUO5PWM5DX': {
                'copies': 1,
                'time': 1234666890,
            },
        }

        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # Test with wrong type:
        bad = [
            (
                'MZZG2ZDSOQVSW2TEMVZG643F',
                {
                    'copies': 2,
                    'time': 1234567890,
                }
            )
        ]
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('stored', dict, list, bad)
        )

        # Test with empty value:
        e = raises(ValueError, f, {})
        self.assertEqual(str(e), 'stored cannot be empty; got {}')

        # Test with bad key
        bad = deepcopy(good)
        bad['MFQWCYLBMFQWCYI='] =  {'copies': 2, 'time': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "len(b32decode(<key in stored>)) not multiple of 5: 'MFQWCYLBMFQWCYI='"
        )

        # Test with wrong value Type
        bad = deepcopy(good)
        v = (2, 1234567890)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = v
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("stored['OVRHK3TUOUQCWIDMNFXGC4TP']", dict, tuple, v)
        )

        # Test with misisng value keys
        bad = deepcopy(good)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = {'number': 2, 'time': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "stored['OVRHK3TUOUQCWIDMNFXGC4TP'] missing keys: ['copies']"
        )
        bad = deepcopy(good)
        bad['OVRHK3TUOUQCWIDMNFXGC4TP'] = {'number': 2, 'added': 1234567890}
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "stored['OVRHK3TUOUQCWIDMNFXGC4TP'] missing keys: ['copies', 'time']"
        )

        # Test with bad 'copies' type/value:
        label = "stored['MZZG2ZDSOQVSW2TEMVZG643F']['copies']"
        bad = deepcopy(good)
        bad['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = 2.0
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % (label, int, float, 2.0)
        )
        bad = deepcopy(good)
        bad['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = 0
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            '%s must be >= 1; got 0' % label
        )

        # Test with bad 'time' type/value:
        label = "stored['MZZG2ZDSOQVSW2TEMVZG643F']['time']"
        bad = deepcopy(good)
        bad['MZZG2ZDSOQVSW2TEMVZG643F']['time'] = '1234567890'
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % (label, (int, float), str, '1234567890')
        )
        bad = deepcopy(good)
        bad['MZZG2ZDSOQVSW2TEMVZG643F']['time'] = -1
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            '%s must be >= 0; got -1' % label
        )

    def test_check_ext(self):
        f = schema.check_ext

        # Test wrong type:
        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('ext', basestring, int, 17)
        )

        # Test empty strings:
        e = raises(ValueError, f, '')
        self.assertEqual(str(e), "ext cannot be empty; got ''")
        e = raises(ValueError, f, u'', 'foo')
        self.assertEqual(str(e), "foo cannot be empty; got u''")

        # Test with upper/mixed case:
        e = raises(ValueError, f, u'Mov')
        self.assertEqual(str(e), "ext must be lowercase; got u'Mov'")
        e = raises(ValueError, f, 'TAR.GZ', 'bar')
        self.assertEqual(str(e), "bar must be lowercase; got 'TAR.GZ'")

        # Test with leading/ending period:
        e = raises(ValueError, f, '.tar.gz')
        self.assertEqual(str(e), "ext cannot start with a period; got '.tar.gz'")
        e = raises(ValueError, f, 'tar.gz.')
        self.assertEqual(str(e), "ext cannot end with a period; got 'tar.gz.'")

        # Test with values that don't batch regex:
        e = raises(ValueError, f, 'tar/gz')
        self.assertEqual(
            str(e),
            r"ext: 'tar/gz' does not match '^[a-z0-9]+(\\.[a-z0-9]+)?$'"
        )
        e = raises(ValueError, f, 'tar..gz')
        self.assertEqual(
            str(e),
            r"ext: 'tar..gz' does not match '^[a-z0-9]+(\\.[a-z0-9]+)?$'"
        )
        e = raises(ValueError, f, 'og*')
        self.assertEqual(
            str(e),
            r"ext: 'og*' does not match '^[a-z0-9]+(\\.[a-z0-9]+)?$'"
        )

        # Test with good values:
        self.assertEqual(f(None), None)
        self.assertEqual(f('mov'), None)
        self.assertEqual(f('tar.gz'), None)

    def test_check_origin(self):
        f = schema.check_origin

        # Test with wrong type
        e = raises(TypeError, f, 17)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('origin', basestring, int, 17)
        )

        # Test when empty
        e = raises(ValueError, f, '')
        self.assertEqual(
            str(e),
            "origin cannot be empty; got ''"
        )

        # Test when not lowercase
        e = raises(ValueError, f, 'useR')
        self.assertEqual(
            str(e),
            "origin must be lowercase; got 'useR'"
        )

        # Test when not valid identifier:
        e = raises(ValueError, f, '9lives')
        self.assertEqual(
            str(e),
            "origin: '9lives' does not match '^[a-z][_a-z0-9]*$'"
        )
        e = raises(ValueError, f, '_foo')
        self.assertEqual(
            str(e),
            "origin: '_foo' does not match '^[a-z][_a-z0-9]*$'"
        )
        e = raises(ValueError, f, 'hello-world')
        self.assertEqual(
            str(e),
            "origin: 'hello-world' does not match '^[a-z][_a-z0-9]*$'"
        )

        # Test some good values:
        self.assertEqual(f('foo'), None)
        self.assertEqual(f('foo_'), None)
        self.assertEqual(f('lives9'), None)
        self.assertEqual(f('foo_lives9'), None)
        self.assertEqual(f('lives9foo_'), None)
        self.assertEqual(f('hello_world'), None)

        # Test with strict=True
        e = raises(ValueError, f, 'foo', strict=True)
        self.assertEqual(
            str(e),
            "origin: 'foo' not in ['user', 'download', 'paid', 'proxy', 'cache', 'render']"
        )

        # Test all good strict=True values
        self.assertEqual(f('user', strict=True), None)
        self.assertEqual(f('download', strict=True), None)
        self.assertEqual(f('paid', strict=True), None)
        self.assertEqual(f('proxy', strict=True), None)
        self.assertEqual(f('cache', strict=True), None)
        self.assertEqual(f('render', strict=True), None)


    def test_check_dmedia_file(self):
        f = schema.check_dmedia_file

        # Test with good doc:
        good = {
            '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
            'ver': 0,
            'type': 'dmedia/file',
            'time': 1234567890,
            'bytes': 20202333,
            'ext': 'mov',
            'origin': 'user',
            'stored': {
                'MZZG2ZDSOQVSW2TEMVZG643F': {
                    'copies': 2,
                    'time': 1234567890,
                },
            },
        }
        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # Test with wrong record type:
        bad = deepcopy(good)
        bad['type'] = 'dmedia/files'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "doc['type'] must be 'dmedia/file'; got 'dmedia/files'"
        )

        # Test with missing attributes:
        for key in ['bytes', 'ext', 'origin', 'stored']:
            bad = deepcopy(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % [key]
            )

        # Test with bytes wrong type:
        bad = deepcopy(good)
        bad['bytes'] *= 1.0
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('bytes', int, float, bad['bytes'])
        )

        # Test with bytes < 1:
        bad = deepcopy(good)
        bad['bytes'] = 0
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'bytes must be >= 1; got 0'
        )
        bad = deepcopy(good)
        bad['bytes'] = -1
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'bytes must be >= 1; got -1'
        )

        # Test with bytes=1
        g = deepcopy(good)
        g['bytes'] = 1
        self.assertEqual(f(g), None)

        # Test with invalid ext
        bad = deepcopy(good)
        bad['ext'] = '.mov'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "ext cannot start with a period; got '.mov'"
        )

        # Test with invalid origin
        bad = deepcopy(good)
        bad['origin'] = 'USER'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "origin must be lowercase; got 'USER'"
        )

        # Make sure origin is checked with strict=True
        bad = deepcopy(good)
        bad['origin'] = 'foo'
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "origin: 'foo' not in ['user', 'download', 'paid', 'proxy', 'cache', 'render']"
        )

        # Test with invalid stored
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = -1
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "stored['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] must be >= 1; got -1"
        )


    def test_check_dmedia_store(self):
        f = schema.check_dmedia_store

        # Test with good doc:
        good = {
            '_id': 'ZR765XWSF6S7JQHLUI4GCG5BHGPE252O',
            'ver': 0,
            'type': 'dmedia/file',
            'time': 1234567890,
            'plugin': 'filestore',
            'copies': 2,
        }
        g = deepcopy(good)
        self.assertEqual(f(g), None)

        # Test with missing attributes:
        for key in ['plugin', 'copies']:
            bad = deepcopy(good)
            del bad[key]
            e = raises(ValueError, f, bad)
            self.assertEqual(
                str(e),
                'doc missing keys: %r' % [key]
            )

        # Test with wrong plugin type/value:
        bad = deepcopy(good)
        bad['plugin'] = 18
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('plugin', basestring, int, 18)
        )
        bad = deepcopy(good)
        bad['plugin'] = 'foo'
        e = raises(ValueError, f, bad)
        plugins = ['filestore', 'removable_filestore', 'ubuntuone', 's3']
        self.assertEqual(
            str(e),
            'plugin %r not in %r' % ('foo', plugins)
        )

        # Test with wrong copies type/value:
        bad = deepcopy(good)
        bad['copies'] = 2.0
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ('copies', int, float, 2.0)
        )
        bad = deepcopy(good)
        bad['copies'] = 0
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'copies must be >= 1; got 0'
        )
        bad = deepcopy(good)
        bad['copies'] = -2
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            'copies must be >= 1; got -2'
        )

    def test_random_id(self):
        f = schema.random_id
        _id = f()
        self.assertEqual(len(_id), 24)
        binary = b32decode(_id)
        self.assertEqual(len(binary), 15)
        self.assertEqual(b32encode(binary), _id)

    def test_create_store(self):
        f = schema.create_store
        tmp = TempDir()
        base = tmp.join('.dmedia')
        machine_id = random_id()

        doc = f(base, machine_id)
        self.assertEqual(schema.check_dmedia_store(doc), None)
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'ver',
                'type',
                'time',
                'plugin',
                'copies',
                'path',
                'machine_id',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/store')
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['plugin'], 'filestore')
        self.assertEqual(doc['copies'], 1)
        self.assertEqual(doc['path'], base)

        doc = f(base, machine_id, copies=3)
        self.assertEqual(schema.check_dmedia_store(doc), None)
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'ver',
                'type',
                'time',
                'plugin',
                'copies',
                'path',
                'machine_id',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/store')
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['plugin'], 'filestore')
        self.assertEqual(doc['copies'], 3)
        self.assertEqual(doc['path'], base)
        self.assertEqual(doc['machine_id'], machine_id)

    def test_create_batch(self):
        f = schema.create_batch
        machine_id = random_id()
        doc = f(machine_id)

        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'ver',
                'type',
                'time',
                'imports',
                'errors',
                'machine_id',
                'stats',
            ])
        )
        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/batch')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['imports'], [])
        self.assertEqual(doc['errors'], [])
        self.assertEqual(doc['machine_id'], machine_id)
        self.assertEqual(
            doc['stats'],
            {
                'considered': {'count': 0, 'bytes': 0},
                'imported': {'count': 0, 'bytes': 0},
                'skipped': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )

    def test_create_import(self):
        f = schema.create_import

        base = '/media/EOS_DIGITAL'
        batch_id = random_id()
        machine_id = random_id()

        keys = set([
            '_id',
            'ver',
            'type',
            'time',
            'base',
            'batch_id',
            'machine_id',
            'log',
            'stats',
        ])

        doc = f(base, batch_id=batch_id, machine_id=machine_id)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(set(doc), keys)

        _id = doc['_id']
        self.assertEqual(b32encode(b32decode(_id)), _id)
        self.assertEqual(len(_id), 24)

        self.assertEqual(doc['type'], 'dmedia/import')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['base'], base)
        self.assertEqual(doc['batch_id'], batch_id)
        self.assertEqual(doc['machine_id'], machine_id)

        doc = f(base)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertEqual(set(doc), keys)
        self.assertEqual(doc['batch_id'], None)
        self.assertEqual(doc['machine_id'], None)
        self.assertEqual(
            doc['log'],
            {
                'imported': [],
                'skipped': [],
                'empty': [],
                'error': [],
            }
        )
        self.assertEqual(
            doc['stats'],
            {
                'imported': {'count': 0, 'bytes': 0},
                'skipped': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
                'error': {'count': 0, 'bytes': 0},
            }
        )
