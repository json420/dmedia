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
from base64 import b32encode, b32decode, b64encode
from copy import deepcopy
import time
from .helpers import raises, TempDir, mov_hash, mov_leaves, mov_size
from dmedia.constants import TYPE_ERROR
from dmedia.schema import random_id
from dmedia import schema


class TestFunctions(TestCase):
    def test_random_id(self):
        f = schema.random_id
        _id = f()
        self.assertEqual(len(_id), 24)
        binary = b32decode(_id)
        self.assertEqual(len(binary), 15)
        self.assertEqual(b32encode(binary), _id)

    def test_check_dmedia(self):
        f = schema.check_dmedia

        bad = [
            ('_id', 'MZZG2ZDSOQVSW2TEMVZG643F'),
            ('type', 'dmedia/foo'),
            ('time', 1234567890),
        ]
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
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
        self.assertIsNone(f(g))

        # Check with bad "_id" type:
        bad = deepcopy(good)
        bad['_id'] = 17
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR % ("doc['_id']", basestring, int, 17)
        )

        # Check with invalid "_id" base32-encoding:
        bad = deepcopy(good)
        bad['_id'] = 'MZZG2ZDS0QVSW2TEMVZG643F'  # Replaced "O" with "0"
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['_id']: Non-base32 digit found: 'MZZG2ZDS0QVSW2TEMVZG643F'"
        )

        # Check with bad "_id" length:
        bad = deepcopy(good)
        bad['_id'] = '2HOFUVDSAYHM74JKVKP4AKQ='
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "len(b32decode(doc['_id'])) not multiple of 5: '2HOFUVDSAYHM74JKVKP4AKQ='"
        )

        # Check with bad "ver" type:
        bad = deepcopy(good)
        bad['ver'] = 0.0
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR % ("doc['ver']", int, float, 0.0)
        )

        # Check with bad "ver" value
        bad['ver'] = 1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['ver'] must equal 0; got 1"
        )

        # Check with bad "type" type:
        bad = deepcopy(good)
        bad['type'] = 18
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR % ("doc['type']", basestring, int, 18)
        )

        # Check with bad "type" value
        bad = deepcopy(good)
        bad['type'] = 'foo/bar'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['type']: 'foo/bar' does not match 'dmedia/[a-z]+$'"
        )

        # Check with bad "time" type
        bad = deepcopy(good)
        bad['time'] = '1234567890'
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR % ("doc['time']", (int, float), str, '1234567890')
        )

        # Check with bad "time" value
        bad = deepcopy(good)
        bad['time'] = -1.7
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['time'] must be >= 0; got -1.7"
        )

        # Check with a missing key:
        for key in ['_id', 'ver', 'type', 'time']:
            bad = deepcopy(good)
            del bad[key]
            with self.assertRaises(ValueError) as cm:
                f(bad)
            self.assertEqual(
                str(cm.exception),
                'doc[{!r}] does not exist'.format(key)
            )

    def test_check_file(self):
        f = schema.check_file

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

        # Test with missing attributes:
        for key in ['bytes', 'ext', 'origin', 'stored']:
            bad = deepcopy(good)
            del bad[key]
            with self.assertRaises(ValueError) as cm:
                f(bad)
            self.assertEqual(
                str(cm.exception),
                "doc[{!r}] does not exist".format(key)
            )

        # Test with wrong "type":
        bad = deepcopy(good)
        bad['type'] = 'dmedia/files'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['type'] must equal 'dmedia/file'; got 'dmedia/files'"
        )

        # Test with bytes wrong type:
        bad = deepcopy(good)
        bad['bytes'] *= 1.0
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR % ("doc['bytes']", int, float, bad['bytes'])
        )

        # Test with bytes == 0:
        bad = deepcopy(good)
        bad['bytes'] = 0
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['bytes'] must be >= 1; got 0"
        )

        # Test with bytes == -1:
        bad = deepcopy(good)
        bad['bytes'] = -1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['bytes'] must be >= 1; got -1"
        )

        # Test with bytes=1
        g = deepcopy(good)
        g['bytes'] = 1
        self.assertIsNone(f(g))

        # Test with invalid ext
        bad = deepcopy(good)
        bad['ext'] = '.mov'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['ext']: '.mov' does not match '^[a-z0-9]+(\\\\.[a-z0-9]+)?$'"
        )

        # Test with upercase origin
        bad = deepcopy(good)
        bad['origin'] = 'USER'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['origin'] must be lowercase; got 'USER'"
        )

        # Test with invalid "origin":
        bad = deepcopy(good)
        bad['origin'] = 'foo'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['origin'] value 'foo' not in ('user', 'download', 'paid', 'proxy', 'cache', 'render')"
        )

        # Test with missing stored "copies":
        bad = deepcopy(good)
        del bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies']
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] does not exist"
        )

        # Test with missing stored "time"
        bad = deepcopy(good)
        del bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['time']
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['time'] does not exist"
        )

        # Test with invalid stored "copies":
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = -1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] must be >= 0; got -1"
        )

        # Test with invalid stored "time":
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['time'] = -1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['time'] must be >= 0; got -1"
        )

        # Test with invalid stored "verified":
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified'] = -1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified'] must be >= 0; got -1"
        )

        # Test with invalid stored "status":
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['status'] = 'broken'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['status'] value 'broken' not in ('partial', 'corrupt')"
        )

        # Test with invalid stored "corrupted":
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['corrupted'] = -1
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['corrupted'] must be >= 0; got -1"
        )


    def test_file_optional(self):

        f = schema.check_file_optional
        f({})

        # content_type
        self.assertIsNone(f({'content_type': 'video/quicktime'}))
        e = raises(TypeError, f, {'content_type': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['content_type']", basestring, int, 42)
        )

        # content_encoding
        self.assertIsNone(f({'content_encoding': 'gzip'}))
        self.assertIsNone(f({'content_encoding': 'deflate'}))
        e = raises(TypeError, f, {'content_encoding': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['content_encoding']", basestring, int, 42)
        )
        e = raises(ValueError, f, {'content_encoding': 'stuff'})
        self.assertEqual(
            str(e),
            "doc['content_encoding'] value 'stuff' not in ('gzip', 'deflate')"
        )

        # media
        self.assertIsNone(f({'media': 'video'}))
        self.assertIsNone(f({'media': 'audio'}))
        self.assertIsNone(f({'media': 'image'}))
        e = raises(TypeError, f, {'media': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['media']", basestring, int, 42)
        )
        e = raises(ValueError, f, {'media': 'stuff'})
        self.assertEqual(
            str(e),
            "doc['media'] value 'stuff' not in ('video', 'audio', 'image')"
        )

        # mtime
        self.assertIsNone(f({'mtime': 1302125982.946627}))
        self.assertIsNone(f({'mtime': 1234567890}))
        e = raises(TypeError, f, {'mtime': '1234567890'})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['mtime']", (int, float), str, '1234567890')
        )
        e = raises(ValueError, f, {'mtime': -1})
        self.assertEqual(
            str(e),
            "doc['mtime'] must be >= 0; got -1"
        )

        # atime
        self.assertIsNone(f({'atime': 1302125982.946627}))
        self.assertIsNone(f({'atime': 1234567890}))
        e = raises(TypeError, f, {'atime': '1234567890'})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['atime']", (int, float), str, '1234567890')
        )
        e = raises(ValueError, f, {'atime': -0.3})
        self.assertEqual(
            str(e),
            "doc['atime'] must be >= 0; got -0.3"
        )

        # name
        self.assertIsNone(f({'name': 'MVI_5899.MOV'}))
        e = raises(TypeError, f, {'name': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['name']", basestring, int, 42)
        )

        # dir
        self.assertIsNone(f({'dir': 'DCIM/100EOS5D2'}))
        e = raises(TypeError, f, {'dir': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['dir']", basestring, int, 42)
        )

        # meta
        self.assertIsNone(f({'meta': {'iso': 800}}))
        e = raises(TypeError, f, {'meta': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['meta']", dict, int, 42)
        )

        # user
        self.assertIsNone(f({'user': {'title': 'cool sunset'}}))
        e = raises(TypeError, f, {'user': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['user']", dict, int, 42)
        )

        # tags
        self.assertIsNone(f({'tags': {'burp': {'start': 6, 'end': 73}}}))
        e = raises(TypeError, f, {'tags': 42})
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['tags']", dict, int, 42)
        )

    def test_check_store(self):
        f = schema.check_store

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
                'doc[{!r}] does not exist'.format(key)
            )

        # Test with wrong plugin type/value:
        bad = deepcopy(good)
        bad['plugin'] = 18
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['plugin']", basestring, int, 18)
        )
        bad = deepcopy(good)
        bad['plugin'] = 'foo'
        e = raises(ValueError, f, bad)
        plugins = ('filestore', 'removable_filestore', 'ubuntuone', 's3')
        self.assertEqual(
            str(e),
            "doc['plugin'] value %r not in %r" % ('foo', plugins)
        )

        # Test with wrong copies type/value:
        bad = deepcopy(good)
        bad['copies'] = 2.0
        e = raises(TypeError, f, bad)
        self.assertEqual(
            str(e),
            TYPE_ERROR % ("doc['copies']", int, float, 2.0)
        )
        bad = deepcopy(good)
        bad['copies'] = 0
        bad = deepcopy(good)
        bad['copies'] = -2
        e = raises(ValueError, f, bad)
        self.assertEqual(
            str(e),
            "doc['copies'] must be >= 0; got -2"
        )

    def test_create_file(self):
        f = schema.create_file
        leaf_hashes = b''.join(mov_leaves)

        store = schema.random_id()

        d = f(mov_hash, mov_size, leaf_hashes, {store: {'copies': 2}})
        schema.check_file(d)
        self.assertEqual(
            set(d),
            set([
                '_id',
                '_attachments',
                'ver',
                'type',
                'time',
                'bytes',
                'ext',
                'origin',
                'stored',
            ])
        )
        self.assertEqual(d['_id'], mov_hash)
        self.assertEqual(
            d['_attachments'],
            {
                'leaves': {
                    'data': b64encode(leaf_hashes),
                    'content_type': 'application/octet-stream',
                }
            }
        )
        self.assertEqual(d['ver'], 0)
        self.assertEqual(d['type'], 'dmedia/file')
        self.assertLessEqual(d['time'], time.time())
        self.assertEqual(d['bytes'], mov_size)
        self.assertIsNone(d['ext'], None)
        self.assertEqual(d['origin'], 'user')

        s = d['stored']
        self.assertIsInstance(s, dict)
        self.assertEqual(list(s), [store])
        self.assertEqual(set(s[store]), set(['copies', 'time']))
        self.assertEqual(s[store]['copies'], 2)
        self.assertEqual(s[store]['time'], d['time'])

        d = f(mov_hash, mov_size, leaf_hashes, {store: {'copies': 2}},
            ext='mov'
        )
        schema.check_file(d)
        self.assertEqual(d['ext'], 'mov')

        d = f(mov_hash, mov_size, leaf_hashes, {store: {'copies': 2}},
            origin='proxy'
        )
        schema.check_file(d)
        self.assertEqual(d['origin'], 'proxy')

    def test_create_store(self):
        f = schema.create_store
        tmp = TempDir()
        base = tmp.join('.dmedia')
        machine_id = random_id()

        doc = f(base, machine_id)
        self.assertEqual(schema.check_store(doc), None)
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
        self.assertEqual(schema.check_store(doc), None)
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
        partition_id = random_id()
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

        doc = f(base, partition_id, batch_id=batch_id, machine_id=machine_id)
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

        doc = f(base, partition_id)
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
