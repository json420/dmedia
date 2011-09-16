# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   David Green <david4dev@gmail.com>
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
import os
from base64 import b32encode, b32decode, b64encode
from copy import deepcopy
import time

from filestore import TYPE_ERROR, DIGEST_BYTES
from microfiber import random_id

from .helpers import TempDir

from dmedia import schema


class TestFunctions(TestCase):
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
            TYPE_ERROR.format('doc', dict, list, bad)
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
            TYPE_ERROR.format("doc['_id']", str, int, 17)
        )

        # Check with invalid "_id" base32-encoding:
        bad = deepcopy(good)
        bad['_id'] = 'MZZG2ZDS0QVSW2TEMVZG643F'  # Replaced "O" with "0"
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['_id']: ID not subset of B32ALPHABET: 'MZZG2ZDS0QVSW2TEMVZG643F'"
        )

        # Check with bad "_id" length:
        bad = deepcopy(good)
        bad['_id'] = '2HOFUVDSAYHM74JKVKP4AKQ'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['_id']: length of ID (23) not multiple of 8: '2HOFUVDSAYHM74JKVKP4AKQ'"
        )

        # Check with bad "ver" type:
        bad = deepcopy(good)
        bad['ver'] = 0.0
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['ver']", int, float, 0.0)
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
            TYPE_ERROR.format("doc['type']", str, int, 18)
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
            TYPE_ERROR.format("doc['time']", (int, float), str, '1234567890')
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
            '_id': 'ROHNRBKS6T4YETP5JHEGQ3OLSBDBWRCKR2BKILJOA3CP7QZW',
            '_attachments': {
                'leaf_hashes': {
                    'data': 'v7t381LIyKsBCUYhkGreXx2qKTyyMfMD2eHWWp/L',
                    'content_type': 'application/octet-stream',
                },
            },
            'ver': 0,
            'type': 'dmedia/file',
            'time': 1234567890,
            'bytes': 20202333,
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
        for key in ['_attachments', 'bytes', 'origin', 'stored']:
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
        try:
            self.assertEqual(
                str(cm.exception),
                TYPE_ERROR.format("doc['bytes']", int, float, bad['bytes'])
            )
        except:
            self.assertEqual(
                str(cm.exception),
                TYPE_ERROR.formtat("doc['bytes']", long, float, bad['bytes'])
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
            "doc['origin'] value 'foo' not in ('user', 'paid', 'download', 'proxy', 'render', 'cache')"
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
        
        # ext
        self.assertIsNone(f({'ext': 'ogv'}))
        with self.assertRaises(TypeError) as cm:
            f({'ext': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['ext']", str, int, 42)
        )
        with self.assertRaises(ValueError) as cm:
            f({'ext': '.mov'})
        self.assertEqual(
            str(cm.exception),
            "doc['ext']: '.mov' does not match '^[a-z0-9]+(\\\\.[a-z0-9]+)?$'"
        )

        # content_type
        self.assertIsNone(f({'content_type': 'video/quicktime'}))
        with self.assertRaises(TypeError) as cm:
            f({'content_type': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['content_type']", str, int, 42)
        )

        # content_encoding
        self.assertIsNone(f({'content_encoding': 'gzip'}))
        self.assertIsNone(f({'content_encoding': 'deflate'}))
        with self.assertRaises(TypeError) as cm:
            f({'content_encoding': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['content_encoding']", str, int, 42)
        )
        with self.assertRaises(ValueError) as cm:
            f({'content_encoding': 'stuff'})
        self.assertEqual(
            str(cm.exception),
            "doc['content_encoding'] value 'stuff' not in ('gzip', 'deflate')"
        )

        # media
        self.assertIsNone(f({'media': 'video'}))
        self.assertIsNone(f({'media': 'audio'}))
        self.assertIsNone(f({'media': 'image'}))
        with self.assertRaises(TypeError) as cm:
            f({'media': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['media']", str, int, 42)
        )
        with self.assertRaises(ValueError) as cm:
            f({'media': 'stuff'})
        self.assertEqual(
            str(cm.exception),
            "doc['media'] value 'stuff' not in ('video', 'audio', 'image')"
        )

        # mtime
        self.assertIsNone(f({'mtime': 1302125982.946627}))
        self.assertIsNone(f({'mtime': 1234567890}))
        with self.assertRaises(TypeError) as cm:
            f({'mtime': '1234567890'})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['mtime']", (int, float), str, '1234567890')
        )
        with self.assertRaises(ValueError) as cm:
            f({'mtime': -1})
        self.assertEqual(
            str(cm.exception),
            "doc['mtime'] must be >= 0; got -1"
        )

        # atime
        self.assertIsNone(f({'atime': 1302125982.946627}))
        self.assertIsNone(f({'atime': 1234567890}))
        with self.assertRaises(TypeError) as cm:
            f({'atime': '1234567890'})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['atime']", (int, float), str, '1234567890')
        )
        with self.assertRaises(ValueError) as cm:
            f({'atime': -0.3})
        self.assertEqual(
            str(cm.exception),
            "doc['atime'] must be >= 0; got -0.3"
        )

        # name
        self.assertIsNone(f({'name': 'MVI_5899.MOV'}))
        with self.assertRaises(TypeError) as cm:
            f({'name': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['name']", str, int, 42)
        )

        # dir
        self.assertIsNone(f({'dir': 'DCIM/100EOS5D2'}))
        with self.assertRaises(TypeError) as cm:
            f({'dir': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['dir']", str, int, 42)
        )

        # meta
        self.assertIsNone(f({'meta': {'iso': 800}}))
        with self.assertRaises(TypeError) as cm:
            f({'meta': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['meta']", dict, int, 42)
        )

        # user
        self.assertIsNone(f({'user': {'title': 'cool sunset'}}))
        with self.assertRaises(TypeError) as cm:
            f({'user': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['user']", dict, int, 42)
        )

        # tags
        self.assertIsNone(f({'tags': {'burp': {'start': 6, 'end': 73}}}))
        with self.assertRaises(TypeError) as cm:
            f({'tags': 42})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['tags']", dict, int, 42)
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
            with self.assertRaises(ValueError) as cm:
                f(bad)
            self.assertEqual(
                str(cm.exception),
                'doc[{!r}] does not exist'.format(key)
            )

        # Test with wrong plugin type/value:
        bad = deepcopy(good)
        bad['plugin'] = 18
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['plugin']", str, int, 18)
        )
        bad = deepcopy(good)
        bad['plugin'] = 'foo'
        with self.assertRaises(ValueError) as cm:
            f(bad)
        plugins = ('filestore', 'removable_filestore', 'ubuntuone', 's3')
        self.assertEqual(
            str(cm.exception),
            "doc['plugin'] value %r not in %r" % ('foo', plugins)
        )

        # Test with wrong copies type/value:
        bad = deepcopy(good)
        bad['copies'] = 2.0
        with self.assertRaises(TypeError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['copies']", int, float, 2.0)
        )
        bad = deepcopy(good)
        bad['copies'] = 0
        bad = deepcopy(good)
        bad['copies'] = -2
        with self.assertRaises(ValueError) as cm:
            f(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['copies'] must be >= 0; got -2"
        )

    def test_create_file(self):
        f = schema.create_file

        _id = random_id(DIGEST_BYTES)
        leaf_hashes = os.urandom(DIGEST_BYTES)
        file_size = 31415
        store_id = random_id()

        doc = f(_id, file_size, leaf_hashes, {store_id: {'copies': 2}})
        schema.check_file(doc)
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_attachments',
                'ver',
                'type',
                'time',
                'bytes',
                'origin',
                'stored',
            ])
        )
        self.assertEqual(doc['_id'], _id)
        self.assertEqual(
            doc['_attachments'],
            {
                'leaf_hashes': {
                    'data': b64encode(leaf_hashes).decode('utf-8'),
                    'content_type': 'application/octet-stream',
                }
            }
        )
        self.assertEqual(doc['ver'], 0)
        self.assertEqual(doc['type'], 'dmedia/file')
        self.assertLessEqual(doc['time'], time.time())
        self.assertEqual(doc['bytes'], file_size)
        self.assertEqual(doc['origin'], 'user')

        s = doc['stored']
        self.assertIsInstance(s, dict)
        self.assertEqual(list(s), [store_id])
        self.assertEqual(set(s[store_id]), set(['copies', 'time']))
        self.assertEqual(s[store_id]['copies'], 2)
        self.assertEqual(s[store_id]['time'], doc['time'])

        doc = f(_id, file_size, leaf_hashes, {store_id: {'copies': 2}},
            origin='proxy'
        )
        schema.check_file(doc)
        self.assertEqual(doc['origin'], 'proxy')

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
                #'partition_id',
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
                #'partition_id',
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
            'partition_id',
            'log',
            'stats',
        ])

        doc = f(base, partition_id, batch_id=batch_id, machine_id=machine_id)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertTrue(isinstance(doc, dict))
        self.assertEqual(set(doc), keys)

        _id = doc['_id']
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
