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

from filestore import TYPE_ERROR, DIGEST_BYTES, ContentHash
from microfiber import random_id

from .base import TempDir

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
        for key in ['_id', 'type', 'time']:
            bad = deepcopy(good)
            del bad[key]
            with self.assertRaises(ValueError) as cm:
                f(bad)
            self.assertEqual(
                str(cm.exception),
                'doc[{!r}] does not exist'.format(key)
            )

    def test_check_file(self):
        # Test with good doc:
        good = {
            '_id': 'ROHNRBKS6T4YETP5JHEGQ3OLSBDBWRCKR2BKILJOA3CP7QZW',
            '_attachments': {
                'leaf_hashes': {
                    'data': 'v7t381LIyKsBCUYhkGreXx2qKTyyMfMD2eHWWp/L',
                    'content_type': 'application/octet-stream',
                },
            },
            'type': 'dmedia/file',
            'time': 1234567890,
            'atime': 1234567890,
            'bytes': 20202333,
            'origin': 'user',
            'stored': {
                'MZZG2ZDSOQVSW2TEMVZG643F': {
                    'copies': 2,
                    'mtime': 1234567890,
                },
            },
        }
        self.assertEqual(schema.check_file(deepcopy(good)), None)

        required = [
            '_id',
            'type',
            'time',
            
            '_attachments',
            'atime',
            'bytes',
            'origin',
            'stored',
        ]
        # Test with missing attributes:
        for key in required:
            bad = deepcopy(good)
            del bad[key]
            with self.assertRaises(ValueError) as cm:
                schema.check_file(bad)
            self.assertEqual(
                str(cm.exception),
                "doc[{!r}] does not exist".format(key)
            )

        # Test with wrong "type":
        bad = deepcopy(good)
        bad['type'] = 'dmedia/files'
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['type'] must equal 'dmedia/file'; got 'dmedia/files'"
        )

        # Bad "bytes" type:
        bad = deepcopy(good)
        bad['bytes'] *= 1.0
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
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

        # Bad "bytes" value == 0:
        bad = deepcopy(good)
        bad['bytes'] = 0
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['bytes'] must be >= 1; got 0"
        )

        # Bad "bytes" value == -1:
        bad = deepcopy(good)
        bad['bytes'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['bytes'] must be >= 1; got -1"
        )

        # Good "bytes" value == 1:
        also_good = deepcopy(good)
        also_good['bytes'] = 1
        self.assertIsNone(schema.check_file(also_good))

        # Test with upercase origin
        bad = deepcopy(good)
        bad['origin'] = 'USER'
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['origin'] must be lowercase; got 'USER'"
        )

        # Test with invalid "origin":
        bad = deepcopy(good)
        bad['origin'] = 'foo'
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['origin'] value 'foo' not in ('user', 'paid', 'download', 'proxy', 'render', 'cache')"
        )

        # Bad "atime" type:
        bad = deepcopy(good)
        bad['atime'] = 1234567890.5
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['atime']", int, float, 1234567890.5)
        )

        # Bad "atime" value:
        bad = deepcopy(good)
        bad['atime'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['atime'] must be >= 0; got -1"
        )
        
        #####################################################
        # Test all manner of things in doc['stored'].values()

        # Missing "copies":
        bad = deepcopy(good)
        del bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies']
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] does not exist"
        )

        # Bad "copies" type:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = 1.5
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format(
                "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies']", int, float, 1.5
            )
        )

        # Bad "copies" value:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['copies'] must be >= 0; got -1"
        )

        # Missing "mtime":
        bad = deepcopy(good)
        del bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime']
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime'] does not exist"
        )

        # Bad "mtime" type:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime'] = 123.45
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format(
                "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime']", int, float, 123.45
            )
        )

        # Bad "mtime" value:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['mtime'] must be >= 0; got -1"
        )

        # Bad "verified" type:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified'] = 1234.5
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format(
                "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified']", int, float, 1234.5
            )
        )

        # Bad "verified" value:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['verified'] must be >= 0; got -1"
        )

        # Bad "pinned" type:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['pinned'] = 1
        with self.assertRaises(TypeError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format(
                "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['pinned']", bool, int, 1
            )
        )

        # Bad "pinned" value:
        bad = deepcopy(good)
        bad['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['pinned'] = False
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['stored']['MZZG2ZDSOQVSW2TEMVZG643F']['pinned'] must equal True; got False"
        )
        
        ##################################################
        # Test doc['partial'], doc['corrupt'], doc['proxy_of']:

        # Empty "partial"
        bad = deepcopy(good)
        bad['partial'] = {}
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['partial'] cannot be empty; got {}"
        )

        # Empty "corrupt"
        bad = deepcopy(good)
        bad['corrupt'] = {}
        with self.assertRaises(ValueError) as cm:
            schema.check_file(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['corrupt'] cannot be empty; got {}"
        )

        # "proxy_of"
        copy = deepcopy(good)
        copy['origin'] = 'proxy'
        bad_id = random_id()
        copy['proxy_of'] = bad_id
        with self.assertRaises(ValueError) as cm:
            schema.check_file(copy)
        self.assertEqual(
            str(cm.exception),
            "doc['proxy_of']: intrinsic ID must be 48 characters, got 24: {!r}".format(bad_id)
        )
        good_id = random_id(DIGEST_BYTES)
        copy['proxy_of'] = good_id
        self.assertIsNone(schema.check_file(copy))

    def test_file_optional(self):

        f = schema.check_file_optional
        f({})
        
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

        # ctime
        self.assertIsNone(f({'ctime': 1302125982.946627}))
        self.assertIsNone(f({'ctime': 1234567890}))
        with self.assertRaises(TypeError) as cm:
            f({'ctime': '1234567890'})
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['ctime']", (int, float), str, '1234567890')
        )
        with self.assertRaises(ValueError) as cm:
            f({'ctime': -1})
        self.assertEqual(
            str(cm.exception),
            "doc['ctime'] must be >= 0; got -1"
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
        # Test with good doc:
        _id = random_id()
        good = {
            '_id': _id,
            'type': 'dmedia/store',
            'time': 1234567890,
            'plugin': 'filestore',
            'copies': 2,
        }
        g = deepcopy(good)
        self.assertEqual(schema.check_store(g), None)

        # Test with missing attributes:
        for key in ['plugin', 'copies']:
            bad = deepcopy(good)
            del bad[key]
            with self.assertRaises(ValueError) as cm:
                schema.check_store(bad)
            self.assertEqual(
                str(cm.exception),
                'doc[{!r}] does not exist'.format(key)
            )

        # Test with wrong plugin type/value:
        bad = deepcopy(good)
        bad['plugin'] = 18
        with self.assertRaises(TypeError) as cm:
            schema.check_store(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['plugin']", str, int, 18)
        )
        bad = deepcopy(good)
        bad['plugin'] = 'foo'
        with self.assertRaises(ValueError) as cm:
            schema.check_store(bad)
        plugins = ('filestore', 'ubuntuone', 's3')
        self.assertEqual(
            str(cm.exception),
            "doc['plugin'] value %r not in %r" % ('foo', plugins)
        )

        # Test with wrong copies type/value:
        bad = deepcopy(good)
        bad['copies'] = 2.0
        with self.assertRaises(TypeError) as cm:
            schema.check_store(bad)
        self.assertEqual(
            str(cm.exception),
            TYPE_ERROR.format("doc['copies']", int, float, 2.0)
        )
        bad = deepcopy(good)
        bad['copies'] = 0
        bad = deepcopy(good)
        bad['copies'] = -2
        with self.assertRaises(ValueError) as cm:
            schema.check_store(bad)
        self.assertEqual(
            str(cm.exception),
            "doc['copies'] must be >= 0; got -2"
        )

    def test_create_file(self):
        timestamp = time.time()
        _id = random_id(DIGEST_BYTES)
        file_size = 31415
        leaf_hashes = os.urandom(DIGEST_BYTES)
        ch = ContentHash(_id, file_size, leaf_hashes)
        store_id = random_id()
        stored = {store_id: {'copies': 2, 'mtime': 1234567890}}

        doc = schema.create_file(timestamp, ch, stored)
        schema.check_file(doc)
        self.assertEqual(
            set(doc),
            set([
                '_id',
                '_attachments',
                'type',
                'time',
                'atime',
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
        self.assertEqual(doc['type'], 'dmedia/file')
        self.assertEqual(doc['time'], timestamp)
        self.assertEqual(doc['atime'], int(timestamp))
        self.assertEqual(doc['bytes'], file_size)
        self.assertEqual(doc['origin'], 'user')
        self.assertIs(doc['stored'], stored)

        s = doc['stored']
        self.assertIsInstance(s, dict)
        self.assertEqual(list(s), [store_id])
        self.assertEqual(set(s[store_id]), set(['copies', 'mtime']))
        self.assertEqual(s[store_id]['copies'], 2)
        self.assertEqual(s[store_id]['mtime'], 1234567890)

        doc = schema.create_file(timestamp, ch, stored, origin='proxy')
        doc['proxy_of'] = random_id(DIGEST_BYTES)
        schema.check_file(doc)
        self.assertEqual(doc['origin'], 'proxy')

    def test_create_filestore(self):
        start = time.time()
        doc = schema.create_filestore()
        self.assertIsNone(schema.check_store(doc))
        self.assertEqual(
            set(doc),
            set([
                '_id',
                'type',
                'time',
                'plugin',
                'copies',
            ])
        )
        self.assertEqual(doc['type'], 'dmedia/store')
        self.assertTrue(start <= doc['time'] <= time.time())
        self.assertEqual(doc['plugin'], 'filestore')
        self.assertEqual(doc['copies'], 1)

        doc = schema.create_filestore(copies=3)
        self.assertEqual(doc['copies'], 3)

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
                'type',
                'time',
                'imports',
                'machine_id',
                'stats',
            ])
        )
        _id = doc['_id']
        self.assertEqual(len(_id), 24)
        self.assertEqual(doc['type'], 'dmedia/batch')
        self.assertTrue(isinstance(doc['time'], (int, float)))
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['imports'], {})
        self.assertEqual(doc['machine_id'], machine_id)
        self.assertEqual(doc['stats'],
            {
                'total': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
            }
        )

    def test_create_import(self):
        basedir = '/media/EOS_DIGITAL'
        machine_id = random_id()
        doc = schema.create_import(basedir, machine_id)
        self.assertEqual(schema.check_dmedia(doc), None)
        self.assertEqual(set(doc),
            set([
                '_id',
                'type',
                'time',
                'basedir',
                'machine_id',
                'files',
                'stats',
            ])
        )
        self.assertEqual(len(doc['_id']), 24)
        self.assertEqual(doc['type'], 'dmedia/import')
        self.assertTrue(doc['time'] <= time.time())
        self.assertEqual(doc['basedir'], basedir)
        self.assertEqual(doc['machine_id'], machine_id)
        self.assertEqual(doc['files'], {})
        self.assertEqual(doc['stats'],
            {
                'total': {'count': 0, 'bytes': 0},
                'new': {'count': 0, 'bytes': 0},
                'duplicate': {'count': 0, 'bytes': 0},
                'empty': {'count': 0, 'bytes': 0},
            }
        )

    def test_project_db_name(self):
        self.assertEqual(
            schema.project_db_name('AAAAAAAAAAAAAAAAAAAAAAAA'),
            'dmedia-0-aaaaaaaaaaaaaaaaaaaaaaaa',
        )
        _id = random_id()
        self.assertEqual(
            schema.project_db_name(_id),
            'dmedia-0-{}'.format(_id.lower())
        )

    def test_get_project_id(self):
        self.assertEqual(
            schema.get_project_id('dmedia-0-aaaaaaaaaaaaaaaaaaaaaaaa'),
            'AAAAAAAAAAAAAAAAAAAAAAAA'
        )
        self.assertIsNone(schema.get_project_id('dmedia-0'))
        self.assertIsNone(schema.get_project_id('novacut-0'))
        self.assertIsNone(
            schema.get_project_id('novacut-0-aaaaaaaaaaaaaaaaaaaaaaaa')
        )
        # Make sure we can round-trip with project_db_name():
        for i in range(1000):
            _id = random_id()
            db_name = schema.project_db_name(_id)
            self.assertEqual(schema.get_project_id(db_name), _id)

    def test_create_project(self):
        doc = schema.create_project()
        schema.check_project(doc)
        self.assertEqual(doc['title'], '')

        doc = schema.create_project(title='Hobo Spaceship')
        schema.check_project(doc)
        self.assertEqual(doc['title'], 'Hobo Spaceship')

    def test_check_job(self):
        good = {
            '_id': 'H6VVCPDJZ7CSFG4V6EEYCPPD',
            'type': 'dmedia/job',
            'time': 1234567890,
            'status': 'waiting',
            'worker': 'novacut-renderer',
            'files': [
                'ROHNRBKS6T4YETP5JHEGQ3OLSBDBWRCKR2BKILJOA3CP7QZW',
            ],
            'job': {
                'Dmedia': 'ignores everything in job',
            },
        }
        self.assertIsNone(schema.check_job(good))

        # Test all posible status:
        for status in ('waiting', 'executing', 'completed', 'failed'):
            doc = deepcopy(good)
            doc['staus'] = status
            schema.check_job(doc)

        # Test with a bad status:
        doc = deepcopy(good)
        doc['status'] = 'vacationing'
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['status'] value 'vacationing' not in ('waiting', 'executing', 'completed', 'failed')"
        )

        # Test with missing worker
        doc = deepcopy(good)
        del doc['worker']
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['worker'] does not exist"
        )

        # Test with bad file IDs
        id1 = random_id(DIGEST_BYTES)
        id2 = random_id()
        doc = deepcopy(good)
        doc['files'] = [id1, id2]
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['files'][1]: intrinsic ID must be 48 characters, got 24: {!r}".format(id2)
        )

        # Test with an empty job
        doc = deepcopy(good)
        doc['job'] = {}
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['job'] cannot be empty; got {}"
        )

        # Test with a bad machine_id
        doc = deepcopy(good)
        doc['machine_id'] = 'foobar'
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['machine_id']: random ID must be 24 characters, got 6: 'foobar'"
        )

        # Test with bad time_start
        doc = deepcopy(good)
        doc['time_start'] = -1
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['time_start'] must be >= 0; got -1"
        )

        # Test with bad time_end
        doc = deepcopy(good)
        doc['time_end'] = -17
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['time_end'] must be >= 0; got -17"
        )

        # Test with an empty result
        doc = deepcopy(good)
        doc['result'] = ''
        with self.assertRaises(ValueError) as cm:
            schema.check_job(doc)
        self.assertEqual(
            str(cm.exception),
            "doc['result'] cannot be empty; got ''"
        )

    def test_create_job(self):
        worker = 'novacut-' + random_id().lower()
        file_id = random_id(DIGEST_BYTES)
        marker = random_id()
        job = schema.create_job(
            worker,
            [file_id],
            {'ignored': marker},
        )
        schema.check_job(job)
        schema.check_dmedia(job)
        self.assertEqual(
            set(job),
            set([
                '_id',
                'type',
                'time',
                'status',
                'worker',
                'files',
                'job',
            ])
        )
        self.assertEqual(job['status'], 'waiting')
        self.assertEqual(job['worker'], worker)
        self.assertEqual(job['files'], [file_id])
        self.assertEqual(job['job'], {'ignored': marker})

