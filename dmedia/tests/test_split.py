# dmedia: dmedia hashing protocol and file layout
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Unit tests for `dmedia.split`.
"""

from unittest import TestCase
import json
from copy import deepcopy

from microfiber import random_id

from dmedia import split, schema


sample_file = """
{
   "_id": "6YOVAUG25HYQGYQTRZ43T75S52D3E4T47S5R2MHJ7CNGEORX",
   "_rev": "8-f73a3a18e3fe634af4cdd543d4b55880",
   "atime": 1320469454.88212,
   "bytes": 1999975956,
   "content_type": "video/quicktime",
   "corrupt": {
   },
   "ctime": 1320472976,
   "ext": "mov",
   "import": {
       "batch_id": "IBGXM2UIF53WZ5FEWUZCYYJ4",
       "import_id": "VTNZLEO5SAWHBEVQLNPZPABY",
       "machine_id": "HW6GAWV33TYHRAT5XNQLDXTR",
       "mtime": 1320472976,
       "src": "/media/H4N_SD_/DCIM/100CANON/MVI_3406.MOV"
   },
   "meta": {
       "aperture": 2.8,
       "camera": "Canon EOS 60D",
       "camera_serial": "0520422197",
       "channels": "Stereo",
       "codec_audio": "Raw 16-bit PCM audio",
       "codec_video": "H.264 / AVC",
       "duration": 339,
       "focal_length": "14.0 mm",
       "fps": 30,
       "height": 1080,
       "iso": 800,
       "lens": "Canon EF 14mm f/2.8L II USM",
       "mtime": 1320458236.37,
       "sample_rate": 48000,
       "shutter": "1/30",
       "width": 1920
   },
   "name": "MVI_3406.MOV",
   "origin": "user",
   "partial": {
   },
   "proxies": {
       "W4J4FRF5UOCOX4KQ3JZIQMFSAAPTBGRBZIBXVCFEFAZ7OMZS": {
           "bytes": 50880190,
           "content_type": "video/webm",
           "height": 540,
           "width": 960
       }
   },
   "stored": {
       "BTVD5CS2HM4OBDLMAC2L7WZM": {
           "copies": 1,
           "mtime": 1320475903.9720492,
           "plugin": "filestore"
       },
       "DLA4NDZRW2LXEPF3RV7YHMON": {
           "copies": 1,
           "mtime": 1320475903.9680493,
           "plugin": "filestore"
       }
   },
   "time": 1320469454.88212,
   "type": "dmedia/file",
   "ver": 0,
   "_attachments": {
       "leaf_hashes": {
           "content_type": "application/octet-stream",
           "revpos": 1,
           "digest": "md5-6h6hUlNAmi0d2fbTpROrLw==",
           "length": 7170,
           "stub": true
       },
       "thumbnail": {
           "content_type": "image/jpeg",
           "revpos": 7,
           "digest": "md5-rDUdjxrzjg5yAsPSt0uWhg==",
           "length": 15146,
           "stub": true
       }
   }
}
"""


core_file = {
    '_attachments': {
        'leaf_hashes': {
            'content_type': 'application/octet-stream',
            'revpos': 1,
            'digest': 'md5-6h6hUlNAmi0d2fbTpROrLw==',
            'length': 7170,
            'stub': True,
        },
    },
    '_id': '6YOVAUG25HYQGYQTRZ43T75S52D3E4T47S5R2MHJ7CNGEORX',
    'ver': 0,
    'type': 'dmedia/file',
    'time': 1320469454.88212,
    'atime': 1320469454.88212,
    'bytes': 1999975956,
    'content_type': 'video/quicktime',
    'ext': 'mov',
    'origin': 'user',
    'stored': {
        'BTVD5CS2HM4OBDLMAC2L7WZM': {
            'copies': 1,
            'mtime': 1320475903.9720492,
            'plugin': 'filestore',
        },
        'DLA4NDZRW2LXEPF3RV7YHMON': {
            'copies': 1,
            'mtime': 1320475903.9680493,
            'plugin': 'filestore',
        },
    },
    'partial': {},
    'corrupt': {},
    'proxies': {
         'W4J4FRF5UOCOX4KQ3JZIQMFSAAPTBGRBZIBXVCFEFAZ7OMZS': {
              'bytes': 50880190,
              'content_type': 'video/webm',
              'height': 540,
              'width': 960
         }
    },
}

project_file = {
    '_id': '6YOVAUG25HYQGYQTRZ43T75S52D3E4T47S5R2MHJ7CNGEORX',
    'bytes': 1999975956,
    'content_type': 'video/quicktime',
    'ctime': 1320472976,
    'ext': 'mov',
    'import': {
         'batch_id': 'IBGXM2UIF53WZ5FEWUZCYYJ4',
         'import_id': 'VTNZLEO5SAWHBEVQLNPZPABY',
         'machine_id': 'HW6GAWV33TYHRAT5XNQLDXTR',
         'mtime': 1320472976,
         'src': '/media/H4N_SD_/DCIM/100CANON/MVI_3406.MOV'
    },
    'meta': {
         'aperture': 2.8,
         'camera': 'Canon EOS 60D',
         'camera_serial': '0520422197',
         'channels': 'Stereo',
         'codec_audio': 'Raw 16-bit PCM audio',
         'codec_video': 'H.264 / AVC',
         'duration': 339,
         'focal_length': '14.0 mm',
         'fps': 30,
         'height': 1080,
         'iso': 800,
         'lens': 'Canon EF 14mm f/2.8L II USM',
         'mtime': 1320458236.37,
         'sample_rate': 48000,
         'shutter': '1/30',
         'width': 1920
    },
    'name': 'MVI_3406.MOV',
    'origin': 'user',
    'proxies': {
         'W4J4FRF5UOCOX4KQ3JZIQMFSAAPTBGRBZIBXVCFEFAZ7OMZS': {
              'bytes': 50880190,
              'content_type': 'video/webm',
              'height': 540,
              'width': 960
         }
    },
    'tags': {},
    'time': 1320469454.88212,
    'type': 'dmedia/file',
    'ver': 0,
    '_attachments': {
         'leaf_hashes': {
              'content_type': 'application/octet-stream',
              'revpos': 1,
              'digest': 'md5-6h6hUlNAmi0d2fbTpROrLw==',
              'length': 7170,
              'stub': True,
         },
         'thumbnail': {
              'content_type': 'image/jpeg',
              'revpos': 7,
              'digest': 'md5-rDUdjxrzjg5yAsPSt0uWhg==',
              'length': 15146,
              'stub': True,
         }
    }
}


sample_proxy = """
{
   "_id": "ZY3EZMTYWEFNDOVU74OUU2SQAY37GUBNHQLIOUIAJHQ6PKRO",
   "_rev": "1-5528ee232e98d5f7d97f57cd6801fc48",
   "atime": 1325055007.130608,
   "bytes": 20979329,
   "content_type": "video/webm",
   "corrupt": {
   },
   "elapsed": 329.1916332244873,
   "ext": "webm",
   "origin": "proxy",
   "partial": {
   },
   "proxyof": "P6HD6F3H7LGSH4Z7VIW7UMLIB2GPETCVNHIIMDVHULTOOF2L",
   "stored": {
       "342QKXWMYVM3ORHO7WKAF4XK": {
           "copies": 1,
           "mtime": 1325055006.8708012,
           "plugin": "filestore"
       }
   },
   "time": 1325055007.130608,
   "type": "dmedia/file",
   "ver": 0,
   "_attachments": {
       "leaf_hashes": {
           "content_type": "application/octet-stream",
           "revpos": 1,
           "digest": "md5-9q57mOLzKk9Uekviuc5QLg==",
           "length": 90,
           "stub": true
       }
   }
}
"""

core_proxy = {
    '_id': 'ZY3EZMTYWEFNDOVU74OUU2SQAY37GUBNHQLIOUIAJHQ6PKRO',
    'atime': 1325055007.130608,
    'bytes': 20979329,
    'content_type': 'video/webm',
    'corrupt': {},
    'ext': 'webm',
    'origin': 'proxy',
    'partial': {},
    'proxy_of': 'P6HD6F3H7LGSH4Z7VIW7UMLIB2GPETCVNHIIMDVHULTOOF2L',
    'stored': {
         '342QKXWMYVM3ORHO7WKAF4XK': {
              'copies': 1,
              'mtime': 1325055006.8708012,
              'plugin': 'filestore'
         }
    },
    'time': 1325055007.130608,
    'type': 'dmedia/file',
    'ver': 0,
    '_attachments': {
         'leaf_hashes': {
              'content_type': 'application/octet-stream',
              'revpos': 1,
              'digest': 'md5-9q57mOLzKk9Uekviuc5QLg==',
              'length': 90,
              'stub': True,
         }
    }
}


project_proxy = {
    '_id': 'ZY3EZMTYWEFNDOVU74OUU2SQAY37GUBNHQLIOUIAJHQ6PKRO',
    'bytes': 20979329,
    'content_type': 'video/webm',
    'elapsed': 329.1916332244873,
    'ext': 'webm',
    'origin': 'proxy',
    'proxy_of': 'P6HD6F3H7LGSH4Z7VIW7UMLIB2GPETCVNHIIMDVHULTOOF2L',
    'tags': {},
    'time': 1325055007.130608,
    'type': 'dmedia/file',
    'ver': 0,
    '_attachments': {
         'leaf_hashes': {
              'content_type': 'application/octet-stream',
              'revpos': 1,
              'digest': 'md5-9q57mOLzKk9Uekviuc5QLg==',
              'length': 90,
              'stub': True,
         }
    }
}


class TestFunctions(TestCase):
    def test_file_to_core(self):
        doc = json.loads(sample_file)
        self.assertEqual(
            dict(split.file_to_core(doc)),
            core_file
        )
        schema.check_file(core_file)
        schema.check_file(core_proxy)

    def test_file_to_project(self):
        doc = json.loads(sample_file)
        self.assertEqual(
            dict(split.file_to_project(doc)),
            project_file
        )

    def test_doc_to_core(self):
        doc = json.loads(sample_file)
        self.assertEqual(split.doc_to_core(doc), core_file)

        doc = json.loads(sample_proxy)
        self.assertEqual(split.doc_to_core(doc), core_proxy)

        # dmedia/store
        doc = {'_id': random_id(), 'type': 'dmedia/store'}
        self.assertIs(split.doc_to_core(doc), doc)

        # dmedia/machine
        doc = {'_id': random_id(), 'type': 'dmedia/machine'}
        self.assertIs(split.doc_to_core(doc), doc)
        
        # dmedia/import
        doc = {'_id': random_id(), 'type': 'dmedia/import'}
        self.assertIsNone(split.doc_to_core(doc))

        # dmedia/batch
        doc = {'_id': random_id(), 'type': 'dmedia/batch'}
        self.assertIsNone(split.doc_to_core(doc))

    def test_doc_to_project(self):
        doc = json.loads(sample_file)
        self.assertEqual(split.doc_to_project(doc), project_file)

        doc = json.loads(sample_proxy)
        self.assertEqual(split.doc_to_project(doc), project_proxy)

        # dmedia/store
        doc = {'_id': random_id(), 'type': 'dmedia/store'}
        self.assertIsNone(split.doc_to_project(doc))

        # dmedia/machine
        doc = {'_id': random_id(), 'type': 'dmedia/machine'}
        self.assertIsNone(split.doc_to_project(doc))

        # dmedia/import
        doc = {'_id': random_id(), 'type': 'dmedia/import'}
        self.assertIs(split.doc_to_project(doc), doc)

        # dmedia/batch
        doc = {'_id': random_id(), 'type': 'dmedia/batch'}
        self.assertIs(split.doc_to_project(doc), doc)
