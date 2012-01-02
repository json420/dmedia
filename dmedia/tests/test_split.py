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

from dmedia import split


sample = """
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


class TestFunctions(TestCase):
    def test_split_to_core(self):
        src = json.loads(sample)
        dst = dict(split.split_to_core(src))
        self.assertEqual(dst,
            {
                '_id': '6YOVAUG25HYQGYQTRZ43T75S52D3E4T47S5R2MHJ7CNGEORX',
                'ver': 0,
                'type': 'dmedia/file',
                'time': 1320469454.88212,
                'atime': 1320469454.88212,
                'bytes': 1999975956,
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
            }
        )
        
        
