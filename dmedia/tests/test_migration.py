# dmedia: distributed media library
# Copyright (C) 2013 Novacut Inc
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
Unit tests for `dmedia.migration`.
"""

from unittest import TestCase
import os

from dbase32 import db32enc
from dbase32.rfc3548 import b32enc

from dmedia import migration


class TestFunctions(TestCase):
    def test_b32_to_db32(self):
        for i in range(100):
            data = os.urandom(15)
            b32 = b32enc(data)
            db32 = db32enc(data)
            self.assertNotEqual(b32, db32)
            self.assertEqual(migration.b32_to_db32(b32), db32)

    def test_migrate_file(self):
        old = {
            "_attachments": {
                "leaf_hashes": {
                    "content_type": "application/octet-stream",
                    "digest": "md5-ZKKmpqjqyogs1tnq1sjCgQ==",
                    "length": 120,
                    "revpos": 1,
                    "stub": True
                }
            },
            "_id": "224ZY4WMNXNKJPIN6U5HMWIMJIGJM2HMA5IOCI3QGDIW2LDT",
            "_rev": "10-7b9067b74908026080ed4c46e63a01ff",
            "atime": 1355388946,
            "bytes": 25272864,
            "origin": "user",
            "stored": {
                "2F6NQJY6FZ2DZKSIWHRXBDPY": {
                    "copies": 1,
                    "mtime": 1366943766,
                    "verified": 1367029021
                },
                "BTVD5CS2HM4OBDLMAC2L7WZM": {
                    "copies": 1,
                    "mtime": 1365931098,
                    "verified": 1366680068
                },
                "DLA4NDZRW2LXEPF3RV7YHMON": {
                    "copies": 1,
                    "mtime": 1366653493,
                    "verified": 1366943766
                },
                "THDYBKMJDSE4CYJBBQHBYBXB": {
                    "copies": 1,
                    "mtime": 1365893318,
                    "verified": 1366945534
                },
                "ZCEHW55N7AHIMTG7VRVAOVUC": {
                    "copies": 0,
                    "mtime": 1366861750
                }
            },
            "time": 1355254766.513135,
            "type": "dmedia/file"
        }

        mdoc = {
            "_attachments": {
                "v1_leaf_hashes": {
                    "content_type": "application/octet-stream",
                    "data": "4m/6sEdR11Sy2kP2OcOqilhYI+2PdZlOYP3JhO1I3ZE/uRxYGmXUd9NlNmxypPUlzfZgEvEQkXf91ctM4U2uNT62KumkmHYEDrzVy0MsJukPz5EBHzujQFKnRvw3omy4bnR3Ge2aIeh/GroKifMQbtdvKQAjRN1A",
                    "digest": "md5-1gc5weDrirGWEtie2Cl3Qw==",
                    "revpos": 1
                }
            },
            "_id": "224ZY4WMNXNKJPIN6U5HMWIMJIGJM2HMA5IOCI3QGDIW2LDT",
            "_rev": "1-56b147a5087f1389e69ee3c3cb1b420c",
            "bytes": 25272864,
            "v1_id": "JPGU6CQ8487K5BTYM3EUDNICUU4UG8ON39LXV6XNIRDE6NXV"
        }

        new = migration.migrate_file(old, mdoc)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
            "_attachments": {
                "leaf_hashes": {
                    "content_type": "application/octet-stream",
                    "data": "4m/6sEdR11Sy2kP2OcOqilhYI+2PdZlOYP3JhO1I3ZE/uRxYGmXUd9NlNmxypPUlzfZgEvEQkXf91ctM4U2uNT62KumkmHYEDrzVy0MsJukPz5EBHzujQFKnRvw3omy4bnR3Ge2aIeh/GroKifMQbtdvKQAjRN1A",
                    "digest": "md5-1gc5weDrirGWEtie2Cl3Qw==",
                    "revpos": 1
                }
            },
            "_id": "JPGU6CQ8487K5BTYM3EUDNICUU4UG8ON39LXV6XNIRDE6NXV",
            "atime": 1355388946,
            "bytes": 25272864,
            "origin": "user",
            "stored": {
                "4MO6W5LTAFVH46EF35TEYPSF": {
                    "copies": 1,
                    "mtime": 1365931098,
                    "verified": 1366680068
                },
                "6E3VG6SKPTEQ7I8UKOYRAFHG": {
                    "copies": 1,
                    "mtime": 1366653493,
                    "verified": 1366943766
                },
                "MA6R4DFC6L7V5RC44JA4R4Q4": {
                    "copies": 1,
                    "mtime": 1365893318,
                    "verified": 1366945534
                },
                "S57APWWGY3ABFM9YOKO3HON5": {
                    "copies": 0,
                    "mtime": 1366861750
                },
                "T8XGJCRX8ST6SDLBPAKQ46IR": {
                    "copies": 1,
                    "mtime": 1366943766,
                    "verified": 1367029021
                }
            },
            "time": 1355254766.513135,
            "type": "dmedia/file"
        })

