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
        for (v0_key, value) in old['stored'].items():
            v1_key = migration.b32_to_db32(v0_key)
            self.assertEqual(new['stored'][v1_key], value)

    def test_migrate_store(self):
        old = {
            "_id": "THDYBKMJDSE4CYJBBQHBYBXB",
            "_rev": "25-2d8b18bdc3dcff97a21bb75b56b56654",
            "atime": 1367035076,
            "copies": 1,
            "plugin": "filestore",
            "time": 1365893138.9566636,
            "type": "dmedia/store"
        }
        new = migration.migrate_store(old)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
            "_id": "MA6R4DFC6L7V5RC44JA4R4Q4",
            "atime": 1367035076,
            "copies": 1,
            "plugin": "filestore",
            "time": 1365893138.9566636,
            "type": "dmedia/store"
        })
        self.assertEqual(migration.b32_to_db32(old['_id']), new['_id'])

    def test_migrate_project(self):
        old = {
            "_id": "VZ76IEM545GVTOVTS4KATSWR",
            "_rev": "1-cb8a53554a9c21d69b6fed7f5bb1f2ee",
            "atime": 1359667301.118886,
            "bytes": 1272543875,
            "count": 52,
            "db_name": "dmedia-0-vz76iem545gvtovts4katswr",
            "time": 1359667301.118886,
            "title": "Test",
            "type": "dmedia/project"
        }
        new = migration.migrate_project(old)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
            "_id": "OSYXB7FWVW9OMHOMLVD3MLPK",
            "atime": 1359667301.118886,
            "bytes": 1272543875,
            "count": 52,
            "db_name": "dmedia-1-osyxb7fwvw9omhomlvd3mlpk",
            "time": 1359667301.118886,
            "title": "Test",
            "type": "dmedia/project"
        })
        self.assertEqual(migration.b32_to_db32(old['_id']), new['_id'])

    def test_migrate_batch(self):
        old = {
            "_id": "BH5RGJLRQILM3BPSBWX45HIE",
            "_rev": "1-c310ed4a024d800d2aa10d19a4f90c95",
            "copies": 3,
            "imports": {
                "EDOVANNPCURG5ZRWD7VDI5LX": {
                    "basedir": "/media/jderose/EOS_DIGITAL",
                    "stats": {
                        "duplicate": {
                            "bytes": 4485454972,
                            "count": 135
                        },
                        "empty": {
                            "bytes": 0,
                            "count": 0
                        },
                        "new": {
                            "bytes": 0,
                            "count": 0
                        },
                        "total": {
                            "bytes": 4485454972,
                            "count": 135
                        }
                    }
                },
                "WSZWZKAXQZVIGAUKHMGCBWCZ": {
                    "basedir": "/media/jderose/EOS_DIGITAL1",
                    "stats": {
                        "duplicate": {
                            "bytes": 1272543875,
                            "count": 52
                        },
                        "empty": {
                            "bytes": 0,
                            "count": 0
                        },
                        "new": {
                            "bytes": 0,
                            "count": 0
                        },
                        "total": {
                            "bytes": 1272543875,
                            "count": 52
                        }
                    }
                }
            },
            "machine_id": "QRWKVZHH4SLHQAXL5XUT6JJGOVJRHERU7V66LHL6TRWMUJQF",
            "rate": "35.8 MB/s",
            "stats": {
                "duplicate": {
                    "bytes": 5757998847,
                    "count": 187
                },
                "empty": {
                    "bytes": 0,
                    "count": 0
                },
                "new": {
                    "bytes": 0,
                    "count": 0
                },
                "total": {
                    "bytes": 5757998847,
                    "count": 187
                }
            },
            "stores": {
                "/home/jderose/.local/share/dmedia": {
                    "copies": 1,
                    "id": "2F6NQJY6FZ2DZKSIWHRXBDPY"
                },
                "/media/jderose/dmedia1": {
                    "copies": 1,
                    "id": "DLA4NDZRW2LXEPF3RV7YHMON"
                },
                "/media/jderose/dmedia2": {
                    "copies": 1,
                    "id": "BTVD5CS2HM4OBDLMAC2L7WZM"
                }
            },
            "time": 1361835132.1264558,
            "time_end": 1361835293.1598194,
            "type": "dmedia/batch"
        }
        new = migration.migrate_batch(old)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
            "_id": "4AWK9CEKJBEFU4IL4PQVWAB7",
            "copies": 3,
            "imports": {
                "76HO3GGI5NK9WSKP6YO6BWEQ": {
                    "basedir": "/media/jderose/EOS_DIGITAL",
                    "stats": {
                        "duplicate": {
                            "bytes": 4485454972,
                            "count": 135
                        },
                        "empty": {
                            "bytes": 0,
                            "count": 0
                        },
                        "new": {
                            "bytes": 0,
                            "count": 0
                        },
                        "total": {
                            "bytes": 4485454972,
                            "count": 135
                        }
                    }
                },
                "PLSPSD3QJSOB93NDAF954P5S": {
                    "basedir": "/media/jderose/EOS_DIGITAL1",
                    "stats": {
                        "duplicate": {
                            "bytes": 1272543875,
                            "count": 52
                        },
                        "empty": {
                            "bytes": 0,
                            "count": 0
                        },
                        "new": {
                            "bytes": 0,
                            "count": 0
                        },
                        "total": {
                            "bytes": 1272543875,
                            "count": 52
                        }
                    }
                }
            },
            "machine_id": "QRWKVZHH4SLHQAXL5XUT6JJGOVJRHERU7V66LHL6TRWMUJQF",
            "rate": "35.8 MB/s",
            "stats": {
                "duplicate": {
                    "bytes": 5757998847,
                    "count": 187
                },
                "empty": {
                    "bytes": 0,
                    "count": 0
                },
                "new": {
                    "bytes": 0,
                    "count": 0
                },
                "total": {
                    "bytes": 5757998847,
                    "count": 187
                }
            },
            "stores": {
                "/home/jderose/.local/share/dmedia": {
                    "copies": 1,
                    "id": "2F6NQJY6FZ2DZKSIWHRXBDPY"
                },
                "/media/jderose/dmedia1": {
                    "copies": 1,
                    "id": "DLA4NDZRW2LXEPF3RV7YHMON"
                },
                "/media/jderose/dmedia2": {
                    "copies": 1,
                    "id": "BTVD5CS2HM4OBDLMAC2L7WZM"
                }
            },
            "time": 1361835132.1264558,
            "time_end": 1361835293.1598194,
            "type": "dmedia/batch"
        })
        for (v0_key, value) in old['imports'].items():
            v1_key = migration.b32_to_db32(v0_key)
            self.assertEqual(new['imports'][v1_key], value)

