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

import dbase32
from dbase32 import rfc3548
from dbase32 import db32enc
from dbase32.rfc3548 import b32enc
from usercouch.misc import CouchTestCase
from microfiber import Server

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
            "atime": 1355388946.1776,
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

        # Test with missing atime
        del old['atime']
        new = migration.migrate_file(old, mdoc)
        self.assertIsNot(new, old)
        self.assertEqual(new['atime'], 1355254766)

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

        # When old['count'], old['bytes'] is missing:
        del old['count']
        del old['bytes']
        new = migration.migrate_project(old)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
            "_id": "OSYXB7FWVW9OMHOMLVD3MLPK",
            "atime": 1359667301.118886,
            "bytes": 0,
            "count": 0,
            "db_name": "dmedia-1-osyxb7fwvw9omhomlvd3mlpk",
            "time": 1359667301.118886,
            "title": "Test",
            "type": "dmedia/project"
        })

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
        self.assertEqual(new['_id'], migration.b32_to_db32(old['_id']))
        for (v0_key, value) in old['imports'].items():
            v1_key = migration.b32_to_db32(v0_key)
            self.assertEqual(new['imports'][v1_key], value)

    def test_migrate_import(self):
        old = {
            "_attachments": {
                "thumbnail": {
                    "content_type": "image/jpeg",
                    "digest": "md5-X3htIqnEE1AVF3QPy4oNoQ==",
                    "length": 19116,
                    "revpos": 1,
                    "stub": True
                }
            },
            "_id": "XQECJCBFVPQXC2R64KEHL4RS",
            "_rev": "1-f5b1ad7534619edd962af00050021de5",
            "basedir": "/media/jderose/EOS_DIGITAL",
            "basedir_ismount": True,
            "batch_id": "YGS74CJEM4ALWELSBJA4X7HO",
            "files": {
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6187.CR2": {
                    "bytes": 24164511,
                    "id": "FMQGK2HVK575NMSP5DJEZUK2F73QPXNFYMJS6UFFGBA44YSM",
                    "mtime": 1360392066.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6188.CR2": {
                    "bytes": 24480241,
                    "id": "V33KL6SZL536JCKY5ZOGTBONAGLNKD5ESKO7CUW24YLXOLZS",
                    "mtime": 1360392068.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6189.CR2": {
                    "bytes": 24072152,
                    "id": "PCYU57ZWBFOE45GPDXAQPVABUOV6PAS233C4KCDKQO4XME7U",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6190.CR2": {
                    "bytes": 24031624,
                    "id": "KJMCMICLWEBMWIOMDLVTNAOELEQRBIEND6BUXVXEAMZPV3BH",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6191.CR2": {
                    "bytes": 24004723,
                    "id": "RMVNNYNRHAI6NQWZC5WD7AGHHFM6C3FW7WM64BWWKJEYI7E6",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6192.CR2": {
                    "bytes": 24021197,
                    "id": "XBCQ44R2XGWGIAGO34WQL5DZLMKLK3TT5XJ3RLAFASZKBWIY",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6193.CR2": {
                    "bytes": 24057124,
                    "id": "IBZHZHS2RASGZCLWKNYHXYCOKHLXTRIEKJBWWIXV3BHU4GJL",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6194.CR2": {
                    "bytes": 24060398,
                    "id": "GSBV5XCGFYW3STSSE6ZJUNBSDX5HUYKPQKNMHUEMGOG6OUHN",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6195.CR2": {
                    "bytes": 24307380,
                    "id": "JD543HEIX6SSSTMHONL5MWZK53VGABENZL32K4EZZVPDTGLG",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6196.CR2": {
                    "bytes": 24192879,
                    "id": "3HVNVGYU7UQJAIV3RQPZ7QXGOVBXYRZNODFP4LLHTR2BA2GU",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6197.CR2": {
                    "bytes": 24353774,
                    "id": "YH6VRBYG5SO65XVCWVS5IBL4XAP4CF5FNSBAAWRRVMUWR2MD",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6198.CR2": {
                    "bytes": 24291455,
                    "id": "YE4GDNVQYPOOWQYQAFOP7RFCQW6KISLNW27BRRQDKABJVJKL",
                    "mtime": 1360392080.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6199.CR2": {
                    "bytes": 24337573,
                    "id": "NFWHFUI6MBSASBNH4QLVQOTPOTB7ZL7I4TAKHJX7WPVLLDMX",
                    "mtime": 1360392080.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6200.CR2": {
                    "bytes": 24364307,
                    "id": "SB2IIIFMGLAAFZNIODYT7QCHPHX37LAR6NIGE67SLSNWL7PW",
                    "mtime": 1360392082.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6201.CR2": {
                    "bytes": 24280808,
                    "id": "TAWQC7YYH6BCZQ253A2ZCGJSMB5NCCWE4ABLLXPX5S5LCVHP",
                    "mtime": 1360392082.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6202.CR2": {
                    "bytes": 24590521,
                    "id": "TC5FXVUDHNDQNAZXAQCHYUX2IOVMPN5KYQQOLORR56M4YFSD",
                    "mtime": 1360392088.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6203.CR2": {
                    "bytes": 25067205,
                    "id": "ES6HNC5Q7PYNLCIN6JLPAKQRC2RGZVKI5RZEHH2E62PQEC7C",
                    "mtime": 1360392088.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6204.CR2": {
                    "bytes": 24488712,
                    "id": "ZNC3FNF46USMRDMXDPCSV4T4S2PFZDT26YP4Y5PGUR7BZVVG",
                    "mtime": 1360392090.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6205.CR2": {
                    "bytes": 24963961,
                    "id": "EVR6U2XTPZLOMZZ4VL2CRD7I7ZLMA56CT7LRAMY2HY2KJC3J",
                    "mtime": 1360392092.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6206.CR2": {
                    "bytes": 25149655,
                    "id": "CKC4WO3VNB55XQTZXVAF7WOSBUNXAJLP27ILDMWTJD4WW7B5",
                    "mtime": 1360392092.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6207.CR2": {
                    "bytes": 24680671,
                    "id": "E2LVGWPVIUTHWG6NFFMPXV3MEDLKY2HNHJDLWVWHJVUFLGCL",
                    "mtime": 1360392098.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6208.CR2": {
                    "bytes": 24714309,
                    "id": "7UUOOPM2OL7BPUBYG75VZXREFBZQOAGMKUQ536GZ5RAY4MVG",
                    "mtime": 1360392100.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6209.CR2": {
                    "bytes": 24291205,
                    "id": "6HENVNK32CR5FVF2T7ED2XE2NFQOH3ZWZ7ICBSHFTG542JEB",
                    "mtime": 1360392108.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6210.CR2": {
                    "bytes": 25334550,
                    "id": "PSTBVLYPYVXCFGIGWUOYOXY3USDCKPFMU4UOSYTOYKLLYRVV",
                    "mtime": 1360392110.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6211.CR2": {
                    "bytes": 24624226,
                    "id": "IU224GVLX6JUD7AWUPWMSO3HHTUGR5YWL7OZ6L3UHR3O5J3U",
                    "mtime": 1360392112.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6212.CR2": {
                    "bytes": 23494569,
                    "id": "4XA23QDT4V3AJTZIRPS2B3QR6L7VAAVLRTRYTVZUH4ESKFRP",
                    "mtime": 1360392114.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6213.CR2": {
                    "bytes": 24041795,
                    "id": "PHDPRZMEPSH2GXV7JK7UWUAX3DXOCSL6TLX6RMZL2VTSAXKN",
                    "mtime": 1360392118.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6214.CR2": {
                    "bytes": 24028923,
                    "id": "JT4MAI3WEGWCYWVRS7J4MCW536F4NPJSSXVZMISK4YF4T3JH",
                    "mtime": 1360392118.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6215.CR2": {
                    "bytes": 23913424,
                    "id": "QXXGGC5KYIEZYEOZVX5N3GEC7NRDVKRLOQYBLJNTOTRNCOHY",
                    "mtime": 1360392120.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6216.CR2": {
                    "bytes": 23845226,
                    "id": "CUL7V5K6N4OQOIRBWST4M2NTHR5BAVVLVTKWRZ6ZUNFGWCJ3",
                    "mtime": 1360392122.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6217.CR2": {
                    "bytes": 24640641,
                    "id": "DN2L6RRSQQOYPGX753NXT2CYBPYCKL57W2NH4BDBUCNAUH2X",
                    "mtime": 1360568646.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6218.CR2": {
                    "bytes": 24200333,
                    "id": "FAW7NGI22LOEV7ISFW7FXIPMUVUMMCDUYFJHKM55WEAAWSMK",
                    "mtime": 1360568648.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6219.CR2": {
                    "bytes": 24402398,
                    "id": "O4ENISIIBBKTW4TQPDL4UE2OZBCDD3O3LCJJOXRGT4QE7PD4",
                    "mtime": 1360568648.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6220.CR2": {
                    "bytes": 24593882,
                    "id": "NK4WA3D6BIXCR5Q5AUH6IMIKAT3XULNXVPVRCHMRMGNRWVAZ",
                    "mtime": 1360568654.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6221.CR2": {
                    "bytes": 24598745,
                    "id": "L2KFY27OZFVQNSCAJXX6SHEMSZYCWKARHKU2MX723JHI52DS",
                    "mtime": 1360568656.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6222.CR2": {
                    "bytes": 24484112,
                    "id": "27MFC2M5EHPE7PCR26NVMTPJF5SFJUQFUGLKGQGENLSWTE7X",
                    "mtime": 1360568660.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6223.CR2": {
                    "bytes": 24690330,
                    "id": "T7RUKRWNWZJA3VDFIOHQZHHPM2QT4VX37NROLJTVTTI2B6PO",
                    "mtime": 1360568660.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6224.CR2": {
                    "bytes": 24460827,
                    "id": "ZOX7LEDVJYBHNJVRF7F3MYWZVMOXJWG6DLWWOEPEM5LLB6JM",
                    "mtime": 1360568662.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6225.CR2": {
                    "bytes": 24471116,
                    "id": "654FH23HLY4THIHT7353MHSYNV2YVQYFHVQDGHGPYPW3XYJA",
                    "mtime": 1360568662.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6226.CR2": {
                    "bytes": 24603831,
                    "id": "WNZNDN3SMOCEXNSQ7EHPAUNXY22KZY7WJZEZTBXCO6Z7RUQD",
                    "mtime": 1360568664.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6227.CR2": {
                    "bytes": 24535757,
                    "id": "ULAFEHXX4NIXX2ERUWBB2UO3SH2IZDZFZHF5DMEJKQQGCNWG",
                    "mtime": 1360568664.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6228.CR2": {
                    "bytes": 24891215,
                    "id": "YXLC7MOA37I3K3HERKVJ7EVGGC4CUTPEIJANOSP5J65ZRIMS",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6229.CR2": {
                    "bytes": 25199170,
                    "id": "F2C53RRYKP7KPJUWF5BGJZXB36UGMRLGUWKSUZ3BBN6VAFOC",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6230.CR2": {
                    "bytes": 24789919,
                    "id": "LZY6VOA6K5ZN5UDNRJKX4HOXXETZGD7PM4CRZ5NDKAVPPWG4",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6231.CR2": {
                    "bytes": 24746424,
                    "id": "PHO4H3J4DHJABDU2XHGO4OVPAHKVLS4GBO4ZCE3ZDQH3EJVV",
                    "mtime": 1360568668.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6232.CR2": {
                    "bytes": 24809460,
                    "id": "IGUIQGB3B3DRQWTAK5O3O3J7MKFMKTIY4YV2XNEDBLRLHCMQ",
                    "mtime": 1360568678.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6233.CR2": {
                    "bytes": 24699538,
                    "id": "OSVJFCFZTZYLDEJ5K74YUMBNZYANBBUBEVVLDPLLEYXXPUI7",
                    "mtime": 1360568678.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6234.CR2": {
                    "bytes": 24802591,
                    "id": "TYB7XEUAZZG7PBEQI4CK5XAQNULDLHQDDP3Z5OLTDE4D7R22",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6235.CR2": {
                    "bytes": 24629902,
                    "id": "4ZQS3VDFPU62S3RN6M3K4D6QSHX6WRPRE6XTF7ISWNVCS7OQ",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6236.CR2": {
                    "bytes": 24673202,
                    "id": "5R6D6Q7QNOWHMWUVP5QSRLUSOI7E4O22MNWIZ4MOEUR335V5",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6237.CR2": {
                    "bytes": 24723757,
                    "id": "4RR7QPTAXGQL6PQS6E747RAOGYKLHZLPVHIC4NBPI6GNXIS7",
                    "mtime": 1360568682.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6238.CR2": {
                    "bytes": 24647627,
                    "id": "B5FIGLLEEVPY4ARSQNEZ6V7FDWT4N2WSNJCHLIPSANJNPZAF",
                    "mtime": 1360568682.0,
                    "status": "duplicate"
                }
            },
            "machine_id": "QRWKVZHH4SLHQAXL5XUT6JJGOVJRHERU7V66LHL6TRWMUJQF",
            "rate": "28.5 MB/s",
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
            },
            "statvfs": {
                "avail": 30735171584,
                "frsize": 32768,
                "readonly": False,
                "size": 32008667136,
                "used": 1273495552
            },
            "stores": {
                "/home/jderose/.local/share/dmedia": {
                    "copies": 1,
                    "id": "2F6NQJY6FZ2DZKSIWHRXBDPY"
                }
            },
            "time": 1361823238.1185207,
            "time_end": 1361823282.8363862,
            "type": "dmedia/import"
        }

        id_map = {
            "27MFC2M5EHPE7PCR26NVMTPJF5SFJUQFUGLKGQGENLSWTE7X": "OTBJ98MDJ8VCUD8SBK68NDO3ML38VQC7KVGWA59J6YJ3NKFQ",
            "3HVNVGYU7UQJAIV3RQPZ7QXGOVBXYRZNODFP4LLHTR2BA2GU": "ISOLQIWOJG8XRQNIJXQLNH8MTP6SID6NA7KK5R8IBYOW8JHB",
            "4RR7QPTAXGQL6PQS6E747RAOGYKLHZLPVHIC4NBPI6GNXIS7": "AYRUILMANIQXCLFHFCVWGFS6GB4Y67AEGQIUTKPEI4WAU984",
            "4XA23QDT4V3AJTZIRPS2B3QR6L7VAAVLRTRYTVZUH4ESKFRP": "HAG7YFCEKP77WOX74NOBQIVKCI8484KUSYLO7EWDFKWRO4KJ",
            "4ZQS3VDFPU62S3RN6M3K4D6QSHX6WRPRE6XTF7ISWNVCS7OQ": "8GLBEM7UP83A98H7YS59WN66HSFV9S79OHKTI3X4O5QIQB7Q",
            "5R6D6Q7QNOWHMWUVP5QSRLUSOI7E4O22MNWIZ4MOEUR335V5": "3CVGIMRGSQ7UJHAG8Q358S3BGQILUABJVIFHYTXEEVYEEHC5",
            "654FH23HLY4THIHT7353MHSYNV2YVQYFHVQDGHGPYPW3XYJA": "ODRKRMNG7QOYAE8Q5XM9EEJKYJIBF5JJAPPARJNQN8CAQARC",
            "6HENVNK32CR5FVF2T7ED2XE2NFQOH3ZWZ7ICBSHFTG542JEB": "U6PH9P6CYX5DYO3KASNSARBAXV7TS8MKHX37EONJEK8MQM4A",
            "7UUOOPM2OL7BPUBYG75VZXREFBZQOAGMKUQ536GZ5RAY4MVG": "DFKPEIY4O54BMVOJJK4PFC8HUFJIXA7ERW84Y5IE8K9EPSLK",
            "B5FIGLLEEVPY4ARSQNEZ6V7FDWT4N2WSNJCHLIPSANJNPZAF": "4PHQUF6FHYTOE6BWBUV6SGHNXANN6DLC6F5KCL6MXJPME7VB",
            "CKC4WO3VNB55XQTZXVAF7WOSBUNXAJLP27ILDMWTJD4WW7B5": "SNJA75R8BXE6AACGYC55VH6LUQPBRWWW8XLYNXGCAIKMI4LM",
            "CUL7V5K6N4OQOIRBWST4M2NTHR5BAVVLVTKWRZ6ZUNFGWCJ3": "HCQV63I787LWUUJQTFN9KJRT48D7488383QTVT38GRVLBN65",
            "DN2L6RRSQQOYPGX753NXT2CYBPYCKL57W2NH4BDBUCNAUH2X": "S8HCBQHBYJV7EMCOMVHHK4R43F6JMP3PFJVPDF9KSO7IC73H",
            "E2LVGWPVIUTHWG6NFFMPXV3MEDLKY2HNHJDLWVWHJVUFLGCL": "JIBBA4XQA4DA76CJI3KSBULPANXRBO7E9GI8Y3BLT57TQXDF",
            "ES6HNC5Q7PYNLCIN6JLPAKQRC2RGZVKI5RZEHH2E62PQEC7C": "TMDAX6TVS4JIMNYUEHOOOHPUYAOEGLQTG98TDJYB6SH783PT",
            "EVR6U2XTPZLOMZZ4VL2CRD7I7ZLMA56CT7LRAMY2HY2KJC3J": "89DMFDLIEHEJQYXD6J4VMYAJGOB49I3S7COVQFMF7HTNXP3W",
            "F2C53RRYKP7KPJUWF5BGJZXB36UGMRLGUWKSUZ3BBN6VAFOC": "OJOFDBRBM4X3UKLPK3UCORLOBCUVQ85MHH46LD8O7FMQOO4F",
            "FAW7NGI22LOEV7ISFW7FXIPMUVUMMCDUYFJHKM55WEAAWSMK": "5S34A6ASKKQECTMD4QCEFJLOLBAIA7ABFS4SDLMDQKTS4RI6",
            "FMQGK2HVK575NMSP5DJEZUK2F73QPXNFYMJS6UFFGBA44YSM": "X6LMYHQMW4KLUQT7VQCQAIIAB3954NGSMRCJX7CQMVUUIJBB",
            "GSBV5XCGFYW3STSSE6ZJUNBSDX5HUYKPQKNMHUEMGOG6OUHN": "GQ7FFPNXOKBIT345DSQJDX3TMFKDRJWFSNGSYUGKHHBJBHLE",
            "IBZHZHS2RASGZCLWKNYHXYCOKHLXTRIEKJBWWIXV3BHU4GJL": "QHNH54QN7LALAOEAUSFYM54TIKJBJAY8RR3RLX4KKDQQGJIH",
            "IGUIQGB3B3DRQWTAK5O3O3J7MKFMKTIY4YV2XNEDBLRLHCMQ": "DFM6IAQAX9JISNPNENBTHUDL96DG93BRJ9O4MG8HMGHHE3AN",
            "IU224GVLX6JUD7AWUPWMSO3HHTUGR5YWL7OZ6L3UHR3O5J3U": "KX9K9D8CK4RLCYILQ7S7X35YTOK8DU85PTYCOAAIKOX673WT",
            "JD543HEIX6SSSTMHONL5MWZK53VGABENZL32K4EZZVPDTGLG": "RJGO9WQD4V84KACT3FILYBVY9J6U8VNXXAJ7JEHASN5Q57JW",
            "JT4MAI3WEGWCYWVRS7J4MCW536F4NPJSSXVZMISK4YF4T3JH": "T8FAYB9REVFCQFYJCCXNP8JHO9LKRB8UD7E6KLE9GSRAYDOD",
            "KJMCMICLWEBMWIOMDLVTNAOELEQRBIEND6BUXVXEAMZPV3BH": "W7JPSWB8TRLYH85PDF3HWA5YNTJWSJNAUMEM73RRTC9QXVW8",
            "L2KFY27OZFVQNSCAJXX6SHEMSZYCWKARHKU2MX723JHI52DS": "VD3PFGX79E6QVDUDYTCPLOYH5PVYTH8UTGU9COSXL6GM6B4R",
            "LZY6VOA6K5ZN5UDNRJKX4HOXXETZGD7PM4CRZ5NDKAVPPWG4": "A93BTX39A5MXILTP85DTTPBQK6M69PBWG3R3CFNGYB3YHKJE",
            "NFWHFUI6MBSASBNH4QLVQOTPOTB7ZL7I4TAKHJX7WPVLLDMX": "HVIVCXJFAC5WHJBGQYNIBX7TB7WSR7MRLSOLKN5KUEYN6MLX",
            "NK4WA3D6BIXCR5Q5AUH6IMIKAT3XULNXVPVRCHMRMGNRWVAZ": "LHRY5WM9Q4RYUYQOVOF797GTC3TBE3IBCR8BT4M5KDDQYBEF",
            "O4ENISIIBBKTW4TQPDL4UE2OZBCDD3O3LCJJOXRGT4QE7PD4": "QUSDCJNEXBOGPGTJRIGYIRIC3TMGB34I5QB9MDQTMT5C64MR",
            "OSVJFCFZTZYLDEJ5K74YUMBNZYANBBUBEVVLDPLLEYXXPUI7": "KQYPHTAJWCUYQUT8BSVQ9IOY83EBOGDD8L6RJIMOSEJEPL3M",
            "PCYU57ZWBFOE45GPDXAQPVABUOV6PAS233C4KCDKQO4XME7U": "FJXJTFDX5HA6KQ6U3YB93SSGNKTR7MP4GPBECJLP8E3XNVJB",
            "PHDPRZMEPSH2GXV7JK7UWUAX3DXOCSL6TLX6RMZL2VTSAXKN": "95WUQOG8N8P69MQ6XWS66ELP49VHJIHEY3P8PA3G6IS8E5WJ",
            "PHO4H3J4DHJABDU2XHGO4OVPAHKVLS4GBO4ZCE3ZDQH3EJVV": "EP5FLHXQHQRFXB59LYSIXDMTYBXHIFABFPJPCC4IKIA9RQX9",
            "PSTBVLYPYVXCFGIGWUOYOXY3USDCKPFMU4UOSYTOYKLLYRVV": "QIMK7DJ8XO6QBQROF9XCCMTNQVFQSD8UQY45YH9MSRA8LKL7",
            "QXXGGC5KYIEZYEOZVX5N3GEC7NRDVKRLOQYBLJNTOTRNCOHY": "STA8YD7Y8GI6DILU7BGRWD73GJJEX7S58SWTGW99YAEHCVBC",
            "RMVNNYNRHAI6NQWZC5WD7AGHHFM6C3FW7WM64BWWKJEYI7E6": "CSKAJ8CW5ULWBHIELUL4J4XNJVYN9QC9LVBQEEH9563W4XS8",
            "SB2IIIFMGLAAFZNIODYT7QCHPHX37LAR6NIGE67SLSNWL7PW": "YAHB6NV6OCHFR6J7ACGHV49W9VSLTSHLNS67GDOWEEQPWKJE",
            "T7RUKRWNWZJA3VDFIOHQZHHPM2QT4VX37NROLJTVTTI2B6PO": "J4AHE9UHFTGX4P4HVIMK8MUQIMFES3B6LQWKKV8XET55Y6OO",
            "TAWQC7YYH6BCZQ253A2ZCGJSMB5NCCWE4ABLLXPX5S5LCVHP": "B7OY37JYF7JJF7C4RR9KMODKJ5MDYTKKJCRFV6M5BXNEJC73",
            "TC5FXVUDHNDQNAZXAQCHYUX2IOVMPN5KYQQOLORR56M4YFSD": "QYCP7IJN7X3WQW4MXR5FRKIRIAO33X9UNWI4YYUW6QPC3EJT",
            "TYB7XEUAZZG7PBEQI4CK5XAQNULDLHQDDP3Z5OLTDE4D7R22": "HKQ53VFOM3M3E6DF69SIHADLD94BPC9XTL4HQYAMOUXH6OVK",
            "ULAFEHXX4NIXX2ERUWBB2UO3SH2IZDZFZHF5DMEJKQQGCNWG": "4COX64LWMA4CAR5KVNT8KVMHWU6NW8QAHNJ8VT7VYWP37MVT",
            "V33KL6SZL536JCKY5ZOGTBONAGLNKD5ESKO7CUW24YLXOLZS": "Y3HPAUM9YOJ4IO6YDJTYDN5PJTC4RC5JEN37LDUT4WKHRJMP",
            "WNZNDN3SMOCEXNSQ7EHPAUNXY22KZY7WJZEZTBXCO6Z7RUQD": "QG58GHJVJOAYBPTARWPSXIGK4AJDIBDPMEKLBVIUKIPNLXJ8",
            "XBCQ44R2XGWGIAGO34WQL5DZLMKLK3TT5XJ3RLAFASZKBWIY": "CH7XNR95XQRRAPTE6L9K56PVP73WX5XM5AT6PPA5T7QGVH6F",
            "YE4GDNVQYPOOWQYQAFOP7RFCQW6KISLNW27BRRQDKABJVJKL": "FU3AOKN3YGBBU6NJ6HHEFK7UJAICHNL7NSO8SYDK8H9BS8LB",
            "YH6VRBYG5SO65XVCWVS5IBL4XAP4CF5FNSBAAWRRVMUWR2MD": "HSNUS6OK4U59THOMDKU9S3INKYRGBDWOIUF4X5E6RLUKVC66",
            "YXLC7MOA37I3K3HERKVJ7EVGGC4CUTPEIJANOSP5J65ZRIMS": "QCL7DIFUOEJCC6L797B8Y7SOFHNI577WDWP9A4DCQVQRVBIR",
            "ZNC3FNF46USMRDMXDPCSV4T4S2PFZDT26YP4Y5PGUR7BZVVG": "3CJWW3UDLDDE7NLFUFK4HYODPPYL9EPUUN4GTQO6M6F6IJCJ",
            "ZOX7LEDVJYBHNJVRF7F3MYWZVMOXJWG6DLWWOEPEM5LLB6JM": "3LCFTF844RSRGQYVNOVIKER3NYCDVRMEQ647NLL4A7X9NHWE"
        }

        new = migration.migrate_import(old, id_map)
        self.assertIsNot(new, old)
        self.assertEqual(new['_id'], migration.b32_to_db32(old['_id']))
        self.assertEqual(new['batch_id'], migration.b32_to_db32(old['batch_id']))
        self.assertEqual(new, {
            "_attachments": {
                "thumbnail": {
                    "content_type": "image/jpeg",
                    "digest": "md5-X3htIqnEE1AVF3QPy4oNoQ==",
                    "length": 19116,
                    "revpos": 1,
                    "stub": True
                }
            },
            "_id": "QJ75C548OIJQ5TKXVD7AEVKL",
            "basedir": "/media/jderose/EOS_DIGITAL",
            "basedir_ismount": True,
            "batch_id": "R9LYV5C7FV3EP7EL4C3VQYAH",
            "files": {
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6187.CR2": {
                    "bytes": 24164511,
                    "id": "X6LMYHQMW4KLUQT7VQCQAIIAB3954NGSMRCJX7CQMVUUIJBB",
                    "mtime": 1360392066.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6188.CR2": {
                    "bytes": 24480241,
                    "id": "Y3HPAUM9YOJ4IO6YDJTYDN5PJTC4RC5JEN37LDUT4WKHRJMP",
                    "mtime": 1360392068.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6189.CR2": {
                    "bytes": 24072152,
                    "id": "FJXJTFDX5HA6KQ6U3YB93SSGNKTR7MP4GPBECJLP8E3XNVJB",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6190.CR2": {
                    "bytes": 24031624,
                    "id": "W7JPSWB8TRLYH85PDF3HWA5YNTJWSJNAUMEM73RRTC9QXVW8",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6191.CR2": {
                    "bytes": 24004723,
                    "id": "CSKAJ8CW5ULWBHIELUL4J4XNJVYN9QC9LVBQEEH9563W4XS8",
                    "mtime": 1360392070.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6192.CR2": {
                    "bytes": 24021197,
                    "id": "CH7XNR95XQRRAPTE6L9K56PVP73WX5XM5AT6PPA5T7QGVH6F",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6193.CR2": {
                    "bytes": 24057124,
                    "id": "QHNH54QN7LALAOEAUSFYM54TIKJBJAY8RR3RLX4KKDQQGJIH",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6194.CR2": {
                    "bytes": 24060398,
                    "id": "GQ7FFPNXOKBIT345DSQJDX3TMFKDRJWFSNGSYUGKHHBJBHLE",
                    "mtime": 1360392072.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6195.CR2": {
                    "bytes": 24307380,
                    "id": "RJGO9WQD4V84KACT3FILYBVY9J6U8VNXXAJ7JEHASN5Q57JW",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6196.CR2": {
                    "bytes": 24192879,
                    "id": "ISOLQIWOJG8XRQNIJXQLNH8MTP6SID6NA7KK5R8IBYOW8JHB",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6197.CR2": {
                    "bytes": 24353774,
                    "id": "HSNUS6OK4U59THOMDKU9S3INKYRGBDWOIUF4X5E6RLUKVC66",
                    "mtime": 1360392078.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6198.CR2": {
                    "bytes": 24291455,
                    "id": "FU3AOKN3YGBBU6NJ6HHEFK7UJAICHNL7NSO8SYDK8H9BS8LB",
                    "mtime": 1360392080.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6199.CR2": {
                    "bytes": 24337573,
                    "id": "HVIVCXJFAC5WHJBGQYNIBX7TB7WSR7MRLSOLKN5KUEYN6MLX",
                    "mtime": 1360392080.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6200.CR2": {
                    "bytes": 24364307,
                    "id": "YAHB6NV6OCHFR6J7ACGHV49W9VSLTSHLNS67GDOWEEQPWKJE",
                    "mtime": 1360392082.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6201.CR2": {
                    "bytes": 24280808,
                    "id": "B7OY37JYF7JJF7C4RR9KMODKJ5MDYTKKJCRFV6M5BXNEJC73",
                    "mtime": 1360392082.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6202.CR2": {
                    "bytes": 24590521,
                    "id": "QYCP7IJN7X3WQW4MXR5FRKIRIAO33X9UNWI4YYUW6QPC3EJT",
                    "mtime": 1360392088.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6203.CR2": {
                    "bytes": 25067205,
                    "id": "TMDAX6TVS4JIMNYUEHOOOHPUYAOEGLQTG98TDJYB6SH783PT",
                    "mtime": 1360392088.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6204.CR2": {
                    "bytes": 24488712,
                    "id": "3CJWW3UDLDDE7NLFUFK4HYODPPYL9EPUUN4GTQO6M6F6IJCJ",
                    "mtime": 1360392090.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6205.CR2": {
                    "bytes": 24963961,
                    "id": "89DMFDLIEHEJQYXD6J4VMYAJGOB49I3S7COVQFMF7HTNXP3W",
                    "mtime": 1360392092.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6206.CR2": {
                    "bytes": 25149655,
                    "id": "SNJA75R8BXE6AACGYC55VH6LUQPBRWWW8XLYNXGCAIKMI4LM",
                    "mtime": 1360392092.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6207.CR2": {
                    "bytes": 24680671,
                    "id": "JIBBA4XQA4DA76CJI3KSBULPANXRBO7E9GI8Y3BLT57TQXDF",
                    "mtime": 1360392098.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6208.CR2": {
                    "bytes": 24714309,
                    "id": "DFKPEIY4O54BMVOJJK4PFC8HUFJIXA7ERW84Y5IE8K9EPSLK",
                    "mtime": 1360392100.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6209.CR2": {
                    "bytes": 24291205,
                    "id": "U6PH9P6CYX5DYO3KASNSARBAXV7TS8MKHX37EONJEK8MQM4A",
                    "mtime": 1360392108.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6210.CR2": {
                    "bytes": 25334550,
                    "id": "QIMK7DJ8XO6QBQROF9XCCMTNQVFQSD8UQY45YH9MSRA8LKL7",
                    "mtime": 1360392110.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6211.CR2": {
                    "bytes": 24624226,
                    "id": "KX9K9D8CK4RLCYILQ7S7X35YTOK8DU85PTYCOAAIKOX673WT",
                    "mtime": 1360392112.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6212.CR2": {
                    "bytes": 23494569,
                    "id": "HAG7YFCEKP77WOX74NOBQIVKCI8484KUSYLO7EWDFKWRO4KJ",
                    "mtime": 1360392114.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6213.CR2": {
                    "bytes": 24041795,
                    "id": "95WUQOG8N8P69MQ6XWS66ELP49VHJIHEY3P8PA3G6IS8E5WJ",
                    "mtime": 1360392118.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6214.CR2": {
                    "bytes": 24028923,
                    "id": "T8FAYB9REVFCQFYJCCXNP8JHO9LKRB8UD7E6KLE9GSRAYDOD",
                    "mtime": 1360392118.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6215.CR2": {
                    "bytes": 23913424,
                    "id": "STA8YD7Y8GI6DILU7BGRWD73GJJEX7S58SWTGW99YAEHCVBC",
                    "mtime": 1360392120.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6216.CR2": {
                    "bytes": 23845226,
                    "id": "HCQV63I787LWUUJQTFN9KJRT48D7488383QTVT38GRVLBN65",
                    "mtime": 1360392122.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6217.CR2": {
                    "bytes": 24640641,
                    "id": "S8HCBQHBYJV7EMCOMVHHK4R43F6JMP3PFJVPDF9KSO7IC73H",
                    "mtime": 1360568646.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6218.CR2": {
                    "bytes": 24200333,
                    "id": "5S34A6ASKKQECTMD4QCEFJLOLBAIA7ABFS4SDLMDQKTS4RI6",
                    "mtime": 1360568648.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6219.CR2": {
                    "bytes": 24402398,
                    "id": "QUSDCJNEXBOGPGTJRIGYIRIC3TMGB34I5QB9MDQTMT5C64MR",
                    "mtime": 1360568648.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6220.CR2": {
                    "bytes": 24593882,
                    "id": "LHRY5WM9Q4RYUYQOVOF797GTC3TBE3IBCR8BT4M5KDDQYBEF",
                    "mtime": 1360568654.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6221.CR2": {
                    "bytes": 24598745,
                    "id": "VD3PFGX79E6QVDUDYTCPLOYH5PVYTH8UTGU9COSXL6GM6B4R",
                    "mtime": 1360568656.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6222.CR2": {
                    "bytes": 24484112,
                    "id": "OTBJ98MDJ8VCUD8SBK68NDO3ML38VQC7KVGWA59J6YJ3NKFQ",
                    "mtime": 1360568660.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6223.CR2": {
                    "bytes": 24690330,
                    "id": "J4AHE9UHFTGX4P4HVIMK8MUQIMFES3B6LQWKKV8XET55Y6OO",
                    "mtime": 1360568660.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6224.CR2": {
                    "bytes": 24460827,
                    "id": "3LCFTF844RSRGQYVNOVIKER3NYCDVRMEQ647NLL4A7X9NHWE",
                    "mtime": 1360568662.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6225.CR2": {
                    "bytes": 24471116,
                    "id": "ODRKRMNG7QOYAE8Q5XM9EEJKYJIBF5JJAPPARJNQN8CAQARC",
                    "mtime": 1360568662.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6226.CR2": {
                    "bytes": 24603831,
                    "id": "QG58GHJVJOAYBPTARWPSXIGK4AJDIBDPMEKLBVIUKIPNLXJ8",
                    "mtime": 1360568664.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6227.CR2": {
                    "bytes": 24535757,
                    "id": "4COX64LWMA4CAR5KVNT8KVMHWU6NW8QAHNJ8VT7VYWP37MVT",
                    "mtime": 1360568664.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6228.CR2": {
                    "bytes": 24891215,
                    "id": "QCL7DIFUOEJCC6L797B8Y7SOFHNI577WDWP9A4DCQVQRVBIR",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6229.CR2": {
                    "bytes": 25199170,
                    "id": "OJOFDBRBM4X3UKLPK3UCORLOBCUVQ85MHH46LD8O7FMQOO4F",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6230.CR2": {
                    "bytes": 24789919,
                    "id": "A93BTX39A5MXILTP85DTTPBQK6M69PBWG3R3CFNGYB3YHKJE",
                    "mtime": 1360568666.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6231.CR2": {
                    "bytes": 24746424,
                    "id": "EP5FLHXQHQRFXB59LYSIXDMTYBXHIFABFPJPCC4IKIA9RQX9",
                    "mtime": 1360568668.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6232.CR2": {
                    "bytes": 24809460,
                    "id": "DFM6IAQAX9JISNPNENBTHUDL96DG93BRJ9O4MG8HMGHHE3AN",
                    "mtime": 1360568678.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6233.CR2": {
                    "bytes": 24699538,
                    "id": "KQYPHTAJWCUYQUT8BSVQ9IOY83EBOGDD8L6RJIMOSEJEPL3M",
                    "mtime": 1360568678.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6234.CR2": {
                    "bytes": 24802591,
                    "id": "HKQ53VFOM3M3E6DF69SIHADLD94BPC9XTL4HQYAMOUXH6OVK",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6235.CR2": {
                    "bytes": 24629902,
                    "id": "8GLBEM7UP83A98H7YS59WN66HSFV9S79OHKTI3X4O5QIQB7Q",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6236.CR2": {
                    "bytes": 24673202,
                    "id": "3CVGIMRGSQ7UJHAG8Q358S3BGQILUABJVIFHYTXEEVYEEHC5",
                    "mtime": 1360568680.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6237.CR2": {
                    "bytes": 24723757,
                    "id": "AYRUILMANIQXCLFHFCVWGFS6GB4Y67AEGQIUTKPEI4WAU984",
                    "mtime": 1360568682.0,
                    "status": "duplicate"
                },
                "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D/IMG_6238.CR2": {
                    "bytes": 24647627,
                    "id": "4PHQUF6FHYTOE6BWBUV6SGHNXANN6DLC6F5KCL6MXJPME7VB",
                    "mtime": 1360568682.0,
                    "status": "duplicate"
                }
            },
            "machine_id": "QRWKVZHH4SLHQAXL5XUT6JJGOVJRHERU7V66LHL6TRWMUJQF",
            "rate": "28.5 MB/s",
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
            },
            "statvfs": {
                "avail": 30735171584,
                "frsize": 32768,
                "readonly": False,
                "size": 32008667136,
                "used": 1273495552
            },
            "stores": {
                "/home/jderose/.local/share/dmedia": {
                    "copies": 1,
                    "id": "2F6NQJY6FZ2DZKSIWHRXBDPY"
                }
            },
            "time": 1361823238.1185207,
            "time_end": 1361823282.8363862,
            "type": "dmedia/import"
        })

    def test_migrate_log(self):
        old = {
           "_id": "ZRE2ISZBQDGQHSB3TAXSYG46",
           "_rev": "1-064b88773ccec534609d3c348440ad11",
           "batch_id": "3BYGSBKAN4UAFWKUH5QLFSF3",
           "bytes": 4108506,
           "dir": "/home/jderose/Pictures",
           "file_id": "ZRE2QKYESOXDO4BHTKUDGBWCQOTI5WFMHLDGR3G6TLKTHO47",
           "import_id": "SAN2XW3OHCFDTPUEE3MSU5LZ",
           "machine_id": "X6I6OU3ZJ3JJ4YU5JJU3UBLQQLVBQURY7OUQ4T3PM6DXTBMQ",
           "mtime": 1363403038,
           "name": "IMG_6306.JPG",
           "project_id": "6OKZCWZE76TVNMAY4PA4H2GU",
           "time": 1366942833.7158074,
           "type": "dmedia/log"
        }
        mdoc = {
            '_id': 'ZRE2QKYESOXDO4BHTKUDGBWCQOTI5WFMHLDGR3G6TLKTHO47',
            'v1_id': 'UCN49FVK5MG3SI44H4GMXVJ8NYD3IK8KVW6XA9NY46EWYKYV',
        }
        new = migration.migrate_log(old, mdoc)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
           "_id": "D8VXBVC4J69JAL4UM3QLR9VX",
           "batch_id": "U4R9L4D3GVN38PDNAWJE8L8U",
           "bytes": 4108506,
           "dir": "/home/jderose/Pictures",
           "file_id": "UCN49FVK5MG3SI44H4GMXVJ8NYD3IK8KVW6XA9NY46EWYKYV",
           "import_id": "L3GTQPUHA586MIN77UFLNWES",
           "mtime": 1363403038,
           "name": "IMG_6306.JPG",
           "project_id": "6OKZCWZE76TVNMAY4PA4H2GU",
           "time": 1366942833.7158074,
           "type": "dmedia/file/import"
        })

    def test_migrate_project_file(self):
        old = {
           "_id": "22G2ZBWVRNIZBESIIYDTJH5LA7BGLX3MY35LWUFGWFN23ZUI",
           "_rev": "1-fb16fdc76be4f470bd88b262997778e1",
           "batch_id": "HJZH6W4TCEFSJ22BN72TLI6Z",
           "bytes": 22926564,
           "content_type": "image/x-canon-cr2",
           "ctime": 1356480728.81,
           "dir": "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D",
           "ext": "cr2",
           "height": 3744,
           "import_id": "JYJPIJNU2SWVJ56O2RYWW4EY",
           "machine_id": "IJNVSWT3FR26CMNUR6NQAGP3H72DTO4APDSGFUG7GZO572NK",
           "media": "image",
           "meta": {
               "aperture": 1.2,
               "camera": "Canon EOS 5D Mark II",
               "camera_serial": "0820500998",
               "focal_length": "50.0 mm",
               "iso": 320,
               "lens": "Canon EF 50mm f/1.2L",
               "shutter": "1/100"
           },
           "name": "IMG_6003.CR2",
           "origin": "user",
           "tags": {
           },
           "time": 1356663921.1282482,
           "type": "dmedia/file",
           "width": 5616,
           "_attachments": {
               "thumbnail": {
                   "content_type": "image/jpeg",
                   "revpos": 1,
                   "digest": "md5-fSvWsFXovkU4m8hYbrfCrg==",
                   "length": 13940,
                   "stub": True
               }
           }
        }
        v1_id = 'PEBQES8UITQBMLIW9ILFJX9WUVI8X6G9NPPOEJJYOKUV3FOQ'
        new = migration.migrate_project_file(old, v1_id)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
           "_id": v1_id,
           "batch_id": migration.b32_to_db32(old['batch_id']),
           "bytes": 22926564,
           "content_type": "image/x-canon-cr2",
           "ctime": 1356480728.81,
           "dir": "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D",
           "ext": "cr2",
           "height": 3744,
           "import_id": migration.b32_to_db32(old['import_id']),
           "machine_id": "IJNVSWT3FR26CMNUR6NQAGP3H72DTO4APDSGFUG7GZO572NK",
           "media": "image",
           "meta": {
               "aperture": 1.2,
               "camera": "Canon EOS 5D Mark II",
               "camera_serial": "0820500998",
               "focal_length": "50.0 mm",
               "iso": 320,
               "lens": "Canon EF 50mm f/1.2L",
               "shutter": "1/100"
           },
           "name": "IMG_6003.CR2",
           "origin": "user",
           "tags": {
           },
           "time": 1356663921.1282482,
           "type": "dmedia/file",
           "width": 5616,
           "_attachments": {
               "thumbnail": {
                   "content_type": "image/jpeg",
                   "revpos": 1,
                   "digest": "md5-fSvWsFXovkU4m8hYbrfCrg==",
                   "length": 13940,
                   "stub": True
               }
           }
        })

        # Test when batch_id and/or import_id is missing:
        del old['batch_id']
        del old['import_id']
        new = migration.migrate_project_file(old, v1_id)
        self.assertIsNot(new, old)
        self.assertEqual(new, {
           "_id": v1_id,
           "bytes": 22926564,
           "content_type": "image/x-canon-cr2",
           "ctime": 1356480728.81,
           "dir": "/media/jderose/EOS_DIGITAL/DCIM/100EOS5D",
           "ext": "cr2",
           "height": 3744,
           "machine_id": "IJNVSWT3FR26CMNUR6NQAGP3H72DTO4APDSGFUG7GZO572NK",
           "media": "image",
           "meta": {
               "aperture": 1.2,
               "camera": "Canon EOS 5D Mark II",
               "camera_serial": "0820500998",
               "focal_length": "50.0 mm",
               "iso": 320,
               "lens": "Canon EF 50mm f/1.2L",
               "shutter": "1/100"
           },
           "name": "IMG_6003.CR2",
           "origin": "user",
           "tags": {
           },
           "time": 1356663921.1282482,
           "type": "dmedia/file",
           "width": 5616,
           "_attachments": {
               "thumbnail": {
                   "content_type": "image/jpeg",
                   "revpos": 1,
                   "digest": "md5-fSvWsFXovkU4m8hYbrfCrg==",
                   "length": 13940,
                   "stub": True
               }
           }
        })


class TestCouchFunctions(CouchTestCase):
    def test_iter_v0_project_dbs(self):
        server = Server(self.env)
        v0_dmedia = sorted(rfc3548.random_id() for i in range(20))
        v1_dmedia = sorted(dbase32.random_id() for i in range(20))
        v0_novacut = sorted(rfc3548.random_id() for i in range(20))
        v1_novacut = sorted(dbase32.random_id() for i in range(20))
        db_names = [
            'dmedia-0',
            'dmedia-1',
            'migrate-0-to-1',
            'novacut-0',
            'novacut-1',
            'thumbnails',
        ]
        for _id in v0_dmedia:
            db_names.append('dmedia-0-' + _id.lower())
        for _id in v1_dmedia:
            db_names.append('dmedia-1-' + _id.lower())
        for _id in v0_novacut:
            db_names.append('novacut-0-' + _id.lower())
        for _id in v1_novacut:
            db_names.append('novacut-1-' + _id.lower())
        for name in db_names:
            server.put(None, name)
        self.assertEqual(
            list(migration.iter_v0_project_dbs(server)),
            [('dmedia-0-' + _id.lower(), _id) for _id in v0_dmedia]
        )

