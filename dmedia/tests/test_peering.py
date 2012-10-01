# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
Unit tests for `dmedia.peering`.
"""

from unittest import TestCase
import os
from os import path

from usercouch import sslhelpers
from microfiber import random_id

from .base import TempDir
from dmedia import peering


class TestPKI(TestCase):
    def test_init(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        self.assertIs(pki.ssldir, tmp.dir)
        self.assertEqual(pki.tmpdir, tmp.join('tmp'))

        # Test when tmpdir already exists
        pki = peering.PKI(tmp.dir)

    def test_tmp_path(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        tmp_id = random_id()
        self.assertEqual(
            pki.tmp_path(tmp_id, 'key'),
            tmp.join('tmp', tmp_id + '.key')
        )
        self.assertEqual(
            pki.tmp_path(tmp_id, 'cert'),
            tmp.join('tmp', tmp_id + '.cert')
        )
        self.assertEqual(
            pki.tmp_path(tmp_id, 'csr'),
            tmp.join('tmp', tmp_id + '.csr')
        )

    def test_tmp_files(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        tmp_id = random_id()
        files = pki.tmp_files(tmp_id)
        self.assertIsInstance(files, peering.TmpFiles)
        self.assertEqual(
            files.key,
            tmp.join('tmp', tmp_id + '.key')
        )
        self.assertEqual(
            files.cert,
            tmp.join('tmp', tmp_id + '.cert')
        )
        self.assertEqual(
            files.csr,
            tmp.join('tmp', tmp_id + '.csr')
        )
        self.assertEqual(files,
            (files.key, files.cert, files.csr)
        )

    def test_path(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        cert_id = random_id(25)
        self.assertEqual(
            pki.path(cert_id, 'key'),
            tmp.join(cert_id + '.key')
        )
        self.assertEqual(
            pki.path(cert_id, 'cert'),
            tmp.join(cert_id + '.cert')
        )
        self.assertEqual(
            pki.path(cert_id, 'srl'),
            tmp.join(cert_id + '.srl')
        )

    def test_files(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        cert_id = random_id(25)
        files = pki.files(cert_id)
        self.assertIsInstance(files, peering.Files)
        self.assertEqual(
            files.key,
            tmp.join(cert_id + '.key')
        )
        self.assertEqual(
            files.cert,
            tmp.join(cert_id + '.cert')
        )
        self.assertEqual(files, (files.key, files.cert))

    def test_ca_files(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        cert_id = random_id(25)
        files = pki.ca_files(cert_id)
        self.assertIsInstance(files, peering.CAFiles)
        self.assertEqual(
            files.key,
            tmp.join(cert_id + '.key')
        )
        self.assertEqual(
            files.cert,
            tmp.join(cert_id + '.cert')
        )
        self.assertEqual(
            files.srl,
            tmp.join(cert_id + '.srl')
        )
        self.assertEqual(files,
            (files.key, files.cert, files.srl)
        )

    def test_create_key(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        cn = pki.create_key()
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', cn + '.key'])
        )
        key_file = path.join(pki.ssldir, cn + '.key')
        data = sslhelpers.get_pubkey(key_file)
        self.assertEqual(cn, peering.hash_pubkey(data))

    def test_create(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        tmp_id = random_id()
        cert_id = pki.create(tmp_id)
        key_file = pki.path(cert_id, 'key')
        cert_file = pki.path(cert_id, 'cert')
        self.assertGreater(path.getsize(key_file), 0)
        self.assertGreater(path.getsize(cert_file), 0)
        cert_data = open(cert_file, 'rb').read()
        self.assertEqual(peering.hash_cert(cert_data), cert_id)
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', cert_id + '.cert', cert_id + '.key'])
        )

    def test_create_csr(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        tmp_id = random_id()
        pki.create_csr(tmp_id)
        key_file = pki.tmp_path(tmp_id, 'key')
        csr_file = pki.tmp_path(tmp_id, 'csr')
        self.assertGreater(path.getsize(key_file), 0)
        self.assertGreater(path.getsize(csr_file), 0)
        self.assertEqual(os.listdir(pki.ssldir), ['tmp'])
        self.assertEqual(
            set(os.listdir(pki.tmpdir)),
            set([tmp_id + '.csr', tmp_id + '.key'])
        )

    def test_issue(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)

        # Create CA
        ca_id = pki.create(random_id())

        # Create CSR
        tmp_id = random_id()
        pki.create_csr(tmp_id)

        # Now test PKI.issue()
        cert_id = pki.issue(tmp_id, ca_id)
        cert_file = pki.path(cert_id, 'cert')
        cert_data = open(cert_file, 'rb').read()
        self.assertEqual(peering.hash_cert(cert_data), cert_id)

        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set([
                'tmp',
                ca_id + '.key',
                ca_id + '.cert',
                ca_id + '.srl',
                cert_id + '.key',
                cert_id + '.cert',
                cert_id + '.csr',
            ])
        )

        # Now try it all over again, this time when the tmp_key_file doesn't
        # exist:
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        ca_id = pki.create(random_id())
        tmp_id = random_id()
        pki.create_csr(tmp_id)
        os.remove(pki.tmp_path(tmp_id, 'key'))
        cert_id = pki.issue(tmp_id, ca_id)
        cert_file = pki.path(cert_id, 'cert')
        cert_data = open(cert_file, 'rb').read()
        self.assertEqual(peering.hash_cert(cert_data), cert_id)
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set([
                'tmp',
                ca_id + '.key',
                ca_id + '.cert',
                ca_id + '.srl',
                cert_id + '.cert',
                cert_id + '.csr',
            ])
        )

