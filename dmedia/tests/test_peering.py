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
import subprocess

from microfiber import random_id

from .base import TempDir
from dmedia import peering


class TestSSLFunctions(TestCase):
    def test_create_key(self):
        tmp = TempDir()
        key = tmp.join('key.pem')

        # bits=1024
        sizes = [883, 887, 891]
        peering.create_key(key, bits=1024)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        # bits=2048 (default)
        sizes = [1671, 1675, 1679]
        peering.create_key(key)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        peering.create_key(key, bits=2048)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        # bits=3072
        sizes = [2455, 2459]
        peering.create_key(key, bits=3072)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)

    def test_create_ca(self):
        tmp = TempDir()
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        peering.create_key(foo_key)
        self.assertFalse(path.exists(foo_ca))
        peering.create_ca(foo_key, '/CN=foo', foo_ca)
        self.assertGreater(path.getsize(foo_ca), 0)

    def test_create_csr(self):
        tmp = TempDir()
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        peering.create_key(bar_key)
        self.assertFalse(path.exists(bar_csr))
        peering.create_csr(bar_key, '/CN=bar', bar_csr)
        self.assertGreater(path.getsize(bar_csr), 0)

    def test_issue_cert(self):
        tmp = TempDir()

        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        peering.create_key(foo_key)
        peering.create_ca(foo_key, '/CN=foo', foo_ca)

        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        peering.create_key(bar_key)
        peering.create_csr(bar_key, '/CN=bar', bar_csr)

        files = (foo_srl, bar_cert)
        for f in files:
            self.assertFalse(path.exists(f))
        peering.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)
        for f in files:
            self.assertGreater(path.getsize(f), 0)

    def test_get_pubkey(self):
        tmp = TempDir()

        # Create CA
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        peering.create_key(foo_key)
        foo_pubkey = peering.get_rsa_pubkey(foo_key)
        peering.create_ca(foo_key, '/CN=foo', foo_ca)

        # Create CSR and issue cert
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        peering.create_key(bar_key)
        bar_pubkey = peering.get_rsa_pubkey(bar_key)
        peering.create_csr(bar_key, '/CN=bar', bar_csr)
        peering.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)

        # Now compare
        os.remove(foo_key)
        os.remove(bar_key)
        self.assertEqual(peering.get_pubkey(foo_ca), foo_pubkey)
        self.assertEqual(peering.get_csr_pubkey(bar_csr), bar_pubkey)
        self.assertEqual(peering.get_pubkey(bar_cert), bar_pubkey)

    def test_get_subject(self):
        tmp = TempDir()

        foo_subject = '/CN={}'.format(random_id(30))
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        peering.create_key(foo_key)
        peering.create_ca(foo_key, foo_subject, foo_ca)
        self.assertEqual(peering.get_subject(foo_ca), foo_subject)

        bar_subject = '/CN={}'.format(random_id(30))
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        peering.create_key(bar_key)
        peering.create_csr(bar_key, bar_subject, bar_csr)
        peering.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)
        self.assertEqual(peering.get_csr_subject(bar_csr), bar_subject)
        self.assertEqual(peering.get_subject(bar_cert), bar_subject)

    def test_get_csr_subject(self):
        tmp = TempDir()
        subject = '/CN={}'.format(random_id(30))
        key_file = tmp.join('foo.key')
        csr_file = tmp.join('foo.csr')
        peering.create_key(key_file)
        peering.create_csr(key_file, subject, csr_file)
        os.remove(key_file)
        self.assertEqual(peering.get_csr_subject(csr_file), subject)


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
        _id = pki.create_key()
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', _id + '.key'])
        )
        key_file = path.join(pki.ssldir, _id + '.key')
        data = peering.get_rsa_pubkey(key_file)
        self.assertEqual(_id, peering.hash_pubkey(data))
    
    def test_verify_key(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        id1 = pki.create_key()
        key1_file = tmp.join(id1 + '.key')
        id2 = pki.create_key()
        key2_file = tmp.join(id2 + '.key')
        self.assertEqual(pki.verify_key(id1), key1_file)
        self.assertEqual(pki.verify_key(id2), key2_file)
        os.remove(key1_file)
        os.rename(key2_file, key1_file)
        with self.assertRaises(peering.PublicKeyError) as cm:
            pki.verify_key(id1)
        self.assertEqual(cm.exception.id, id1)
        self.assertEqual(cm.exception.filename, key1_file)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_key(id2)

    def test_create_ca(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        _id = pki.create_key()
        ca_file = tmp.join(_id + '.ca')
        self.assertFalse(path.exists(ca_file))
        self.assertEqual(pki.create_ca(_id), ca_file)
        self.assertTrue(path.isfile(ca_file))
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', _id + '.key', _id + '.ca'])
        )

    def test_verify_ca(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        id1 = pki.create_key()
        id2 = pki.create_key()
        ca1_file = pki.create_ca(id1)
        ca2_file = pki.create_ca(id2)
        os.remove(tmp.join(id1 + '.key'))
        os.remove(tmp.join(id2 + '.key'))
        self.assertEqual(pki.verify_ca(id1), ca1_file)
        self.assertEqual(pki.verify_ca(id2), ca2_file)
        os.remove(ca1_file)
        os.rename(ca2_file, ca1_file)
        with self.assertRaises(peering.PublicKeyError) as cm:
            pki.verify_ca(id1)
        self.assertEqual(cm.exception.id, id1)
        self.assertEqual(cm.exception.filename, ca1_file)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_ca(id2)

    def test_create_csr(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        _id = pki.create_key()
        csr_file = tmp.join(_id + '.csr')
        self.assertFalse(path.exists(csr_file))
        self.assertEqual(pki.create_csr(_id), csr_file)
        self.assertTrue(path.isfile(csr_file))
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', _id + '.key', _id + '.csr'])
        )

    def test_verify_csr(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)
        id1 = pki.create_key()
        id2 = pki.create_key()
        csr1_file = pki.create_csr(id1)
        csr2_file = pki.create_csr(id2)
        os.remove(tmp.join(id1 + '.key'))
        os.remove(tmp.join(id2 + '.key'))
        self.assertEqual(pki.verify_csr(id1), csr1_file)
        self.assertEqual(pki.verify_csr(id2), csr2_file)
        os.remove(csr1_file)
        os.rename(csr2_file, csr1_file)
        with self.assertRaises(peering.PublicKeyError) as cm:
            pki.verify_csr(id1)
        self.assertEqual(cm.exception.id, id1)
        self.assertEqual(cm.exception.filename, csr1_file)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_csr(id2)

    def test_issue_cert(self):
        tmp = TempDir()
        pki = peering.PKI(tmp.dir)

        # Create the CA
        ca_id = pki.create_key()
        pki.create_ca(ca_id)

        # Create the CSR
        cert_id = pki.create_key()
        pki.create_csr(cert_id)
        os.remove(tmp.join(cert_id + '.key'))

        # Now test
        cert_file = tmp.join(cert_id + '.cert')
        self.assertFalse(path.exists(cert_file))
        self.assertEqual(pki.issue_cert(cert_id, ca_id), cert_file)
        self.assertGreater(path.getsize(cert_file), 0)
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set([
                'tmp',
                ca_id + '.key',
                ca_id + '.ca',
                ca_id + '.srl',
                cert_id + '.csr',
                cert_id + '.cert',
            ])
        )

