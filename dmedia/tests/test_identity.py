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
Unit tests for `dmedia.identity`.
"""

from unittest import TestCase
import os
from os import path, urandom
import subprocess

from dbase32 import db32dec, random_id
from skein import skein512

from .base import TempDir
from dmedia import identity


class TestSkeinFunctions(TestCase):
    def test_hash_pubkey(self):
        data = urandom(500)
        _id = identity.hash_pubkey(data)
        self.assertIsInstance(_id, str)
        self.assertEqual(len(_id), 48)
        skein = skein512(data,
            digest_bits=240,
            pers=b'20120918 jderose@novacut.com dmedia/pubkey',
        )
        self.assertEqual(
            db32dec(_id),
            skein.digest()
        )

        # Sanity check
        for i in range(1000):
            self.assertNotEqual(_id,
                identity.hash_pubkey(urandom(500)),
            )

    def test_compute_response(self):
        secret = urandom(5)
        challenge = urandom(20)
        nonce = urandom(20)
        hash1 = urandom(30)
        hash2 = urandom(30)
        response = identity.compute_response(
            secret, challenge, nonce, hash1, hash2
        )
        self.assertIsInstance(response, str)
        self.assertEqual(len(response), 56)
        skein = skein512(hash1 + hash2,
            digest_bits=280,
            pers=b'20120918 jderose@novacut.com dmedia/response',
            key=secret,
            nonce=(challenge + nonce),
        )
        self.assertEqual(
            db32dec(response),
            skein.digest()
        )

        # Test with direction reversed
        self.assertNotEqual(response,
            identity.compute_response(secret, challenge, nonce, hash2, hash1)
        )

        # Test with wrong secret
        for i in range(100):
            self.assertNotEqual(response,
                identity.compute_response(urandom(5), challenge, nonce, hash1, hash2)
            )

        # Test with wrong challange
        for i in range(100):
            self.assertNotEqual(response,
                identity.compute_response(secret, urandom(20), nonce, hash1, hash2)
            )

        # Test with wrong nonce
        for i in range(100):
            self.assertNotEqual(response,
                identity.compute_response(secret, challenge, urandom(20), hash1, hash2)
            )

        # Test with wrong challenger_hash
        for i in range(100):
            self.assertNotEqual(response,
                identity.compute_response(secret, challenge, nonce, urandom(30), hash2)
            )

        # Test with wrong responder_hash
        for i in range(100):
            self.assertNotEqual(response,
                identity.compute_response(secret, challenge, nonce, hash1, urandom(30))
            )

    def test_compute_csr_mac(self):
        secret = os.urandom(5)
        remote_hash = os.urandom(30)
        local_hash = os.urandom(30)
        csr_data = os.urandom(500)
        mac = identity.compute_csr_mac(secret, csr_data, remote_hash, local_hash)
        self.assertIsInstance(mac, str)
        self.assertEqual(len(mac), 56)
        skein = skein512(csr_data,
            digest_bits=280,
            pers=b'20120918 jderose@novacut.com dmedia/csr',
            key=secret,
            key_id=(remote_hash + local_hash),
        )
        self.assertEqual(
            db32dec(mac),
            skein.digest()
        )

        # Test with direction reversed
        self.assertNotEqual(mac,
            identity.compute_csr_mac(secret, csr_data, local_hash, remote_hash)
        )

        # Test with wrong secret
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_csr_mac(os.urandom(5), csr_data, remote_hash, local_hash)
            )

        # Test with wrong cert_data
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_csr_mac(secret, os.urandom(500), remote_hash, local_hash)
            )

        # Test with wrong remote_hash
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_csr_mac(secret, csr_data, os.urandom(30), local_hash)
            )

        # Test with wrong local_hash
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_csr_mac(secret, csr_data, remote_hash, os.urandom(30))
            )

    def test_compute_cert_mac(self):
        secret = os.urandom(5)
        remote_hash = os.urandom(30)
        local_hash = os.urandom(30)
        cert_data = os.urandom(500)
        mac = identity.compute_cert_mac(secret, cert_data, remote_hash, local_hash)
        self.assertIsInstance(mac, str)
        self.assertEqual(len(mac), 56)
        skein = skein512(cert_data,
            digest_bits=280,
            pers=b'20120918 jderose@novacut.com dmedia/cert',
            key=secret,
            key_id=(remote_hash + local_hash),
        )
        self.assertEqual(
            db32dec(mac),
            skein.digest()
        )

        # Test with direction reversed
        self.assertNotEqual(mac,
            identity.compute_cert_mac(secret, cert_data, local_hash, remote_hash)
        )

        # Test with wrong secret
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_cert_mac(os.urandom(5), cert_data, remote_hash, local_hash)
            )

        # Test with wrong cert_data
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_cert_mac(secret, os.urandom(500), remote_hash, local_hash)
            )

        # Test with wrong remote_hash
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_cert_mac(secret, cert_data, os.urandom(30), local_hash)
            )

        # Test with wrong local_hash
        for i in range(100):
            self.assertNotEqual(mac,
                identity.compute_cert_mac(secret, cert_data, remote_hash, os.urandom(30))
            )


class TestChallengeResponse(TestCase):
    def test_init(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        self.assertIs(inst.id, id1)
        self.assertIs(inst.peer_id, id2)
        self.assertEqual(inst.local_hash, identity.db32dec(id1))
        self.assertEqual(inst.remote_hash, identity.db32dec(id2))

    def test_get_secret(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        s1 = inst.get_secret()
        self.assertIsInstance(s1, str)
        self.assertEqual(len(s1), 8)
        self.assertEqual(identity.db32dec(s1), inst.secret)
        s2 = inst.get_secret()
        self.assertNotEqual(s1, s2)
        self.assertIsInstance(s2, str)
        self.assertEqual(len(s2), 8)
        self.assertEqual(identity.db32dec(s2), inst.secret)

    def test_set_secret(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        s1 = random_id(5)
        self.assertIsNone(inst.set_secret(s1))
        self.assertEqual(identity.db32enc(inst.secret), s1)
        s2 = random_id(5)
        self.assertIsNone(inst.set_secret(s2))
        self.assertEqual(identity.db32enc(inst.secret), s2)

    def test_get_challenge(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        c1 = inst.get_challenge()
        self.assertIsInstance(c1, str)
        self.assertEqual(len(c1), 32)
        self.assertEqual(identity.db32dec(c1), inst.challenge)
        c2 = inst.get_challenge()
        self.assertNotEqual(c1, c2)
        self.assertIsInstance(c2, str)
        self.assertEqual(len(c2), 32)
        self.assertEqual(identity.db32dec(c2), inst.challenge)

    def test_create_response(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        local_hash = db32dec(id1)
        remote_hash = db32dec(id2)
        secret1 = random_id(5)
        challenge1 = random_id(20)
        inst.set_secret(secret1)
        (nonce1, response1) = inst.create_response(challenge1)
        self.assertIsInstance(nonce1, str)
        self.assertEqual(len(nonce1), 32)
        self.assertIsInstance(response1, str)
        self.assertEqual(len(response1), 56)
        self.assertEqual(response1,
            identity.compute_response(
                db32dec(secret1), db32dec(challenge1), db32dec(nonce1),
                remote_hash, local_hash
            )
        )

        # Same secret and challenge, make sure a new nonce is used
        (nonce2, response2) = inst.create_response(challenge1)
        self.assertNotEqual(nonce2, nonce1)
        self.assertNotEqual(response2, response1)
        self.assertIsInstance(nonce2, str)
        self.assertEqual(len(nonce2), 32)
        self.assertIsInstance(response2, str)
        self.assertEqual(len(response2), 56)
        self.assertEqual(response2,
            identity.compute_response(
                db32dec(secret1), db32dec(challenge1), db32dec(nonce2),
                remote_hash, local_hash
            )
        )

        # Different secret
        secret2 = random_id(5)
        inst.set_secret(secret2)
        (nonce3, response3) = inst.create_response(challenge1)
        self.assertNotEqual(nonce3, nonce1)
        self.assertNotEqual(response3, response1)
        self.assertNotEqual(nonce3, nonce2)
        self.assertNotEqual(response3, response2)
        self.assertIsInstance(nonce3, str)
        self.assertEqual(len(nonce3), 32)
        self.assertIsInstance(response3, str)
        self.assertEqual(len(response3), 56)
        self.assertEqual(response3,
            identity.compute_response(
                db32dec(secret2), db32dec(challenge1), db32dec(nonce3),
                remote_hash, local_hash
            )
        )

        # Different challenge
        challenge2 = random_id(20)
        (nonce4, response4) = inst.create_response(challenge2)
        self.assertNotEqual(nonce4, nonce1)
        self.assertNotEqual(response4, response1)
        self.assertNotEqual(nonce4, nonce2)
        self.assertNotEqual(response4, response2)
        self.assertNotEqual(nonce4, nonce3)
        self.assertNotEqual(response4, response3)
        self.assertIsInstance(nonce4, str)
        self.assertEqual(len(nonce4), 32)
        self.assertIsInstance(response4, str)
        self.assertEqual(len(response4), 56)
        self.assertEqual(response4,
            identity.compute_response(
                db32dec(secret2), db32dec(challenge2), db32dec(nonce4),
                remote_hash, local_hash
            )
        )

    def test_check_response(self):
        id1 = random_id(30)
        id2 = random_id(30)
        inst = identity.ChallengeResponse(id1, id2)
        local_hash = db32dec(id1)
        remote_hash = db32dec(id2)
        secret = inst.get_secret()
        challenge = inst.get_challenge()
        nonce = random_id(20)
        response = identity.compute_response(
            db32dec(secret), db32dec(challenge), db32dec(nonce),
            local_hash, remote_hash
        )
        self.assertIsNone(inst.check_response(nonce, response))

        # Test with (local, remote) order flipped
        bad = identity.compute_response(
            db32dec(secret), db32dec(challenge), db32dec(nonce),
            remote_hash, local_hash
        )
        with self.assertRaises(identity.WrongResponse) as cm:
            inst.check_response(nonce, bad)
        self.assertEqual(cm.exception.expected, response)
        self.assertEqual(cm.exception.got, bad)
        self.assertFalse(hasattr(inst, 'secret'))
        self.assertFalse(hasattr(inst, 'challenge'))
        inst.secret = db32dec(secret)
        inst.challenge = db32dec(challenge)

        # Test with wrong secret
        for i in range(100):
            bad = identity.compute_response(
                os.urandom(5), db32dec(challenge), db32dec(nonce),
                local_hash, remote_hash
            )
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, bad)
            self.assertEqual(cm.exception.expected, response)
            self.assertEqual(cm.exception.got, bad)
            self.assertFalse(hasattr(inst, 'secret'))
            self.assertFalse(hasattr(inst, 'challenge'))
            inst.secret = db32dec(secret)
            inst.challenge = db32dec(challenge)

        # Test with wrong challenge
        for i in range(100):
            bad = identity.compute_response(
                db32dec(secret), os.urandom(20), db32dec(nonce),
                local_hash, remote_hash
            )
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, bad)
            self.assertEqual(cm.exception.expected, response)
            self.assertEqual(cm.exception.got, bad)
            self.assertFalse(hasattr(inst, 'secret'))
            self.assertFalse(hasattr(inst, 'challenge'))
            inst.secret = db32dec(secret)
            inst.challenge = db32dec(challenge)

        # Test with wrong nonce
        for i in range(100):
            bad = identity.compute_response(
                db32dec(secret), db32dec(challenge), os.urandom(20),
                local_hash, remote_hash
            )
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, bad)
            self.assertEqual(cm.exception.expected, response)
            self.assertEqual(cm.exception.got, bad)
            self.assertFalse(hasattr(inst, 'secret'))
            self.assertFalse(hasattr(inst, 'challenge'))
            inst.secret = db32dec(secret)
            inst.challenge = db32dec(challenge)

        # Test with wrong local_hash
        for i in range(100):
            bad = identity.compute_response(
                db32dec(secret), db32dec(challenge), db32dec(nonce),
                os.urandom(30), remote_hash
            )
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, bad)
            self.assertEqual(cm.exception.expected, response)
            self.assertEqual(cm.exception.got, bad)
            self.assertFalse(hasattr(inst, 'secret'))
            self.assertFalse(hasattr(inst, 'challenge'))
            inst.secret = db32dec(secret)
            inst.challenge = db32dec(challenge)

        # Test with wrong remote_hash
        for i in range(100):
            bad = identity.compute_response(
                db32dec(secret), db32dec(challenge), db32dec(nonce),
                local_hash, os.urandom(30)
            )
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, bad)
            self.assertEqual(cm.exception.expected, response)
            self.assertEqual(cm.exception.got, bad)
            self.assertFalse(hasattr(inst, 'secret'))
            self.assertFalse(hasattr(inst, 'challenge'))
            inst.secret = db32dec(secret)
            inst.challenge = db32dec(challenge)

        # Test with more nonce, used as expected:
        for i in range(100):
            newnonce = random_id(20)
            good = identity.compute_response(
                db32dec(secret), db32dec(challenge), db32dec(newnonce),
                local_hash, remote_hash
            )
            self.assertNotEqual(good, response)
            self.assertIsNone(inst.check_response(newnonce, good))

        # Sanity check on directionality, in other words, check that the
        # response created locally can't accidentally be verified as the
        # response from the other end
        secret = random_id(5)
        for i in range(1000):
            inst.set_secret(secret)
            challenge = inst.get_challenge()
            (nonce, response) = inst.create_response(challenge)
            with self.assertRaises(identity.WrongResponse) as cm:
                inst.check_response(nonce, response)


class TestSSLFunctions(TestCase):
    def test_create_key(self):
        tmp = TempDir()
        key = tmp.join('key.pem')

        # bits=1024
        sizes = [883, 887, 891]
        identity.create_key(key, bits=1024)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        # bits=2048 (default)
        sizes = [1671, 1675, 1679]
        identity.create_key(key)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        identity.create_key(key, bits=2048)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)
        os.remove(key)

        # bits=3072
        sizes = [2455, 2459]
        identity.create_key(key, bits=3072)
        self.assertLess(min(sizes) - 25, path.getsize(key))
        self.assertLess(path.getsize(key), max(sizes) + 25)

    def test_create_ca(self):
        tmp = TempDir()
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        identity.create_key(foo_key)
        self.assertFalse(path.exists(foo_ca))
        identity.create_ca(foo_key, '/CN=foo', foo_ca)
        self.assertGreater(path.getsize(foo_ca), 0)

    def test_create_csr(self):
        tmp = TempDir()
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        identity.create_key(bar_key)
        self.assertFalse(path.exists(bar_csr))
        identity.create_csr(bar_key, '/CN=bar', bar_csr)
        self.assertGreater(path.getsize(bar_csr), 0)

    def test_issue_cert(self):
        tmp = TempDir()

        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        identity.create_key(foo_key)
        identity.create_ca(foo_key, '/CN=foo', foo_ca)

        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        identity.create_key(bar_key)
        identity.create_csr(bar_key, '/CN=bar', bar_csr)

        files = (foo_srl, bar_cert)
        for f in files:
            self.assertFalse(path.exists(f))
        identity.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)
        for f in files:
            self.assertGreater(path.getsize(f), 0)

    def test_get_pubkey(self):
        tmp = TempDir()

        # Create CA
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        identity.create_key(foo_key)
        foo_pubkey = identity.get_rsa_pubkey(foo_key)
        identity.create_ca(foo_key, '/CN=foo', foo_ca)

        # Create CSR and issue cert
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        identity.create_key(bar_key)
        bar_pubkey = identity.get_rsa_pubkey(bar_key)
        identity.create_csr(bar_key, '/CN=bar', bar_csr)
        identity.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)

        # Now compare
        os.remove(foo_key)
        os.remove(bar_key)
        self.assertEqual(identity.get_pubkey(foo_ca), foo_pubkey)
        self.assertEqual(identity.get_csr_pubkey(bar_csr), bar_pubkey)
        self.assertEqual(identity.get_pubkey(bar_cert), bar_pubkey)

    def test_get_subject(self):
        tmp = TempDir()

        foo_subject = '/CN={}'.format(random_id(30))
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        identity.create_key(foo_key)
        identity.create_ca(foo_key, foo_subject, foo_ca)
        self.assertEqual(identity.get_subject(foo_ca), foo_subject)

        bar_subject = '/CN={}'.format(random_id(30))
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        identity.create_key(bar_key)
        identity.create_csr(bar_key, bar_subject, bar_csr)
        identity.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)
        self.assertEqual(identity.get_csr_subject(bar_csr), bar_subject)
        self.assertEqual(identity.get_subject(bar_cert), bar_subject)

    def test_get_csr_subject(self):
        tmp = TempDir()
        subject = '/CN={}'.format(random_id(30))
        key_file = tmp.join('foo.key')
        csr_file = tmp.join('foo.csr')
        identity.create_key(key_file)
        identity.create_csr(key_file, subject, csr_file)
        os.remove(key_file)
        self.assertEqual(identity.get_csr_subject(csr_file), subject)

    def test_get_issuer(self):
        tmp = TempDir()

        foo_subject = '/CN={}'.format(random_id(30))
        foo_key = tmp.join('foo.key')
        foo_ca = tmp.join('foo.ca')
        foo_srl = tmp.join('foo.srl')
        identity.create_key(foo_key)
        identity.create_ca(foo_key, foo_subject, foo_ca)
        self.assertEqual(identity.get_issuer(foo_ca), foo_subject)

        bar_subject = '/CN={}'.format(random_id(30))
        bar_key = tmp.join('bar.key')
        bar_csr = tmp.join('bar.csr')
        bar_cert = tmp.join('bar.cert')
        identity.create_key(bar_key)
        identity.create_csr(bar_key, bar_subject, bar_csr)
        identity.issue_cert(bar_csr, foo_ca, foo_key, foo_srl, bar_cert)
        self.assertEqual(identity.get_csr_subject(bar_csr), bar_subject)
        self.assertEqual(identity.get_issuer(bar_cert), foo_subject)

    def test_ssl_verify(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)

        ca1 = pki.create_key()
        pki.create_ca(ca1)
        cert1 = pki.create_key()
        pki.create_csr(cert1)
        pki.issue_cert(cert1, ca1)
        ca1_file = pki.path(ca1, 'ca')
        cert1_file = pki.path(cert1, 'cert')
        self.assertEqual(identity.ssl_verify(ca1_file, ca1_file), ca1_file)
        self.assertEqual(identity.ssl_verify(cert1_file, ca1_file), cert1_file)
        with self.assertRaises(subprocess.CalledProcessError):
            identity.ssl_verify(ca1_file, cert1_file)

        ca2 = pki.create_key()
        pki.create_ca(ca2)
        cert2 = pki.create_key()
        pki.create_csr(cert2)
        pki.issue_cert(cert2, ca2)
        ca2_file = pki.path(ca2, 'ca')
        cert2_file = pki.path(cert2, 'cert')
        self.assertEqual(identity.ssl_verify(ca2_file, ca2_file), ca2_file)
        self.assertEqual(identity.ssl_verify(cert2_file, ca2_file), cert2_file)
        with self.assertRaises(subprocess.CalledProcessError):
            identity.ssl_verify(ca2_file, cert2_file)

        with self.assertRaises(subprocess.CalledProcessError):
            identity.ssl_verify(ca2_file, ca1_file)


class TestPKI(TestCase):
    def test_init(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        self.assertIs(pki.ssldir, tmp.dir)
        self.assertEqual(pki.tmpdir, tmp.join('tmp'))

        # Test when tmpdir already exists
        pki = identity.PKI(tmp.dir)

    def test_random_tmp(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        filename = pki.random_tmp()
        self.assertEqual(path.dirname(filename), tmp.join('tmp'))
        self.assertEqual(len(path.basename(filename)), 24)

    def test_path(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
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

    def test_create_key(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        _id = pki.create_key()
        self.assertEqual(os.listdir(pki.tmpdir), [])
        self.assertEqual(
            set(os.listdir(pki.ssldir)),
            set(['tmp', _id + '.key'])
        )
        key_file = path.join(pki.ssldir, _id + '.key')
        data = identity.get_rsa_pubkey(key_file)
        self.assertEqual(_id, identity.hash_pubkey(data))

    def test_verify_key(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        id1 = pki.create_key()
        key1_file = tmp.join(id1 + '.key')
        id2 = pki.create_key()
        key2_file = tmp.join(id2 + '.key')
        self.assertEqual(pki.verify_key(id1), key1_file)
        self.assertEqual(pki.verify_key(id2), key2_file)
        os.remove(key1_file)
        os.rename(key2_file, key1_file)
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.verify_key(id1)
        self.assertEqual(cm.exception.filename, key1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_key(id2)

    def test_read_key(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        id1 = pki.create_key()
        key1_file = tmp.join(id1 + '.key')
        data1 = open(key1_file, 'rb').read()
        id2 = pki.create_key()
        key2_file = tmp.join(id2 + '.key')
        data2 = open(key2_file, 'rb').read()
        self.assertEqual(pki.read_key(id1), data1)
        self.assertEqual(pki.read_key(id2), data2)
        os.remove(key1_file)
        os.rename(key2_file, key1_file)
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.read_key(id1)
        self.assertEqual(cm.exception.filename, key1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.read_key(id2)

    def test_write_key(self):
        tmp1 = TempDir()
        src = identity.PKI(tmp1.dir)
        tmp2 = TempDir()
        dst = identity.PKI(tmp2.dir)

        id1 = src.create_key()
        data1 = open(src.verify_key(id1), 'rb').read()
        id2 = src.create_key()
        data2 = open(src.verify_key(id2), 'rb').read()

        with self.assertRaises(identity.PublicKeyError) as cm:
            dst.write_key(id1, data2)
        self.assertEqual(path.dirname(cm.exception.filename), dst.tmpdir)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)

        with self.assertRaises(identity.PublicKeyError) as cm:
            dst.write_key(id2, data1)
        self.assertEqual(path.dirname(cm.exception.filename), dst.tmpdir)
        self.assertEqual(cm.exception.expected, id2)
        self.assertEqual(cm.exception.got, id1)

        self.assertEqual(dst.write_key(id1, data1), dst.path(id1, 'key'))
        self.assertEqual(open(dst.path(id1, 'key'), 'rb').read(), data1)

        self.assertEqual(dst.write_key(id2, data2), dst.path(id2, 'key'))
        self.assertEqual(open(dst.path(id2, 'key'), 'rb').read(), data2)

    def test_create_ca(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
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
        pki = identity.PKI(tmp.dir)
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
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.verify_ca(id1)
        self.assertEqual(cm.exception.filename, ca1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_ca(id2)

        # Test with bad subject
        id3 = pki.create_key()
        key_file = pki.path(id3, 'key')
        ca_file = pki.path(id3, 'ca')
        identity.create_ca(key_file, '/CN={}'.format(id1), ca_file)
        with self.assertRaises(identity.SubjectError) as cm:
            pki.verify_ca(id3)
        self.assertEqual(cm.exception.filename, ca_file)
        self.assertEqual(cm.exception.expected, '/CN={}'.format(id3))
        self.assertEqual(cm.exception.got, '/CN={}'.format(id1))

        # Test with bad issuer
        pki.create_ca(id3)
        id4 = pki.create_key()
        pki.create_csr(id4)
        pki.issue_cert(id4, id3)
        os.rename(pki.path(id4, 'cert'), pki.path(id4, 'ca'))
        with self.assertRaises(identity.IssuerError) as cm:
            pki.verify_ca(id4)
        self.assertEqual(cm.exception.filename, pki.path(id4, 'ca'))
        self.assertEqual(cm.exception.expected, '/CN={}'.format(id4))
        self.assertEqual(cm.exception.got, '/CN={}'.format(id3))

    def test_read_ca(self):
        self.skipTest('FIXME')

    def test_write_ca(self):
        self.skipTest('FIXME')

    def test_create_csr(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
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
        pki = identity.PKI(tmp.dir)
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
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.verify_csr(id1)
        self.assertEqual(cm.exception.filename, csr1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_csr(id2)

        # Test with bad subject
        id3 = pki.create_key()
        key_file = pki.path(id3, 'key')
        csr_file = pki.path(id3, 'csr')
        identity.create_csr(key_file, '/CN={}'.format(id1), csr_file)
        with self.assertRaises(identity.SubjectError) as cm:
            pki.verify_csr(id3)
        self.assertEqual(cm.exception.filename, csr_file)
        self.assertEqual(cm.exception.expected, '/CN={}'.format(id3))
        self.assertEqual(cm.exception.got, '/CN={}'.format(id1))

    def test_read_csr(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        id1 = pki.create_key()
        id2 = pki.create_key()
        csr1_file = pki.create_csr(id1)
        csr2_file = pki.create_csr(id2)
        data1 = open(csr1_file, 'rb').read()
        data2 = open(csr2_file, 'rb').read()
        os.remove(tmp.join(id1 + '.key'))
        os.remove(tmp.join(id2 + '.key'))
        self.assertEqual(pki.read_csr(id1), data1)
        self.assertEqual(pki.read_csr(id2), data2)
        os.remove(csr1_file)
        os.rename(csr2_file, csr1_file)
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.read_csr(id1)
        self.assertEqual(cm.exception.filename, csr1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.read_csr(id2)

        # Test with bad subject
        id3 = pki.create_key()
        key_file = pki.path(id3, 'key')
        csr_file = pki.path(id3, 'csr')
        identity.create_csr(key_file, '/CN={}'.format(id1), csr_file)
        with self.assertRaises(identity.SubjectError) as cm:
            pki.read_csr(id3)
        self.assertEqual(cm.exception.filename, csr_file)
        self.assertEqual(cm.exception.expected, '/CN={}'.format(id3))
        self.assertEqual(cm.exception.got, '/CN={}'.format(id1))

    def test_write_csr(self):
        self.skipTest('FIXME')

    def test_issue_cert(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)

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

    def test_issue_subcert(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)

        # Level 0
        id0 = pki.create_key()
        pki.create_ca(id0)

        # Level 1
        id1 = pki.create_key()
        pki.create_csr(id1)
        pki.issue_cert(id1, id0)

        # Level 2
        id2 = pki.create_key()
        pki.create_csr(id2)
        pki.issue_subcert(id2, id1)

    def test_verify_cert(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        ca_id = pki.create_key()
        pki.create_ca(ca_id)

        id1 = pki.create_key()
        pki.create_csr(id1)
        pki.issue_cert(id1, ca_id)

        id2 = pki.create_key()
        pki.create_csr(id2)
        pki.issue_cert(id2, ca_id)

        cert1_file = pki.path(id1, 'cert')
        cert2_file = pki.path(id2, 'cert')
        self.assertEqual(pki.verify_cert(id1, ca_id), cert1_file)
        self.assertEqual(pki.verify_cert(id2, ca_id), cert2_file)
        os.remove(cert1_file)
        os.rename(cert2_file, cert1_file)
        with self.assertRaises(identity.PublicKeyError) as cm:
            pki.verify_cert(id1, ca_id)
        self.assertEqual(cm.exception.filename, cert1_file)
        self.assertEqual(cm.exception.expected, id1)
        self.assertEqual(cm.exception.got, id2)
        with self.assertRaises(subprocess.CalledProcessError) as cm:
            pki.verify_cert(id2, ca_id)

        # Test with bad subject
        id3 = pki.create_key()
        csr_file = pki.path(id3, 'csr')
        cert_file = pki.path(id3, 'cert')
        identity.create_csr(pki.path(id3, 'key'), '/CN={}'.format(id1), csr_file)
        identity.issue_cert(
            csr_file,
            pki.path(ca_id, 'ca'),
            pki.path(ca_id, 'key'),
            pki.path(ca_id, 'srl'),
            cert_file
        )
        with self.assertRaises(identity.SubjectError) as cm:
            pki.verify_cert(id3, ca_id)
        self.assertEqual(cm.exception.filename, cert_file)
        self.assertEqual(cm.exception.expected, '/CN={}'.format(id3))
        self.assertEqual(cm.exception.got, '/CN={}'.format(id1))

    def test_get_ca(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        ca_id = pki.create_key(1024)
        pki.create_ca(ca_id)

        ca = pki.get_ca(ca_id)
        self.assertIsInstance(ca, identity.CA)
        self.assertEqual(ca.id, ca_id)
        self.assertEqual(ca.ca_file, pki.path(ca_id, 'ca'))
        self.assertEqual(ca, (ca.id, ca.ca_file))

    def test_get_cert(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        ca_id = pki.create_key()
        pki.create_ca(ca_id)
        cert_id = pki.create_key()
        pki.create_csr(cert_id)
        pki.issue_cert(cert_id, ca_id)

        cert = pki.get_cert(cert_id, ca_id)
        self.assertIsInstance(cert, identity.Cert)
        self.assertEqual(cert.id, cert_id)
        self.assertEqual(cert.cert_file, pki.path(cert_id, 'cert'))
        self.assertEqual(cert.key_file, pki.path(cert_id, 'key'))
        self.assertEqual(cert, (cert.id, cert.cert_file, cert.key_file))

    def test_load_machine(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        machine_id = pki.create_key()
        pki.create_ca(machine_id)

        machine = pki.load_machine(machine_id)
        self.assertIsInstance(machine, identity.Machine)
        self.assertEqual(machine.id, machine_id)
        self.assertEqual(machine.ca_file, pki.path(machine_id, 'ca'))
        self.assertEqual(machine.key_file, pki.path(machine_id, 'key'))
        self.assertIsNone(machine.cert_file)
        self.assertEqual(machine,
            (machine.id, machine.ca_file, machine.key_file, None)
        )

        user_id = pki.create_key()
        pki.create_ca(user_id)
        pki.create_csr(machine_id)
        pki.issue_cert(machine_id, user_id)
        machine = pki.load_machine(machine_id, user_id)
        self.assertIsInstance(machine, identity.Machine)
        self.assertEqual(machine.id, machine_id)
        self.assertEqual(machine.ca_file, pki.path(machine_id, 'ca'))
        self.assertEqual(machine.key_file, pki.path(machine_id, 'key'))
        self.assertEqual(machine.cert_file, pki.path(machine_id, 'cert'))
        self.assertEqual(machine,
            (machine.id, machine.ca_file, machine.key_file, machine.cert_file)
        )

    def test_load_user(self):
        tmp = TempDir()
        pki = identity.PKI(tmp.dir)
        user_id = pki.create_key()
        pki.create_ca(user_id)

        user = pki.load_user(user_id)
        self.assertIsInstance(user, identity.User)
        self.assertEqual(user.id, user_id)
        self.assertEqual(user.ca_file, pki.path(user_id, 'ca'))
        self.assertEqual(user.key_file, pki.path(user_id, 'key'))
        self.assertEqual(user, (user.id, user.ca_file, user.key_file))

        os.remove(pki.path(user_id, 'key'))
        user = pki.load_user(user_id)
        self.assertIsInstance(user, identity.User)
        self.assertEqual(user.id, user_id)
        self.assertEqual(user.ca_file, pki.path(user_id, 'ca'))
        self.assertIsNone(user.key_file)
        self.assertEqual(user, (user.id, user.ca_file, None))


class TestTempPKI(TestCase):
    def test_init(self):
        pki = identity.TempPKI()
        self.assertIsInstance(pki.server_ca, identity.CA)
        self.assertIsInstance(pki.server, identity.Cert)
        self.assertIsNone(pki.client_ca)
        self.assertIsNone(pki.client)
        self.assertEqual(
            pki.get_server_config(),
            {
                'cert_file': pki.server.cert_file,
                'key_file': pki.server.key_file,
            }
        )
        self.assertEqual(
            pki.get_client_config(),
            {
                'ca_file': pki.server_ca.ca_file,
                'check_hostname': False,
            }
        )

        pki = identity.TempPKI(client_pki=True)
        self.assertIsInstance(pki.server_ca, identity.CA)
        self.assertIsInstance(pki.server, identity.Cert)
        self.assertIsInstance(pki.client_ca, identity.CA)
        self.assertIsInstance(pki.client, identity.Cert)
        self.assertEqual(
            pki.get_server_config(),
            {
                'cert_file': pki.server.cert_file,
                'key_file': pki.server.key_file,
                'ca_file': pki.client_ca.ca_file,
            }
        )
        self.assertEqual(
            pki.get_client_config(),
            {
                'ca_file': pki.server_ca.ca_file,
                'check_hostname': False,
                'cert_file': pki.client.cert_file,
                'key_file': pki.client.key_file,
            }
        )
