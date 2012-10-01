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
Securely peer one device with another device on the same local network.

We want it to be easy for a user to associate multiple devices with the same
user CA. This is the local-network "user account" that works without any cloud
infrastructure, without a Novacut account or any other 3rd-party account.

To do the peering, the user needs both devices side-by-side.  One device is
already associated with the user, and the other is the one to be associated.

The existing device generates a random secret and displays it on the screen.
The user then enter the secret on the second device, and each device does a
challenge-response to make the other prove it has the same secret.

In a nutshell:

    1. A generates random ``nonce1`` to challenge B
    2. B must respond with ``hash(secret + nonce1)``
    3. B generates random ``nonce2`` to challenge A
    4. A must respond with ``hash(secret + nonce2)``

We only give the responder one try, and if it's wrong, the process starts over
with a new secret.  If the user makes a typo, we don't let them try again to
correctly type the initial secret... they must try and correctly type a new
secret.

Limiting the responder to only one attempt lets us to use a fairly low-entropy
secret, something easy for the user to type.  And importantly, the secret
is not susceptible to any "offline" attack, because there isn't an intrinsicly
correct answer.

For example, this would not be the case if we used the secret to encrypt a
message and send it from one to another.  An attacker could run through the
keyspace till (say) gpg stopped telling them the passphrase is wrong.



Request 1:

    GET /challenge

Response 1:

    {"challenge": ""}


Request 2:

    POST /response

    {"nonce": "", "response": "", "counter_challenge": ""}

Response 2:

    {"nonce": "", "counter_response": ""} 



1. A starts server, publishes _dmedia-offer._tcp under cert_a_id

2. B downloads cert_a from A, if hash doesn't match cert_a_id, ABORT

3. B prompts user about offer, if user declines, ABORT

4. B starts server, publishes _dmedia-accept._tcp under cert_b_id

5. A downloads cert_b from B, if hash doesn't match cert_b_id, ABORT

6. A generates secret, displays to user, waits for request from B

7. User enters secret on B

8. B does GET /challenge to get challenge from A

9. B does POST /response to post response and counter-challenge to A

10. If response is wrong, A assumes user typo, RESTART at (6) with new secret

11. A returns counter-response to B

12. If counter-response is wrong, B ABORTS with scary warning

13. DONE!
    
    

"""

from base64 import b32encode, b32decode
import os
from os import path
import stat
import tempfile
import shutil
from collections import namedtuple
from subprocess import check_call, check_output

from skein import skein512
from microfiber import random_id


DAYS = 365 * 10
CA = namedtuple('CA', 'id ca_file')
Cert = namedtuple('Cert', 'id cert_file key_file')

# Skein personalization strings
PERS_PUBKEY = b'20120918 jderose@novacut.com dmedia/pubkey'
PERS_RESPONSE = b'20120918 jderose@novacut.com dmedia/response'


class IdentityError(Exception):
    def __init__(self, filename, expected, got):
        self.filename = filename
        self.expected = expected
        self.got = got
        super().__init__(
            'expected {!r}; got {!r}'.format(expected, got)
        )


class PublicKeyError(IdentityError):
    pass


class SubjectError(IdentityError):
    pass


def create_key(dst_file, bits=2048):
    """
    Create an RSA keypair and save it to *dst_file*.
    """
    assert bits % 1024 == 0
    check_call(['openssl', 'genrsa',
        '-out', dst_file,
        str(bits)
    ])


def create_ca(key_file, subject, dst_file):
    """
    Create a self-signed X509 certificate authority.

    *subject* should be an str in the form ``'/CN=foo'``.
    """
    check_call(['openssl', 'req',
        '-new',
        '-x509',
        '-sha384',
        '-days', str(DAYS),
        '-key', key_file,
        '-subj', subject,
        '-out', dst_file,
    ])


def create_csr(key_file, subject, dst_file):
    """
    Create a certificate signing request.

    *subject* should be an str in the form ``'/CN=foo'``.
    """
    check_call(['openssl', 'req',
        '-new',
        '-sha384',
        '-key', key_file,
        '-subj', subject,
        '-out', dst_file,
    ])


def issue_cert(csr_file, ca_file, key_file, srl_file, dst_file):
    """
    Create a signed certificate from a certificate signing request.
    """
    check_call(['openssl', 'x509',
        '-req',
        '-sha384',
        '-days', str(DAYS),
        '-CAcreateserial',
        '-in', csr_file,
        '-CA', ca_file,
        '-CAkey', key_file,
        '-CAserial', srl_file,
        '-out', dst_file
    ])


def get_rsa_pubkey(key_file):
    return check_output(['openssl', 'rsa',
        '-pubout',
        '-in', key_file,
    ])


def get_csr_pubkey(csr_file):
    return check_output(['openssl', 'req',
        '-pubkey',
        '-noout',
        '-in', csr_file,
    ])  


def get_pubkey(cert_file):
    return check_output(['openssl', 'x509',
        '-pubkey',
        '-noout',
        '-in', cert_file,
    ])


def get_csr_subject(csr_file):
    """
    Get subject from certificate signing request in *csr_file*.
    """
    line = check_output(['openssl', 'req',
        '-subject',
        '-noout',
        '-in', csr_file,
    ]).decode('utf-8').rstrip('\n')

    prefix = 'subject='
    if not line.startswith(prefix):
        raise Exception(line)
    return line[len(prefix):]


def get_subject(cert_file):
    """
    Get the subject from an X509 certificate (CA or issued certificate).
    """
    line = check_output(['openssl', 'x509',
        '-subject',
        '-noout',
        '-in', cert_file,
    ]).decode('utf-8').rstrip('\n')

    prefix = 'subject= '  # Different than get_csr_subject()
    if not line.startswith(prefix):
        raise Exception(line)
    return line[len(prefix):]


def verify_key(filename, _id):
    actual_id = hash_pubkey(get_rsa_pubkey(filename))
    if _id != actual_id:
        raise PublicKeyError(filename, _id, actual_id)
    return filename


def verify_csr(filename, _id):
    actual_id = hash_pubkey(get_csr_pubkey(filename))
    if _id != actual_id:
        raise PublicKeyError(filename, _id, actual_id)
    subject = make_subject(_id)
    actual_subject = get_csr_subject(filename)
    if subject != actual_subject:
        raise SubjectError(filename, subject, actual_subject)
    return filename


def verify(filename, _id):
    actual_id = hash_pubkey(get_pubkey(filename))
    if _id != actual_id:
        raise PublicKeyError(filename, _id, actual_id)
    subject = make_subject(_id)
    actual_subject = get_subject(filename)
    if subject != actual_subject:
        raise SubjectError(filename, subject, actual_subject)
    return filename


def _hash_pubkey(data):
    return skein512(data,
        digest_bits=200,
        pers=PERS_PUBKEY,
    ).digest()


def hash_pubkey(data):
    return b32encode(_hash_pubkey(data)).decode('utf-8')


def _hash_cert(cert_data):
    return skein512(cert_data,
        digest_bits=200,
        pers=PERS_CERT,
    ).digest()


def hash_cert(cert_data):
    return b32encode(_hash_cert(cert_data)).decode('utf-8')


def compute_response(secret, challenge, nonce, challenger_hash, responder_hash):
    """

    :param secret: the shared secret

    :param challenge: a nonce generated by the challenger

    :param none: a nonce generated by the responder

    :param challenger_hash: hash of the challengers certificate

    :param responder_hash: hash of the responders certificate
    """
    skein = skein512(
        digest_bits=280,
        pers=PERS_RESPONSE,
        key=secret,
        nonce=(challange + nonce),
    )
    skein.update(challenger_hash)
    skein.update(responder_hash)
    return b32encode(skein.digest()).decode('utf-8')


def ensuredir(d):
    try:
        os.mkdir(d)
    except OSError:
        mode = os.lstat(d).st_mode
        if not stat.S_ISDIR(mode):
            raise ValueError('not a directory: {!r}'.format(d))


def make_subject(cn):
    return '/CN={}'.format(cn)


class PKI:
    def __init__(self, ssldir):
        self.ssldir = ssldir
        self.tmpdir = path.join(ssldir, 'tmp')
        ensuredir(self.tmpdir)

    def random_tmp(self):
        return path.join(self.tmpdir, random_id())

    def path(self, _id, ext):
        return path.join(self.ssldir, '.'.join([_id, ext]))

    def create_key(self):
        tmp_file = self.random_tmp()
        create_key(tmp_file)
        _id = hash_pubkey(get_rsa_pubkey(tmp_file))
        key_file = self.path(_id, 'key')
        os.rename(tmp_file, key_file)
        return _id

    def verify_key(self, _id):
        key_file = self.path(_id, 'key')
        return verify_key(key_file, _id)

    def create_ca(self, _id):
        key_file = self.verify_key(_id)
        subject = make_subject(_id)
        tmp_file = self.random_tmp()
        ca_file = self.path(_id, 'ca')
        create_ca(key_file, subject, tmp_file)
        os.rename(tmp_file, ca_file)
        return ca_file

    def verify_ca(self, _id):
        ca_file = self.path(_id, 'ca')
        return verify(ca_file, _id)

    def create_csr(self, _id):
        key_file = self.verify_key(_id)
        subject = make_subject(_id)
        tmp_file = self.random_tmp()
        csr_file = self.path(_id, 'csr')
        create_csr(key_file, subject, tmp_file)
        os.rename(tmp_file, csr_file)
        return csr_file

    def verify_csr(self, _id):
        csr_file = self.path(_id, 'csr')
        return verify_csr(csr_file, _id)

    def issue_cert(self, _id, ca_id):
        csr_file = self.verify_csr(_id)
        tmp_file = self.random_tmp()
        cert_file = self.path(_id, 'cert')

        ca_file = self.verify_ca(ca_id)
        key_file = self.verify_key(ca_id)
        srl_file = self.path(ca_id, 'srl')

        issue_cert(csr_file, ca_file, key_file, srl_file, tmp_file)
        os.rename(tmp_file, cert_file)
        return cert_file

    def verify_cert(self, _id):
        cert_file = self.path(_id, 'cert')
        return verify(cert_file, _id)

    def get_ca(self, _id):
        return CA(_id, self.verify_ca(_id))

    def get_cert(self, _id):
        return Cert(_id, self.verify_cert(_id), self.verify_key(_id))


class TempPKI(PKI):
    def __init__(self, client_pki=False):
        ssldir = tempfile.mkdtemp(prefix='TempPKI.')
        super().__init__(ssldir)
        assert self.ssldir is ssldir
        (self.server_ca, self.server) = self.create_pki()
        if client_pki:
            (self.client_ca, self.client) = self.create_pki()
        else:
            self.client_ca = None
            self.client = None

    def __del__(self):
        if path.isdir(self.ssldir):
            shutil.rmtree(self.ssldir)

    def create_pki(self):
        ca_id = self.create_key()
        self.create_ca(ca_id)

        cert_id = self.create_key()
        self.create_csr(cert_id)
        self.issue_cert(cert_id, ca_id)

        return (self.get_ca(ca_id), self.get_cert(cert_id))

    def get_server_config(self):
        config = {
            'cert_file': self.server.cert_file,
            'key_file': self.server.key_file,
        }
        if self.client_ca is not None:
            config.update({
                'ca_file': self.client_ca.ca_file,
            })
        return config

    def get_client_config(self):
        config = {
            'ca_file': self.server_ca.ca_file,
            'check_hostname': False,
        }
        if self.client is not None:
            config.update({
                'cert_file': self.client.cert_file,
                'key_file': self.client.key_file,   
            })
        return config

