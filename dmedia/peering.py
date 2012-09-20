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


    * Server starts, advertises on Avahi using ca_hash as the ID
    * Client does GET /ca_file
    * Client verifies that CA hash matches Avahi ID
    * Client configures SSLContext to verify and use ca_file

    * Client does GET /challenge
    * Server creates challenge and secret, displays secret to user
    * Server returns JSON with challenge and user_id
    * User enters secret on client
    * Client creates CSR and response
    * Client does PUT /response to send JSON with CSR, response, and
      counter-challenge

    * Server verifies response
    * Server issues cert and creates counter-response
    * Server returns JSON with cert and counter-response

    * Client verifies counter-response
    * Client verifies cert

Request:

    PUT /challenge

    {"challenge": ""}

Response:

    {"nonce": "", "response": ""}



"""

from base64 import b32encode, b32decode
import os
from os import path
import tempfile
import shutil
from collections import namedtuple

from skein import skein512
from microfiber import random_id
from usercouch.sslhelpers import gen_key, gen_ca, gen_csr, gen_cert


# Skein personalization strings
PERS_CERT = b'20120918 jderose@novacut.com dmedia/cert'
PERS_RESPONSE = b'20120918 jderose@novacut.com dmedia/response'

Files = namedtuple('Files', 'key_file cert_file srl_file')
TmpFiles = namedtuple('TmpFiles', 'key_file cert_file csr_file')


def _hash_cert(cert_data):
    return skein512(cert_data,
        digest_bits=200,
        pers=PERS_CERT,
    ).digest()


def hash_cert(cert_data):
    return b32encode(_hash_cert(cert_data)).decode('utf-8')


def compute_response(secret, challenge, nonce, client_hash, server_hash):
    skein = skein512(
        digest_bits=280,
        pers=PERS_RESPONSE,
        key=secret,
        nonce=(challange + nonce),
    )
    skein.update(client_hash)
    skein.update(server_hash)
    return b32encode(skein.digest()).decode('utf-8')


def ensuredir(d):
    try:
        os.mkdir(d)
    except OSError:
        mode = os.lstat(d).st_mode
        if not stat.S_ISDIR(mode):
            raise ValueError('not a directory: {!r}'.format(d))


def get_subject(tmp_id):
    return '/CN={}'.format(tmp_id)


class PKI:
    def __init__(self, ssldir):
        self.ssldir = ssldir
        self.tmpdir = path.join(ssldir, 'tmp')
        ensuredir(self.tmpdir)

    def tmp_path(self, tmp_id, ext):
        return path.join(self.tmpdir, '.'.join([tmp_id, ext]))

    def tmp_files(self, tmp_id):
        return TmpFiles(
            self.tmp_path(tmp_id, 'key'),
            self.tmp_path(tmp_id, 'cert'),
            self.tmp_path(tmp_id, 'csr'),
        )

    def path(self, cert_id, ext):
        return path.join(self.ssldir, '.'.join([cert_id, ext]))

    def files(self, cert_id):
        return Files(
            self.path(cert_id, 'key'),
            self.path(cert_id, 'cert'),
            self.path(cert_id, 'srl'),
        ) 

    def create(self, tmp_id):
        subject = get_subject(tmp_id)
        key = self.tmp_path(tmp_id, 'key')
        cert = self.tmp_path(tmp_id, 'cert')
        gen_key(key)
        gen_ca(key, subject, cert)
        cert_data = open(cert, 'rb').read()
        cert_id = hash_cert(cert_data)
        os.rename(key, self.path(cert_id, 'key'))
        os.rename(cert, self.path(cert_id, 'cert'))
        return cert_id

    def create_csr(self, tmp_id):
        subject = get_subject(tmp_id)
        key = self.tmp_path(tmp_id, 'key')
        csr = self.tmp_path(tmp_id, 'csr')
        gen_key(key)
        gen_csr(key, subject, csr)

    def issue(self, tmp_id, ca_id):
        tmp = self.tmp_files(tmp_id)
        ca = self.files(ca_id)
        gen_cert(
            tmp.csr_file, ca.cert_file, ca.key_file, ca.srl_file, tmp.cert_file
        )
        cert_data = open(tmp.cert_file, 'rb').read()
        cert_id = hash_cert(cert_data)
        return cert_id


class TempPKI(PKI):
    def __init__(self):
        ssldir = tempfile.mkdtemp(prefix='TempPKI.')
        super().__init__(ssldir)
        assert self.ssldir is ssldir

    def __del__(self):
        if path.isdir(self.ssldir):
            shutil.rmtree(self.ssldir)

