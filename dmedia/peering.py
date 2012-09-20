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


Request:

    POST /

    {"challenge": ""}

Response:

    {"nonce": "", "response": ""}


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
import tempfile
import shutil
from collections import namedtuple

from skein import skein512
from microfiber import random_id
from usercouch.sslhelpers import gen_key, gen_ca, gen_csr, gen_cert

from .server import BaseWSGI


# Skein personalization strings
PERS_CERT = b'20120918 jderose@novacut.com dmedia/cert'
PERS_RESPONSE = b'20120918 jderose@novacut.com dmedia/response'

TmpFiles = namedtuple('TmpFiles', 'key cert csr')
Files = namedtuple('Files', 'key cert')
CAFiles = namedtuple('CAFiles', 'key cert srl')


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
        )

    def ca_files(self, ca_id):
        return CAFiles(
            self.path(ca_id, 'key'),
            self.path(ca_id, 'cert'),
            self.path(ca_id, 'srl'),
        )

    def read(self, cert_id):
        cert_data = open(self.path(cert_id, 'cert'), 'rb').read()
        assert hash_cert(cert_data) == cert_id
        return cert_data

    def create(self, tmp_id):
        subject = get_subject(tmp_id)
        tmp = self.tmp_files(tmp_id)
        gen_key(tmp.key)
        gen_ca(tmp.key, subject, tmp.cert)
        cert_data = open(tmp.cert, 'rb').read()
        cert_id = hash_cert(cert_data)
        cert = self.files(cert_id)
        os.rename(tmp.key, cert.key)
        os.rename(tmp.cert, cert.cert)
        return cert_id

    def create_csr(self, tmp_id):
        subject = get_subject(tmp_id)
        tmp = self.tmp_files(tmp_id)
        gen_key(tmp.key)
        gen_csr(tmp.key, subject, tmp.csr)

    def issue(self, tmp_id, ca_id):
        tmp = self.tmp_files(tmp_id)
        ca = self.ca_files(ca_id)
        gen_cert(tmp.csr, ca.cert, ca.key, ca.srl, tmp.cert)
        cert_data = open(tmp.cert, 'rb').read()
        cert_id = hash_cert(cert_data)
        cert = self.files(cert_id)
        os.rename(tmp.cert, cert.cert)
        os.rename(tmp.csr, self.path(cert_id, 'csr'))
        try:
            os.rename(tmp.key, cert.key)
        except OSError:
            pass
        return cert_id


class TempPKI(PKI):
    def __init__(self):
        ssldir = tempfile.mkdtemp(prefix='TempPKI.')
        super().__init__(ssldir)
        assert self.ssldir is ssldir

    def __del__(self):
        if path.isdir(self.ssldir):
            shutil.rmtree(self.ssldir)


class WSGIApp(BaseWSGI):
    def __init__(self, cert_data):
        self.cert_data = cert_data

    def GET(self, environ, start_response):
        if environ['PATH_INFO'] != '/':
            raise NotFound()
        headers = [
            ('Content-Length', str(len(self.cert_data))),
            ('Content-Type', 'text/plain'),
        ]
        start_response('200 OK', headers)
        return [self.cert_data]

