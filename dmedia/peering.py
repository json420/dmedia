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
already associated with the user, and the other the one to be associated.

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
susceptible is not to any "offline" attack, because there isn't an intrinsic
correct answer.

For example, this would not be the case if we used the secret to encrypt a
message and send it from one to another.  An attacker could run through the
keyspace till (say) gpg stopped telling them the passphrase is wrong.
"""

from base64 import b32encode, b32decode
import os
from collections import namedtuple

from skein import skein512


# Skein personalization strings
PERS_USER_CA = b'20120918 jderose@novacut.com peering/user-ca'
PERS_MACHINE_CSR = b'20120918 jderose@novacut.com peering/machine-csr'
PERS_MACHINE_CERT = b'20120918 jderose@novacut.com peering/machine-cert'
PERS_CLIENT = b'20120918 jderose@novacut.com peering/client'
PERS_SERVER = b'20120918 jderose@novacut.com peering/server'

DIGEST_BITS = 240


def hash_user_ca(data):
    return skein512(data,
        digest_bits=DIGEST_BITS,
        pers=PERS_USER_CA,
    ).digest()


def hash_machine_csr(data):
    return skein512(data,
        digest_bits=DIGEST_BITS,
        pers=PERS_MACHINE_CSR,
    ).digest()


def hash_machine_cert(data):
    return skein512(data,
        digest_bits=DIGEST_BITS,
        pers=PERS_MACHINE_CERT,
    ).digest()


def client_response(challenge, secret, csr_hash, ca_hash):
    """
    This hash proves that the client knows the secret.

    This allows the server to answer the question, "Did the machine that made
    this certificate signing request have the secret?"

    And we tie this hash to the *ca_hash* so the value is only usable for
    the user CA in question.
    """
    skein = skein512(ca_hash,
        digest_bits=DIGEST_BITS,
        pers=PERS_CLIENT,
        nonce=challenge,
        key=secret,
    )
    skein.update(csr_hash)
    return skein.digest()


def server_response(challenge, secret, cert_hash, ca_hash):
    """
    This hash proves that the server knows the secret.

    This allows the client to answer the question, "Did the machine that issued
    this certificate have the secret?"

    And we tie this hash to the *ca_hash* so the value is only usable for
    the user CA in question.
    """
    skein = skein512(ca_hash,
        digest_bits=DIGEST_BITS,
        pers=PERS_SERVER,
        nonce=challenge,
        key=secret,
    )
    skein.update(cert_hash)
    return skein.digest()

