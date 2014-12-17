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
Secure peering protocol, SSL-based machine and user identity.

For some more background and original context, see:

    https://bugs.launchpad.net/dmedia/+bug/1064674

In a nutshell, this is the protocol:

1) The device to be added (which I'll call the "client") advertises
_dmedia-offer._tcp over Avahi using the hash of the machine cert public key as
an ID.  It spins up an HTTPD instance using its cert_file, key_file, and running
only the InfoApp.

2) The existing device (which I'll call the "server") always listens for
_dmedia_offer._tcp, and when one is found, it downloads the cert from client and
verifies that it has the correct public key hash, and then the server does a
test request to make sure the client has the corresponding private key.  If this
step doesn't check out, the user never even sees the peering offer.  It is
silently ignored.

3) If the user on the server side accepts the offer, the server spins up an
HTTPD instance using its cert_file and key_file, and the client's ca_file, and
will only allow connections from that specific client already bound to the
peering session. The server advertising _dmedia-accept._tcp over Avahi, using
the hash of the user CA cert public key as an ID.

4) When the client sees _dmedia-accept._tcp, it likewise downloads the server's
cert and checks that it matches the public key hash advertised on Avahi. If this
checks out, the client then reconfigures its HTTPD instance with a new
SSLContext that only allows incoming connections from certs signed by the user
CA. It also replaces the InfoApp with the ClientApp.

5) At this point in the protocol, secure communication has been established
between exactly two machines, but we don't yet know if they are the intended
machines. That's where the challenge-response steps in. The server generates and
displays a 40-bit secret code (base32 encoded, quite easy to read and type).

6) The user reads the secret from the server, types it on the client. The client
then does a GET /challenge, followed by POST /response. If the secret was wrong
(say a typo), the server creates a new secret and the user tries again. The
reason we can get away with a fairly low-entropy secret is we allow exactly one
attempt, after which things are reset with a new secret. After the user has
successfully entered the secret, the user's job is done, but the software has
more work to do.

7) At this point, the client has verified itself with the server, but the server
must now pass a counter-challenge. The server does a GET /challenge, then
POST /response. The only reason this would fail is if the user is under attack,
if someone else is trying to man-in-the middle the peering. If this fails, we
abort the peering, and give the user a helpful (but appropriately scary
sounding) error message.

8) The client then generates a certificate signing requests and does a
POST /csr. The server checks the CSR, makes sure it's for the expected client
pubic key, and then isuses a cert, returning in in the HTTP response.

9) The client then rigorously checks the cert, makes sure all the public keys,
issuer, subject, etc are correct. After this verification, the peering is
complete.
"""

import os
from os import path
import stat
from collections import namedtuple
from subprocess import check_call, check_output
import logging

from _skein import skein512
from dbase32 import db32enc, db32dec, random_id


# Skein personalization strings
PERS_PUBKEY = b'20120918 jderose@novacut.com dmedia/pubkey'
PERS_RESPONSE = b'20120918 jderose@novacut.com dmedia/response'
PERS_CSR = b'20120918 jderose@novacut.com dmedia/csr'
PERS_CERT = b'20120918 jderose@novacut.com dmedia/cert'

MAC_BITS = 280
DAYS = 365 * 10
CA = namedtuple('CA', 'id ca_file')
Cert = namedtuple('Cert', 'id cert_file key_file')
Machine = namedtuple('Machine', 'id ca_file key_file cert_file')
User = namedtuple('User', 'id ca_file key_file')

log = logging.getLogger()


###################
# Custom exceptions

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

class IssuerError(IdentityError):
    pass

class VerificationError(IdentityError):
    pass


class WrongResponse(Exception):
    def __init__(self, expected, got):
        self.expected = expected
        self.got = got
        super().__init__('Incorrect response')


class WrongMAC(Exception):
    def __init__(self, expected, got):
        self.expected = expected
        self.got = got
        super().__init__('Incorrect MAC')


###########################################################
# Skein-based hashing functions and ChallengeResponse class

def hash_pubkey(pubkey_data):
    """
    Hash an RSA public key to compute the Dmedia identity ID.

    For example:

    >>> hash_pubkey(b'The PEM encoded public key')
    'CMAIHQ4SDKF3N96DQ6KYVSKOOLDRNFMASGJNHLNGMJTBHVN6'

    """
    skein = skein512(pubkey_data,
        digest_bits=240,
        pers=PERS_PUBKEY,
    )
    return db32enc(skein.digest())


def compute_response(secret, challenge, nonce, challenger_hash, responder_hash):
    """
    Compute response hash used in challenge-response protocol.

    :param secret: the shared secret
    :param challenge: a nonce generated by the challenger
    :param none: a nonce generated by the responder
    :param challenger_hash: hash of the challenger's public key
    :param responder_hash: hash of the responder's public key
    """
    assert len(secret) == 5
    assert len(challenge) == 20
    assert len(nonce) == 20
    assert len(challenger_hash) == 30
    assert len(responder_hash) == 30
    skein = skein512(
        digest_bits=MAC_BITS,
        pers=PERS_RESPONSE,
        key=secret,
        nonce=(challenge + nonce),
    )
    skein.update(challenger_hash)
    skein.update(responder_hash)
    return db32enc(skein.digest())


def compute_csr_mac(secret, csr_data, remote_hash, local_hash):
    """
    Compute MAC to prove machine that created CSR has the secret.

    :param secret: the shared secret
    :param csr_data: PEM encoded certificate signing request
    :param remote_hash: hash of remote peer's public key
    :param local_hash: hash of local peer's public key
    """
    assert len(secret) == 5
    assert len(remote_hash) == 30
    assert len(local_hash) == 30
    skein = skein512(csr_data,
        digest_bits=MAC_BITS,
        pers=PERS_CSR,
        key=secret,
        key_id=(remote_hash + local_hash),
    )
    return db32enc(skein.digest())


def compute_cert_mac(secret, cert_data, remote_hash, local_hash):
    """
    Compute MAC to prove machine that issued certificate has the secret.

    :param secret: the shared secret
    :param cert_data: PEM encoded certificate
    :param remote_hash: hash of remote peer's public key
    :param local_hash: hash of local peer's public key
    """
    assert len(secret) == 5
    assert len(remote_hash) == 30
    assert len(local_hash) == 30
    skein = skein512(cert_data,
        digest_bits=MAC_BITS,
        pers=PERS_CERT,
        key=secret,
        key_id=(remote_hash + local_hash),
    )
    return db32enc(skein.digest())


class ChallengeResponse:
    """
    Helper class to hold state for challenge-response protocol.
    """

    def __init__(self, _id, peer_id):
        self.id = _id
        self.peer_id = peer_id
        self.local_hash = db32dec(_id)
        self.remote_hash = db32dec(peer_id)
        assert len(self.local_hash) == 30
        assert len(self.remote_hash) == 30

    def get_secret(self):
        # 40-bit secret (8 characters when base32 encoded)
        self.secret = os.urandom(5)
        return db32enc(self.secret)

    def set_secret(self, secret):
        assert len(secret) == 8
        self.secret = db32dec(secret)
        assert len(self.secret) == 5

    def get_challenge(self):
        self.challenge = os.urandom(20)
        return db32enc(self.challenge)

    def create_response(self, challenge):
        nonce = os.urandom(20)
        response = compute_response(
            self.secret,
            db32dec(challenge),
            nonce,
            self.remote_hash,
            self.local_hash
        )
        return (db32enc(nonce), response)

    def check_response(self, nonce, response):
        expected = compute_response(
            self.secret,
            self.challenge,
            db32dec(nonce),
            self.local_hash,
            self.remote_hash
        )
        if response != expected:
            del self.secret
            del self.challenge
            raise WrongResponse(expected, response)

    def csr_mac(self, csr_data):
        return compute_csr_mac(
            self.secret,
            csr_data,
            self.remote_hash,
            self.local_hash,
        )

    def check_csr_mac(self, csr_data, mac):
        expected = compute_csr_mac(
            self.secret,
            csr_data,
            self.local_hash,
            self.remote_hash,
        )
        if mac != expected:
            del self.secret
            raise WrongMAC(expected, mac)

    def cert_mac(self, cert_data):
        return compute_cert_mac(
            self.secret,
            cert_data,
            self.remote_hash,
            self.local_hash,
        )

    def check_cert_mac(self, cert_data, mac):
        expected = compute_cert_mac(
            self.secret,
            cert_data,
            self.local_hash,
            self.remote_hash,
        )
        if mac != expected:
            del self.secret
            raise WrongMAC(expected, mac)



##########################
# openssl helper functions

def make_subject(cn):
    """
    Make an openssl certificate subject from the common name *cn*.

    For example:

    >>> make_subject('foo')
    '/CN=foo'

    """
    return '/CN={}'.format(cn)


def create_key(dst_file, bits=2048):
    """
    Create an RSA keypair and save it to *dst_file*.
    """
    assert bits % 1024 == 0
    assert bits >= 1024
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


def get_issuer(cert_file):
    """
    Get the issuer from an X509 certificate (CA or issued certificate).
    """
    line = check_output(['openssl', 'x509',
        '-issuer',
        '-noout',
        '-in', cert_file,
    ]).decode('utf-8').rstrip('\n')

    prefix = 'issuer= '  # Different than get_csr_subject()
    if not line.startswith(prefix):
        raise Exception(line)
    return line[len(prefix):]


def ssl_verify(cert_file, ca_file):
    line = check_output(['openssl', 'verify',
        '-CAfile', ca_file,
        cert_file
    ]).decode('utf-8')
    expected = '{}: OK\n'.format(cert_file)
    if line != expected:
        raise VerificationError(cert_file, expected, line)
    return cert_file


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


def verify_ca(filename, _id):
    filename = verify(filename, _id)
    issuer = make_subject(_id)
    actual_issuer = get_issuer(filename)
    if issuer != actual_issuer:
        raise IssuerError(filename, issuer, actual_issuer)
    return ssl_verify(filename, filename)
    return filename


def verify_cert(cert_file, cert_id, ca_file, ca_id):
    filename = verify(cert_file, cert_id)
    issuer = make_subject(ca_id)
    actual_issuer = get_issuer(filename)
    if issuer != actual_issuer:
        raise IssuerError(filename, issuer, actual_issuer)
    return ssl_verify(filename, ca_file)
    return filename



###########################
# The PKI class and related

def ensuredir(d):
    try:
        os.mkdir(d)
    except OSError:
        mode = os.lstat(d).st_mode
        if not stat.S_ISDIR(mode):
            raise ValueError('not a directory: {!r}'.format(d))


class PKI:
    def __init__(self, ssldir):
        self.ssldir = ssldir
        self.tmpdir = path.join(ssldir, 'tmp')
        ensuredir(self.tmpdir)
        self.user = None
        self.machine = None

    def random_tmp(self):
        return path.join(self.tmpdir, random_id())

    def path(self, _id, ext):
        return path.join(self.ssldir, '.'.join([_id, ext]))

    def create_key(self, bits=2048):
        tmp_file = self.random_tmp()
        create_key(tmp_file, bits)
        _id = hash_pubkey(get_rsa_pubkey(tmp_file))
        key_file = self.path(_id, 'key')
        os.rename(tmp_file, key_file)
        return _id

    def verify_key(self, _id):
        key_file = self.path(_id, 'key')
        return verify_key(key_file, _id)

    def read_key(self, _id):
        key_file = self.verify_key(_id)
        return open(key_file, 'rb').read()

    def write_key(self, _id, data):
        tmp_file = self.random_tmp()
        open(tmp_file, 'wb').write(data)
        verify_key(tmp_file, _id)
        key_file = self.path(_id, 'key')
        os.rename(tmp_file, key_file)
        return key_file

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
        return verify_ca(ca_file, _id)

    def read_ca(self, _id):
        ca_file = self.verify_ca(_id)
        return open(ca_file, 'rb').read()

    def write_ca(self, _id, data):
        tmp_file = self.random_tmp()
        open(tmp_file, 'wb').write(data)
        verify_ca(tmp_file, _id)
        ca_file = self.path(_id, 'ca')
        os.rename(tmp_file, ca_file)
        return ca_file

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

    def read_csr(self, _id):
        csr_file = self.verify_csr(_id)
        return open(csr_file, 'rb').read()

    def write_csr(self, _id, data):
        tmp_file = self.random_tmp()
        open(tmp_file, 'wb').write(data)
        verify_csr(tmp_file, _id)
        csr_file = self.path(_id, 'csr')
        os.rename(tmp_file, csr_file)
        return csr_file

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

    def issue_subcert(self, _id, subca_id):
        csr_file = self.verify_csr(_id)
        tmp_file = self.random_tmp()
        cert_file = self.path(_id, 'cert')

        ca_file = self.path(subca_id, 'cert')
        key_file = self.verify_key(subca_id)
        srl_file = self.path(subca_id, 'srl')

        issue_cert(csr_file, ca_file, key_file, srl_file, tmp_file)
        os.rename(tmp_file, cert_file)
        return cert_file

    def verify_cert(self, cert_id, ca_id):
        cert_file = self.path(cert_id, 'cert')
        ca_file = self.verify_ca(ca_id)
        return verify_cert(cert_file, cert_id, ca_file, ca_id)

    def read_cert(self, cert_id, ca_id):
        cert_file = self.verify_cert(cert_id, ca_id)
        return open(cert_file, 'rb').read()

    def write_cert(self, cert_id, ca_id, cert_data):
        ca_file = self.verify_ca(ca_id)
        tmp_file = self.random_tmp()
        open(tmp_file, 'wb').write(cert_data)
        verify_cert(tmp_file, cert_id, ca_file, ca_id)
        cert_file = self.path(cert_id, 'cert')
        os.rename(tmp_file, cert_file)
        return cert_file

    def get_ca(self, _id):
        return CA(_id, self.verify_ca(_id))

    def get_cert(self, cert_id, ca_id):
        return Cert(
            cert_id,
            self.verify_cert(cert_id, ca_id),
            self.verify_key(cert_id)
        )

    def load_machine(self, machine_id, user_id=None):
        ca_file = self.verify_ca(machine_id)
        key_file = self.verify_key(machine_id)
        if user_id is None:
            cert_file = None
        else:
            cert_file = self.verify_cert(machine_id, user_id)
        return Machine(machine_id, ca_file, key_file, cert_file)

    def load_user(self, user_id):
        ca_file = self.verify_ca(user_id)
        if path.exists(self.path(user_id, 'key')):
            key_file = self.verify_key(user_id)
        else:
            key_file = None
        return User(user_id, ca_file, key_file)

    def load_pki(self, machine_id, user_id=None):
        self.machine = self.load_machine(machine_id, user_id)
        self.user = (None if user_id is None else self.load_user(user_id))  


# FIXME: This is only for unit testing, but requires `tempfile` and `shutil`,
# modules we don't want to needlessly import when running the service.
class TempPKI(PKI):
    def __init__(self, client_pki=False):
        import tempfile
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
            import shutil
            shutil.rmtree(self.ssldir)

    def create_pki(self):
        ca_id = self.create_key()
        self.create_ca(ca_id)

        cert_id = self.create_key()
        self.create_csr(cert_id)
        self.issue_cert(cert_id, ca_id)

        return (self.get_ca(ca_id), self.get_cert(cert_id, ca_id))

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


class IdenityClient:
    def __init__(self, peer_id, address):
        if not isinstance(address, tuple):
            raise TypeError('address must be a tuple') 
        if len(address) == 4:
            self.family = socket.AF_INET6
        elif len(address) == 2:
            self.family = socket.AF_INET
        else:
            raise ValueError(
                'address: must have 2 or 4 items; got {!r}'.format(address)
            )
        self.address = address

    def getpeercert(self):
        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        sslctx.options |= ssl.OP_NO_COMPRESSION
        sslctx.set_ciphers(
            'ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384'
        )
        sock = socket.socket(self.family, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            sock.connect(self.address)
            sock = sslctx.wrap_socket(sock)
            return ssl.DER_cert_to_PEM_cert(sock.getpeercert(True)).encode()
        finally:
            sock.shutdown(socket.SHUT_RDWR)

