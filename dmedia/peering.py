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

import base64
import os
from os import path
import stat
import tempfile
import shutil
from collections import namedtuple
from subprocess import check_call, check_output
import json
import socket
import logging

from skein import skein512
from microfiber import random_id, dumps

from dmedia.httpd import WSGIError


DAYS = 365 * 10
CA = namedtuple('CA', 'id ca_file')
Cert = namedtuple('Cert', 'id cert_file key_file')
Machine = namedtuple('Machine', 'id ca_file key_file cert_file')
User = namedtuple('User', 'id ca_file key_file')

# Skein personalization strings
PERS_PUBKEY = b'20120918 jderose@novacut.com dmedia/pubkey'
PERS_RESPONSE = b'20120918 jderose@novacut.com dmedia/response'
PERS_CSR = b'20120918 jderose@novacut.com dmedia/csr'
PERS_CERT = b'20120918 jderose@novacut.com dmedia/cert'

MAC_BITS = 280

USER = os.environ.get('USER')
HOST = socket.gethostname()

log = logging.getLogger()


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
        #'-sha384',
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
        #'-sha384',
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
        #'-sha384',
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


###########################################
# Helper functions for base32 encode/decode

def encode(value):
    """
    Base32-encode the bytes *value*.

    For example:

    >>> encode(b'skein')
    'ONVWK2LO'

    """
    assert isinstance(value, bytes)
    assert len(value) > 0 and len(value) % 5 == 0
    return base64.b32encode(value).decode('utf-8')


def decode(value):
    """
    Base32-decode the str *value*.

    For example:

    >>> decode('ONVWK2LO')
    b'skein'

    """
    assert isinstance(value, str)
    assert len(value) > 0 and len(value) % 8 == 0
    return base64.b32decode(value.encode('utf-8'))



###############################
# Skein-based hashing functions

def hash_pubkey(pubkey_data):
    """
    Hash an RSA public key to compute the Dmedia identity ID.

    For example:

    >>> hash_pubkey(b'The PEM encoded public key')
    'JTHPOXBZKRMAUGDKXDR74ZRVVSKYUMTHZNQUOSUNTQ2IO4UD'

    """
    skein = skein512(pubkey_data,
        digest_bits=240,
        pers=PERS_PUBKEY,
    )
    return encode(skein.digest())


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
    return encode(skein.digest())


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
    return encode(skein.digest())


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
    return encode(skein.digest())


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


class ChallengeResponse:
    def __init__(self, _id, peer_id):
        self.id = _id
        self.peer_id = peer_id
        self.local_hash = decode(_id)
        self.remote_hash = decode(peer_id)
        assert len(self.local_hash) == 30
        assert len(self.remote_hash) == 30

    def get_secret(self):
        # 40-bit secret (8 characters when base32 encoded)
        self.secret = os.urandom(5)
        return encode(self.secret)

    def set_secret(self, secret):
        assert len(secret) == 8
        self.secret = decode(secret)
        assert len(self.secret) == 5

    def get_challenge(self):
        self.challenge = os.urandom(20)
        return encode(self.challenge)

    def create_response(self, challenge):
        nonce = os.urandom(20)
        response = compute_response(
            self.secret,
            decode(challenge),
            nonce,
            self.remote_hash,
            self.local_hash
        )
        return (encode(nonce), response)

    def check_response(self, nonce, response):
        expected = compute_response(
            self.secret,
            self.challenge,
            decode(nonce),
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


class InfoApp:
    def __init__(self, _id):
        self.id = _id
        obj = {
            'id': _id,
            'user': USER,
            'host': HOST,
        }
        self.info = dumps(obj).encode('utf-8')
        self.info_length = str(len(self.info))

    def __call__(self, environ, start_response):
        if environ['wsgi.multithread'] is not False:
            raise WSGIError('500 Internal Server Error')
        if environ['PATH_INFO'] != '/':
            raise WSGIError('410 Gone')
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        start_response('200 OK',
            [
                ('Content-Length', self.info_length),
                ('Content-Type', 'application/json'),
            ]
        )
        return [self.info]


class ClientApp:
    allowed_states = (
        'ready',
        'gave_challenge',
        'in_response',
        'wrong_response',
        'response_ok',
    )

    forwarded_states = (
        'wrong_response',
        'response_ok',
    )

    def __init__(self, cr, queue):
        assert isinstance(cr, ChallengeResponse)
        self.cr = cr
        self.queue = queue
        self.__state = None
        self.map = {
            '/challenge': self.get_challenge,
            '/response': self.put_response,
        }

    def get_state(self):
        return self.__state

    def set_state(self, state):
        if state not in self.__class__.allowed_states:
            self.__state = None
            log.error('invalid state: %r', state)
            raise Exception('invalid state: {!r}'.format(state))
        self.__state = state
        if state in self.__class__.forwarded_states:
            self.queue.put(state)

    state = property(get_state, set_state)

    def __call__(self, environ, start_response):
        if environ['wsgi.multithread'] is not False:
            raise WSGIError('500 Internal Server Error')
        if environ.get('SSL_CLIENT_VERIFY') != 'SUCCESS':
            raise WSGIError('403 Forbidden')
        if environ.get('SSL_CLIENT_S_DN_CN') != self.cr.peer_id:
            raise WSGIError('403 Forbidden')
        if environ.get('SSL_CLIENT_I_DN_CN') != self.cr.peer_id:
            raise WSGIError('403 Forbidden')

        path_info = environ['PATH_INFO']
        if path_info not in self.map:
            raise WSGIError('410 Gone')
        log.info('%s %s', environ['REQUEST_METHOD'], environ['PATH_INFO'])
        try:
            obj = self.map[path_info](environ)            
            data = json.dumps(obj).encode('utf-8')
            start_response('200 OK',
                [
                    ('Content-Length', str(len(data))),
                    ('Content-Type', 'application/json'),
                ]
            )
            return [data]
        except WSGIError as e:
            raise e
        except Exception:
            log.exception('500 Internal Server Error')
            raise WSGIError('500 Internal Server Error')

    def get_challenge(self, environ):
        if self.state != 'ready':
            raise WSGIError('400 Bad Request Order')
        self.state = 'gave_challenge'
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        return {
            'challenge': self.cr.get_challenge(),
        }

    def put_response(self, environ):
        if self.state != 'gave_challenge':
            raise WSGIError('400 Bad Request Order')
        self.state = 'in_response'
        if environ['REQUEST_METHOD'] != 'PUT':
            raise WSGIError('405 Method Not Allowed')
        data = environ['wsgi.input'].read()
        obj = json.loads(data.decode('utf-8'))
        nonce = obj['nonce']
        response = obj['response']
        try:
            self.cr.check_response(nonce, response)
        except WrongResponse:
            self.state = 'wrong_response'
            raise WSGIError('401 Unauthorized')
        self.state = 'response_ok'
        return {'ok': True}


class ServerApp(ClientApp):

    allowed_states = (
        'info',
        'counter_response_ok',
        'in_csr',
        'bad_csr',
        'cert_issued',
    ) + ClientApp.allowed_states

    forwarded_states = (
        'bad_csr',
        'cert_issued',
    ) + ClientApp.forwarded_states

    def __init__(self, cr, queue, pki):
        super().__init__(cr, queue)
        self.pki = pki
        self.map['/'] = self.get_info
        self.map['/csr'] = self.post_csr

    def get_info(self, environ):
        if self.state != 'info':
            raise WSGIError('400 Bad Request State')
        self.state = 'ready'
        if environ['REQUEST_METHOD'] != 'GET':
            raise WSGIError('405 Method Not Allowed')
        return {
            'id': self.cr.id,
            'user': USER,
            'host': HOST,
        }

    def post_csr(self, environ):
        if self.state != 'counter_response_ok':
            raise WSGIError('400 Bad Request Order')
        self.state = 'in_csr'
        if environ['REQUEST_METHOD'] != 'POST':
            raise WSGIError('405 Method Not Allowed')
        data = environ['wsgi.input'].read()
        d = json.loads(data.decode('utf-8'))
        csr_data = base64.b64decode(d['csr'].encode('utf-8'))
        try:
            self.cr.check_csr_mac(csr_data, d['mac'])
            self.pki.write_csr(self.cr.peer_id, csr_data)
            self.pki.issue_cert(self.cr.peer_id, self.cr.id)
            cert_data = self.pki.read_cert(self.cr.peer_id, self.cr.id)
        except Exception as e:
            log.exception('could not issue cert')
            self.state = 'bad_csr'
            raise WSGIError('401 Unauthorized')       
        self.state = 'cert_issued'
        return {
            'cert': base64.b64encode(cert_data).decode('utf-8'),
            'mac': self.cr.cert_mac(cert_data),
        }


def ensuredir(d):
    try:
        os.mkdir(d)
    except OSError:
        mode = os.lstat(d).st_mode
        if not stat.S_ISDIR(mode):
            raise ValueError('not a directory: {!r}'.format(d))


def make_subject(cn):
    """
    Make an openssl certificate subject from the common name *cn*.

    For example:

    >>> make_subject('foo')
    '/CN=foo'

    """
    return '/CN={}'.format(cn)


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
