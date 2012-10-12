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
Helper functions for starting Dmedia.
"""

import os
from os import path
import time
import json
import socket
from base64 import b64encode
import logging

from usercouch import UserCouch

from .peering import PKI


log = logging.getLogger()


def load_config(filename):
    try:
        return json.load(open(filename, 'r'))
    except IOError:
        return None


def save_config(filename, config):
    tmp = filename + '.tmp'
    fp = open(tmp, 'w')
    json.dump(config, fp,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',',': '),
        indent=4,
    )
    fp.close()
    os.rename(tmp, filename)


def pem_attachment(filename):
    data = open(filename, 'rb').read()
    return {
        'data': b64encode(data).decode('utf-8'),
        'content_type': 'text/plain',
    }


def create_doc(_id, doc_type):
    return {
        '_id': _id,
#        '_attachments': {
#            'key': pem_attachment(key_file),
#            'ca': pem_attachment(ca_file),
#        },
        'type': doc_type,
        'time': time.time(),
    }


class DmediaCouch(UserCouch):
    def __init__(self, basedir):
        super().__init__(basedir)
        self.pki = PKI(self.paths.ssl)
        self.machine = self.load_config('machine')
        self.user = self.load_config('user')

    def load_config(self, name):
        return load_config(path.join(self.basedir, name + '.json'))

    def save_config(self, name, config):
        save_config(path.join(self.basedir, name + '.json'), config)

    def isfirstrun(self):
        return self.user is None

    def create_machine(self):
        if self.machine is not None:
            raise Exception('machine already exists')
        log.info('Creating RSA machine identity...')
        machine_id = self.pki.create_key()
        log.info('... machine_id: %s', machine_id)
        self.pki.create_ca(machine_id)
        doc = create_doc(machine_id, 'dmedia/machine')
        self.save_config('machine', doc)
        self.machine = self.load_config('machine')
        return machine_id

    def create_user(self):
        if self.machine is None:
            raise Exception('must create machine first')
        if self.user is not None:
            raise Exception('user already exists')
        log.info('Creating RSA user identity...')
        user_id = self.pki.create_key()
        log.info('... user_id: %s', user_id)
        self.pki.create_ca(user_id)
        self.pki.create_csr(self.machine['_id'])
        self.pki.issue_cert(self.machine['_id'], user_id)
        doc = create_doc(user_id, 'dmedia/user')
        self.save_config('user', doc)
        self.user = self.load_config('user')
        return user_id

    def load_pki(self):
        if self.user is None:
            self.pki.load_pki(self.machine['_id'])
        else:
            self.pki.load_pki(self.machine['_id'], self.user['_id'])

    def get_ssl_config(self):
        return {
            'check_hostname': False,
            'max_depth': 1,
            'ca_file': self.pki.user.ca_file,
            'cert_file': self.pki.machine.cert_file,
            'key_file': self.pki.machine.key_file,
        }

    def get_bootstrap_config(self):
        return {
            'username': 'admin',
            'replicator': self.get_ssl_config(),
        }

    def auto_bootstrap(self):
        assert self.user is not None
        assert self.machine is not None
        self.load_pki()
        config = self.get_bootstrap_config()
        return self.bootstrap('basic', config)

