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

from usercouch import UserCouch, random_oauth
from microfiber import random_id

from .peering import PKI


def load_config(filename):
    return json.load(open(filename, 'r'))
    

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


def get_usercouch(basedir):
    couch = UserCouch(basedir)
    couch.pki = PKI(couch.paths.ssl)
    return couch


def machine_filename(couch):
    assert isinstance(couch, UserCouch)
    return path.join(couch.basedir, 'machine.json')


def user_filename(couch):
    assert isinstance(couch, UserCouch)
    return path.join(couch.basedir, 'user.json')


def has_machine(couch):
    assert isinstance(couch, UserCouch)
    return path.isfile(machine_filename(couch))


def has_user(couch):
    assert isinstance(couch, UserCouch)
    return path.isfile(user_filename(couch))


def init_machine(couch):
    assert isinstance(couch, UserCouch)
    assert not has_machine(couch)
    machine_id = couch.pki.create_key()
    couch.pki.create_ca(machine_id)
    config = create_machine(machine_id)
    save_config(machine_filename(couch), config)


def load_machine(couch):
    config = load_config(machine_filename(couch))
    return config


def init_user(couch, machine_id):
    assert isinstance(couch, UserCouch)
    assert not has_user(couch)
    user_id = couch.pki.create_key()
    couch.pki.create_ca(user_id)
    cert_id = couch.pki.create_key()
    couch.pki.create_csr(cert_id)
    couch.pki.issue_cert(cert_id, user_id)
    config = create_user(user_id)
    config['certs'][machine_id] = cert_id
    save_config(user_filename(couch), config)


def add_machine(couch, machine_id, user):
    assert isinstance(couch, UserCouch)
    assert machine_id not in user['certs']
    couch.pki.create_csr(machine_id)
    cert_id = couch.pki.issue(machine_id, user['_id'])
    user['certs'][machine_id] = cert_id
    save_config(user_filename(couch), user)


def load_machine(couch):
    config = load_config(machine_filename(couch))
    return config


def load_user(couch, machine_id):
    if not has_user(couch):
        return None
    config = load_config(user_filename(couch))
    return config


def bootstrap_config(couch, machine_id, user_id):
    assert isinstance(couch, UserCouch)
    if user_id is None:
        return {'username': 'admin'}
    ca = couch.pki.get_ca(user_id)
    cert = couch.pki.get_cert(machine_id)
    return {
        'username': 'admin',
        'replicator': {
            'ca_file': ca.ca_file,
            'cert_file': cert.cert_file,
            'key_file': cert.key_file,
        },
    }


def start_usercouch(couch):
    if not has_machine(couch):
        init_machine(couch)
    machine = load_machine(couch)
    machine_id = machine['_id']
    user = load_user(couch, machine_id)
    (auth, config) = bootstrap_args(couch, machine_id, user)
    env = couch.bootstrap(auth, config)
    return (env, machine, user)


def create_machine(_id):
    """
    Create a 'dmedia/machine' document.
    """
    return {
        '_id': _id,
        'type': 'dmedia/machine',
        'time': time.time(),
        'hostname': socket.gethostname(),
    }


def create_user(_id):
    """
    Create a 'dmedia/user' document.
    """
    return {
        '_id': _id,
        'type': 'dmedia/user',
        'time': time.time(),
        'oauth': random_oauth(),
        'certs': {},
    }

