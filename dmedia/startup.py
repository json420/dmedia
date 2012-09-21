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
    try:
        return json.load(open(filename, 'r'))
    except Exception:
        pass


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
    tmp_id = random_id()
    machine_id = couch.pki.create(tmp_id)
    config = create_machine(machine_id, tmp_id)
    save_config(machine_filename(couch), config)


def load_machine(couch):
    config = load_config(machine_filename(couch))
    return config


def init_user(couch, machine_id):
    assert isinstance(couch, UserCouch)
    tmp_id = random_id()
    user_id = couch.pki.create(tmp_id)
    couch.pki.create_csr(machine_id)
    cert_id = couch.pki.issue(machine_id, user_id)
    config = create_user(user_id, tmp_id)
    config['certs'][machine_id] = cert_id
    save_config(user_filename(couch), config)


def load_machine(couch):
    config = load_config(machine_filename(couch))
    return config


def load_user(couch, machine_id):
    if not has_user(couch):
        return None
    config = load_config(user_filename(couch))
    return config


def bootstrap_args(couch, machine_id, user):
    assert isinstance(couch, UserCouch)
    if user is None:
        return ('basic', None)
    ca = couch.pki.files(user['_id'])
    cert = couch.pki.files(user['certs'][machine_id])
    config = {
        'bind_address': '0.0.0.0',
        'oauth': user['oauth'],
        'ssl': {
            'key_file': cert.key,
            'cert_file': cert.cert,
        },
        'replicator': {
            'ca_file': ca.cert,
        },
    }
    return ('oauth', config)


def start_usercouch(couch):
    machine = load_machine(couch)
    machine_id = machine['_id']
    user = load_user(couch, machine_id)
    (auth, config) = bootstrap_args(couch, machine_id, user)
    env = couch.bootstrap(auth, config)
    return (env, machine, user)


def create_machine(machine_id, cn):
    """
    Create a 'dmedia/machine' document.
    """
    return {
        '_id': machine_id,
        'type': 'dmedia/machine',
        'time': time.time(),
        'cn': cn,
        'hostname': socket.gethostname(),
    }


def create_user(user_id, cn):
    """
    Create a 'dmedia/user' document.
    """
    return {
        '_id': user_id,
        'type': 'dmedia/user',
        'time': time.time(),
        'cn': cn,
        'oauth': random_oauth(),
        'certs': {},
    }

