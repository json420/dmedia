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
Unit tests for `dmedia.startup`.
"""

from unittest import TestCase
import os
from os import path
import json
from copy import deepcopy
import time

import usercouch
from microfiber import random_id, Server

from .base import TempDir
from dmedia.peering import PKI
from dmedia import startup


class TestFunctions(TestCase):
    def test_load_config(self):
        tmp = TempDir()
        filename = tmp.join('foo.json')

        self.assertIsNone(startup.load_config(filename))

        config = {'junk': random_id()}
        json.dump(config, open(filename, 'w'))
        self.assertEqual(startup.load_config(filename), config)

        open(filename, 'w').write('bad json, not treat for you')
        with self.assertRaises(ValueError) as cm:
            startup.load_config(filename)

    def test_save_config(self):
        tmp = TempDir()
        config = {'stuff': random_id()}
        filename = tmp.join('foo.json')

        with self.assertRaises(TypeError) as cm:
            startup.save_config(filename, object)
        self.assertFalse(path.exists(filename))
        self.assertTrue(path.isfile(filename + '.tmp'))

        startup.save_config(filename, config)
        self.assertTrue(path.isfile(filename))
        self.assertFalse(path.exists(filename + '.tmp'))
        self.assertEqual(json.load(open(filename, 'r')), config)

    def test_get_ssl_config(self):
        tmp = TempDir()
        pki = PKI(tmp.dir)
        machine_id = pki.create_key()
        pki.create_ca(machine_id)
        pki.create_csr(machine_id)
        user_id = pki.create_key()
        pki.create_ca(user_id)
        pki.issue_cert(machine_id, user_id)

        self.assertIsNone(startup.get_ssl_config(pki))

        pki.machine = pki.get_cert(machine_id)
        self.assertIsNone(startup.get_ssl_config(pki))

        pki.user = pki.get_ca(user_id)
        self.assertEqual(
            startup.get_ssl_config(pki),
            {
                'check_hostname': False,
                'max_depth': 1,
                'ca_file': pki.path(user_id, 'ca'),
                'cert_file': pki.path(machine_id, 'cert'),
                'key_file': pki.path(machine_id, 'key'),
            }
        )

    def test_get_bootstrap_config(self):
        tmp = TempDir()
        pki = PKI(tmp.dir)
        machine_id = pki.create_key()
        pki.create_ca(machine_id)
        pki.create_csr(machine_id)
        user_id = pki.create_key()
        pki.create_ca(user_id)
        pki.issue_cert(machine_id, user_id)

        self.assertEqual(
            startup.get_bootstrap_config(pki),
            {'username': 'admin'},
        )

        pki.machine = pki.get_cert(machine_id)
        self.assertEqual(
            startup.get_bootstrap_config(pki),
            {'username': 'admin'},
        )

        pki.user = pki.get_ca(user_id)
        self.assertEqual(
            startup.get_bootstrap_config(pki),
            {
                'username': 'admin',
                'replicator': {
                    'check_hostname': False,
                    'max_depth': 1,
                    'ca_file': pki.path(user_id, 'ca'),
                    'cert_file': pki.path(machine_id, 'cert'),
                    'key_file': pki.path(machine_id, 'key'),
                },
            }
        )


class TestDmediaCouch(TestCase):
    def test_init(self):
        tmp = TempDir()

        inst = startup.DmediaCouch(tmp.dir)
        self.assertEqual(inst.basedir, tmp.dir)
        self.assertIsInstance(inst.pki, PKI)
        self.assertEqual(inst.pki.ssldir, inst.paths.ssl)
        self.assertIsNone(inst.machine)
        self.assertIsNone(inst.user)

        # Test lockfile
        with self.assertRaises(usercouch.LockError) as cm:
            inst2 = startup.DmediaCouch(tmp.dir)
        self.assertEqual(cm.exception.lockfile, tmp.join('lockfile'))
        del inst

        machine_id = random_id()
        user_id = random_id()
        json.dump({'_id': machine_id}, open(tmp.join('machine.json'), 'w'))
        json.dump({'_id': user_id}, open(tmp.join('user.json'), 'w'))
        inst = startup.DmediaCouch(tmp.dir)
        self.assertEqual(inst.basedir, tmp.dir)
        self.assertIsInstance(inst.pki, PKI)
        self.assertEqual(inst.pki.ssldir, inst.paths.ssl)
        self.assertEqual(inst.machine, {'_id': machine_id})
        self.assertEqual(inst.user, {'_id': user_id})

    def test_load_config(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        self.assertIsNone(inst.load_config('foo'))

        doc = {'_id': random_id(), 'type': 'stuff/junk', 'time': time.time()}
        json.dump(doc, open(tmp.join('foo.json'), 'w'))
        self.assertEqual(inst.load_config('foo'), doc)

    def test_save_config(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        doc = {'_id': random_id(), 'type': 'stuff/junk', 'time': time.time()}
        self.assertIsNone(inst.save_config('foo', doc))
        self.assertEqual(json.load(open(tmp.join('foo.json'), 'r')), doc)

    def test_isfirstrun(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        self.assertTrue(inst.isfirstrun())
        inst.user = 'foo'
        self.assertFalse(inst.isfirstrun())
        inst.user = None
        self.assertTrue(inst.isfirstrun())

    def test_create_machine(self):
        class Subclass(startup.DmediaCouch):
            def __init__(self):
                self.machine = 'foo'

        inst = Subclass()
        with self.assertRaises(Exception) as cm:
            inst.create_machine()
        self.assertEqual(
            str(cm.exception),
            'machine already exists'
        )

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        machine_id = inst.create_machine()
        self.assertIsInstance(machine_id, str)
        self.assertEqual(len(machine_id), 48)
        self.assertIsInstance(inst.machine, dict)
        self.assertEqual(inst.machine['_id'], machine_id)
        self.assertEqual(inst.load_config('machine'), inst.machine)

        with self.assertRaises(Exception) as cm:
            inst.create_machine()
        self.assertEqual(
            str(cm.exception),
            'machine already exists'
        )
        
    def test_create_machine(self):
        class Subclass(startup.DmediaCouch):
            def __init__(self):
                self.machine = 'foo'

        inst = Subclass()
        with self.assertRaises(Exception) as cm:
            inst.create_machine()
        self.assertEqual(str(cm.exception), 'machine already exists')

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        machine_id = inst.create_machine()
        self.assertIsInstance(machine_id, str)
        self.assertEqual(len(machine_id), 48)
        self.assertIsInstance(inst.machine, dict)
        self.assertEqual(inst.machine['_id'], machine_id)
        self.assertEqual(inst.load_config('machine'), inst.machine)

        with self.assertRaises(Exception) as cm:
            inst.create_machine()
        self.assertEqual(str(cm.exception), 'machine already exists')

    def test_create_user(self):
        class Subclass(startup.DmediaCouch):
            def __init__(self):
                self.machine = 'foo'
                self.user = 'bar'

        inst = Subclass()
        with self.assertRaises(Exception) as cm:
            inst.create_user()
        self.assertEqual(str(cm.exception), 'user already exists')

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        with self.assertRaises(Exception) as cm:
            inst.create_user()
        self.assertEqual(str(cm.exception), 'must create machine first')
        machine_id = inst.create_machine()
        user_id = inst.create_user()
        self.assertNotEqual(machine_id, user_id)
        self.assertIsInstance(user_id, str)
        self.assertEqual(len(user_id), 48)
        self.assertIsInstance(inst.user, dict)
        self.assertEqual(inst.user['_id'], user_id)
        self.assertEqual(inst.load_config('user'), inst.user)

        with self.assertRaises(Exception) as cm:
            inst.create_user()
        self.assertEqual(str(cm.exception), 'user already exists')

    def test_auto_bootstrap(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        env = inst.auto_bootstrap()
        s = Server(env)
        self.assertEqual(s.get()['couchdb'], 'Welcome')
        self.assertIsNone(inst.get_ssl_config())

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        inst.firstrun_init(create_user=False)
        env = inst.auto_bootstrap()
        s = Server(env)
        self.assertEqual(s.get()['couchdb'], 'Welcome')
        self.assertIsNone(inst.get_ssl_config())

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        inst.firstrun_init(create_user=True)
        env = inst.auto_bootstrap()
        s = Server(env)
        self.assertEqual(s.get()['couchdb'], 'Welcome')
        self.assertEqual(
            inst.get_ssl_config(),
            {
                'check_hostname': False,
                'max_depth': 1,
                'ca_file': inst.pki.user.ca_file,
                'cert_file': inst.pki.machine.cert_file,
                'key_file': inst.pki.machine.key_file,
            }
        )
