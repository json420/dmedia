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
from hashlib import md5

import usercouch
from dbase32 import random_id, db32enc
from microfiber import Server

from .base import TempDir
from dmedia.identity import PKI
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

    def test_machine_to_uuid(self):
        for i in range(100):
            data = os.urandom(30)
            machine_id = db32enc(data)
            md5sum = md5(data).hexdigest()
            self.assertEqual(startup.machine_to_uuid(machine_id), md5sum)
        self.assertEqual(startup.machine_to_uuid('3' * 48), 
            '862dec5c27142824a394bc6464928f48'
        )
        self.assertEqual(startup.machine_to_uuid('9' * 48), 
            '89bed3d7d4d7ee93f8130407dadf1b9c'
        )
        self.assertEqual(startup.machine_to_uuid('A' * 48), 
            'ed4f855c76217b409fcb12d378efd460'
        )
        self.assertEqual(startup.machine_to_uuid('Y' * 48), 
            '9e489c7c597142c7c3ac1201c95b54e1'
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
        self.assertIsNone(inst.mthread)

        # Test lockfile
        with self.assertRaises(usercouch.LockError) as cm:
            inst2 = startup.DmediaCouch(tmp.dir)
        self.assertEqual(cm.exception.lockfile, tmp.join('lockfile'))
        del inst

        machine_id = random_id()
        user_id = random_id()
        json.dump({'_id': machine_id}, open(tmp.join('machine-1.json'), 'w'))
        json.dump({'_id': user_id}, open(tmp.join('user-1.json'), 'w'))
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
        self.assertEqual(str(cm.exception), 'machine already exists')

        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        machine_id = inst.create_machine()
        self.assertIsInstance(machine_id, str)
        self.assertEqual(len(machine_id), 48)
        self.assertIsInstance(inst.machine, dict)
        self.assertEqual(inst.machine['_id'], machine_id)
        self.assertEqual(inst.load_config('machine-1'), inst.machine)

        with self.assertRaises(Exception) as cm:
            inst.create_machine()
        self.assertEqual(str(cm.exception), 'machine already exists')

    def test_create_machine_if_needed(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        self.assertIs(inst.create_machine_if_needed(), True)
        self.assertIs(inst.create_machine_if_needed(), False)
        self.assertIs(inst.wait_for_machine(), True)
        self.assertIsNone(inst.mthread)
        self.assertIs(inst.wait_for_machine(), False)

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
        self.assertEqual(inst.load_config('user-1'), inst.user)

        with self.assertRaises(Exception) as cm:
            inst.create_user()
        self.assertEqual(str(cm.exception), 'user already exists')

    def test_get_ssl_config(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        machine_id = inst.create_machine()
        user_id = inst.create_user()
        inst.load_pki()
        self.assertEqual(
            inst.get_ssl_config(),
            {
                'check_hostname': False,
                'max_depth': 1,
                'ca_file': inst.pki.path(user_id, 'ca'),
                'cert_file': inst.pki.path(machine_id, 'cert'),
                'key_file': inst.pki.path(machine_id, 'key'),
            }
        )

    def test_get_bootstrap_config(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        machine_id = inst.create_machine()
        user_id = inst.create_user()
        inst.load_pki()
        self.assertEqual(
            inst.get_bootstrap_config(),
            {
                'username': 'admin',
                'uuid': startup.machine_to_uuid(machine_id),
                'replicator': {
                    'check_hostname': False,
                    'max_depth': 1,
                    'ca_file': inst.pki.path(user_id, 'ca'),
                    'cert_file': inst.pki.path(machine_id, 'cert'),
                    'key_file': inst.pki.path(machine_id, 'key'),
                },
            }
        )

    def test_auto_bootstrap(self):
        tmp = TempDir()
        inst = startup.DmediaCouch(tmp.dir)
        inst.create_machine()
        inst.create_user()
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

