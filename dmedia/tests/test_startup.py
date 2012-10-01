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

import usercouch
from microfiber import random_id

from .base import TempDir
from dmedia.peering import PKI
from dmedia import startup


class TestFunctions(TestCase):
    def test_load_config(self):
        tmp = TempDir()
        filename = tmp.join('foo.json')
        
        with self.assertRaises(IOError) as cm:
            startup.load_config(filename)

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

    def test_get_usercouch(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertIsInstance(couch, usercouch.UserCouch)
        self.assertEqual(couch.basedir, tmp.dir)
        self.assertIsNone(couch.couchdb)
        self.assertIsInstance(couch.pki, PKI)
        self.assertIs(couch.pki.ssldir, couch.paths.ssl)

        # Test lockfile
        with self.assertRaises(usercouch.LockError) as cm:
            couch2 = startup.get_usercouch(tmp.dir)
        self.assertEqual(cm.exception.lockfile, tmp.join('lockfile'))

    def test_machine_filename(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertEqual(
            startup.machine_filename(couch),
            path.join(tmp.dir, 'machine.json')
        )

    def test_user_filename(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertEqual(
            startup.user_filename(couch),
            path.join(tmp.dir, 'user.json')
        )

    def test_has_machine(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertFalse(startup.has_machine(couch))
        tmp.touch('machine.json')
        self.assertTrue(startup.has_machine(couch))

    def test_has_user(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertFalse(startup.has_user(couch))
        tmp.touch('user.json')
        self.assertTrue(startup.has_user(couch))

    def test_init_machine(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertIsNone(startup.init_machine(couch))
        doc = json.load(open(tmp.join('machine.json'), 'r'))
        self.assertIsInstance(doc, dict)

    def test_init_user(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        machine_id = couch.pki.create_key()
        self.assertIsNone(startup.init_user(couch, machine_id))
        doc = json.load(open(tmp.join('user.json'), 'r'))
        self.assertIsInstance(doc, dict)

    def test_bootstrap_config(self):
        tmp = TempDir()
        couch = startup.get_usercouch(tmp.dir)
        self.assertEqual(
            startup.bootstrap_config(couch, None, None),
            {'username': 'admin'},
        )
        machine_id = couch.pki.create_key()
        couch.pki.create_ca(machine_id)
        couch.pki.create_csr(machine_id)
        self.assertEqual(
            startup.bootstrap_config(couch, machine_id, None),
            {'username': 'admin'},
        )
        user_id = couch.pki.create_key()
        couch.pki.create_ca(user_id)
        couch.pki.issue_cert(machine_id, user_id)
        self.assertEqual(
            startup.bootstrap_config(couch, machine_id, user_id),
            {
                'username': 'admin',
                'replicator': {
                    'ca_file': couch.pki.path(user_id, 'ca'),
                    'cert_file': couch.pki.path(machine_id, 'cert'),
                    'key_file': couch.pki.path(machine_id, 'key'),
                },
            }
        )
        
        

