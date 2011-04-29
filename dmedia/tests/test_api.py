# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2011 Jason Gerard DeRose <jderose@novacut.com>
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
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

"""
Unit tests for `dmedia.api` module.
"""

import os
from os import path
from subprocess import Popen
import json
import time

import gnomekeyring

import dmedia
from dmedia.abstractcouch import get_db
from dmedia import api

from .couch import CouchCase
from .helpers import random_bus


tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


def get_auth():
    data = gnomekeyring.find_items_sync(
        gnomekeyring.ITEM_GENERIC_SECRET,
        {'desktopcouch': 'basic'}
    )
    (user, password) = data[0].secret.split(':')
    return (user, password)


class TestDMedia(CouchCase):
    klass = api.DMedia

    def setUp(self):
        """
        Launch dmedia dbus service using a random bus name.

        This will launch dmedia-service with a random bus name like this:

            dmedia-service --bus org.test3ISHAWZVSWVN5I5S.DMedia

        How do people usually unit test dbus services?  This works, but not sure
        if there is a better idiom in common use.  --jderose
        """
        super(TestDMedia, self).setUp()
        self.bus = random_bus()
        cmd = [script,
            '--bus', self.bus,
            '--env', json.dumps(self.env),
        ]
        self.service = Popen(cmd)
        time.sleep(1)  # Give dmedia-service time to start

    def tearDown(self):
        super(TestDMedia, self).tearDown()
        try:
            self.service.terminate()
            self.service.wait()
        except OSError:
            pass
        finally:
            self.service = None

    def test_all(self):
        inst = self.klass(self.bus)

        # DMedia.Version()
        self.assertEqual(inst.version(), dmedia.__version__)

        # DMedia.GetEnv()
        env = inst.get_env()
        self.assertEqual(env['oauth'], self.env['oauth'])
        self.assertEqual(env['port'], self.env['port'])
        self.assertEqual(env['url'], self.env['url'])
        self.assertEqual(env['dbname'], self.env['dbname'])

        # DMedia.GetAuthURL()
        (user, password) = get_auth()
        self.assertEqual(
            inst.get_auth_url(),
            'http://{user}:{password}@localhost:{port}/'.format(
                user=user, password=password, port=self.env['port']
            )
        )

        # DMedia.HasApp()
        db = get_db(self.env)
        self.assertNotIn('app', db)
        self.assertTrue(inst.has_app())
        self.assertTrue(db['app']['_rev'].startswith('1-'))
        self.assertTrue(inst.has_app())
        self.assertTrue(db['app']['_rev'].startswith('1-'))

        # DMedia.ListTransfers()
        self.assertEqual(inst.list_transfers(), [])

        # DMedia.Kill()
        self.assertIsNone(self.service.poll(), None)
        inst.kill()
        self.assertTrue(inst._proxy is None)
        time.sleep(1)  # Give dmedia-service time to shutdown
        self.assertEqual(self.service.poll(), 0)
