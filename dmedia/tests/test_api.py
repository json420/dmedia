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

import dmedia
from dmedia import api

from .couch import CouchCase
from .helpers import random_bus


tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


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
            '--dbname', self.dbname,
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
        env = json.loads(inst.proxy.GetEnv())
        self.assertEqual(env['oauth'], self.env['oauth'])
