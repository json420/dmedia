# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#
# dmedia: distributed media library
# Copyright (C) 2010 Jason Gerard DeRose <jderose@novacut.com>
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
Unit tests for `dmedialib.client` module.
"""

from unittest import TestCase
import os
from os import path
from subprocess import Popen
import time
from base64 import b32encode
import dbus
import dmedialib
from dmedialib import client, service

tree = path.dirname(path.dirname(path.abspath(dmedialib.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


class test_Client(TestCase):
    klass = client.Client

    def setUp(self):
        """
        Launch dmedia dbus service using a random bus name.
        """
        random = 'test' + b32encode(os.urandom(10))
        self.busname = '.'.join(['org', random, 'DMedia'])
        self.service = Popen([script, '--bus', self.busname])
        time.sleep(1)

    def tearDown(self):
        try:
            self.service.terminate()
            self.service.wait()
        except OSError:
            pass
        finally:
            self.service = None

    def new(self):
        return self.klass(busname=self.busname)

    def test_init(self):
        # Test with no busname
        inst = self.klass()
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._conn, dbus.SessionBus)

        # Test with busname=None
        inst = self.klass(busname=None)
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._conn, dbus.SessionBus)

        # Test with busname='test.busname'
        inst = self.klass(busname='test.freedesktop.DMedia')
        self.assertEqual(inst._busname, 'test.freedesktop.DMedia')
        self.assertTrue(inst._conn, dbus.SessionBus)

    def test_proxy(self):
        inst = self.new()
        p = inst._proxy
        self.assertTrue(isinstance(p, dbus.proxies.ProxyObject))

    def test_kill(self):
        inst = self.new()
        self.assertEqual(self.service.poll(), None)
        inst.kill()
        time.sleep(1)
        self.assertEqual(self.service.poll(), 0)
