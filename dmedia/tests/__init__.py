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
Unit tests for `dmedia` package.
"""

from unittest import TestCase
import sys
import os
from os import path
import logging
from subprocess import check_call, Popen, PIPE

from .base import TempHome

import dmedia


TREE = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
IN_TREE = path.isfile(path.join(TREE, 'setup.py'))


class TestScripts(TestCase):
    def setUp(self):
        if os.environ.get('DMEDIA_TEST_CORE_ONLY') == 'true':
            self.skipTest('not running script unit tests during build')

    def check_script(self, name, install_base):
        """
        Do a basic sanity check on a script.

        Currently verifies that -h and --version both work.
        """
        base = (TREE if IN_TREE else install_base)
        script = path.join(base, name)
        self.assertTrue(path.isfile(script))
        check_call([script, '-h'])
        p = Popen([script, '--version'], stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate()
        self.assertEqual(p.returncode, 0)
        # FIXME: Unconditionally run these once we drop Python 3.3 compatability:
        if sys.version_info >= (3, 4):
            self.assertEqual(stdout.decode('utf-8'), filestore.__version__ + '\n')
            self.assertEqual(stderr.decode('utf-8'), '')
        return script

    def test_dmedia_service(self):
        script = self.check_script('dmedia-service', '/usr/lib/dmedia')

    def test_dmedia_cli(self):
        script = self.check_script('dmedia-cli', '/usr/bin')

    def test_dmedia_gtk(self):
        self.skipTest('FIXME: switch UserWebKit to argparse')
        script = self.check_script('dmedia-gtk', '/usr/bin')

    def test_dmedia_peer_gtk(self):
        script = self.check_script('dmedia-peer-gtk', '/usr/bin')

    def test_dmedia_provision_drive(self):
        script = self.check_script('dmedia-provision-drive', '/usr/bin')

    def test_dmedia_migrate(self):
        script = self.check_script('dmedia-migrate', '/usr/bin')

    def test_dmedia_v0_v1_upgrade(self):
        script = self.check_script('dmedia-v0-v1-upgrade', '/usr/bin')


class TestConstants(TestCase):
    def test_version(self):
        self.assertIsInstance(dmedia.__version__, str)
        (year, month, rev) = dmedia.__version__.split('.')
        y = int(year)
        self.assertTrue(y >= 13)
        self.assertEqual(str(y), year)
        m = int(month)
        self.assertTrue(1 <= m <= 12)
        self.assertEqual('{:02d}'.format(m), month)
        r = int(rev)
        self.assertTrue(r >= 0)
        self.assertEqual(str(r), rev)


class TestFunctions(TestCase):
    def test_configure_logging(self):
        tmp = TempHome()
        cache = tmp.join('.cache', 'dmedia')
        self.assertFalse(path.isdir(cache))
        log = dmedia.configure_logging()
        self.assertIsInstance(log, logging.RootLogger)
        self.assertTrue(path.isdir(cache))
        self.assertEqual(os.listdir(cache), ['setup.py.log'])
        self.assertTrue(path.isfile(path.join(cache, 'setup.py.log')))
 
    def test_get_dmedia_dir(self):
        tmp = TempHome()
        d = dmedia.get_dmedia_dir()
        self.assertEqual(d, tmp.join('.local', 'share', 'dmedia'))
        self.assertTrue(path.isdir(d))
        self.assertEqual(os.listdir(d), [])
