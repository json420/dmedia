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
import gobject
import dmedialib
from dmedialib import client, service
from dmedialib.constants import VIDEO, AUDIO, IMAGE, EXTENSIONS
from .helpers import TempDir


tree = path.dirname(path.dirname(path.abspath(dmedialib.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


class SignalCapture(object):
    def __init__(self):
        self.signals = []

    def on_status(self, obj, msg):
        self.signals.append(
            ('status', msg)
        )

    def on_progress(self, obj, msg):
        self.signals.append(
            ('progress', msg)
        )


class test_Client(TestCase):
    klass = client.Client

    def setUp(self):
        """
        Launch dmedia dbus service using a random bus name.

        This will launch dmedia-service with a random bus name like this:

            dmedia-service --dummy --bus org.test3ISHAWZVSWVN5I5S.DMedia

        How do people usually unit test dbus services?  This works, but not sure
        if there is a better idiom in common use.  --jderose
        """
        random = 'test' + b32encode(os.urandom(10))  # 80-bits of entropy
        self.busname = '.'.join(['org', random, 'DMedia'])
        self.service = Popen([script, '--dummy', '--bus', self.busname])
        time.sleep(1)  # Give dmedia-service time to start

    def tearDown(self):
        try:
            self.service.terminate()
            self.service.wait()
        except OSError:
            pass
        finally:
            self.service = None

    def new(self):
        return self.klass(busname=self.busname, signals=False)

    def test_init(self):
        # Test with no busname
        inst = self.klass()
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._signals is True)
        self.assertTrue(inst._conn, dbus.SessionBus)

        # Test with signals=False
        inst = self.klass(signals=False)
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._signals is False)
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
        self.assertTrue(inst._Client__proxy is None)
        p = inst._proxy
        self.assertTrue(isinstance(p, dbus.proxies.ProxyObject))
        self.assertTrue(inst._Client__proxy is p)
        self.assertTrue(inst._proxy is p)

    def test_connect_signals(self):
        tmp = TempDir()
        inst = self.new()
        c = SignalCapture()
        inst.connect('import_status', c.on_status)
        inst.connect('import_progress', c.on_progress)

        inst._connect_signals()
        mainloop = gobject.MainLoop()
        gobject.timeout_add(7000, mainloop.quit)

        self.assertEqual(inst.import_start(tmp.path), 'started')
        mainloop.run()

        self.assertEqual(len(inst._captured), len(c.signals))
        self.assertEqual(len(inst._captured), 7)

    def test_kill(self):
        inst = self.new()
        self.assertEqual(self.service.poll(), None)
        inst.kill()
        time.sleep(2)  # Give dmedia-service time to shutdown
        self.assertEqual(self.service.poll(), 0)

    def test_version(self):
        inst = self.new()
        self.assertEqual(inst.version(), dmedialib.__version__)

    def test_get_extensions(self):
        inst = self.new()
        self.assertEqual(inst.get_extensions(['video']), sorted(VIDEO))
        self.assertEqual(inst.get_extensions(['audio']), sorted(AUDIO))
        self.assertEqual(inst.get_extensions(['image']), sorted(IMAGE))
        self.assertEqual(inst.get_extensions(['all']), sorted(EXTENSIONS))
        self.assertEqual(
            inst.get_extensions(['video', 'audio']),
            sorted(VIDEO + AUDIO)
        )
        self.assertEqual(
            inst.get_extensions(['video', 'image']),
            sorted(VIDEO + IMAGE)
        )
        self.assertEqual(
            inst.get_extensions(['audio', 'image']),
            sorted(AUDIO + IMAGE)
        )
        self.assertEqual(
            inst.get_extensions(['video', 'audio', 'image']),
            sorted(EXTENSIONS)
        )
        self.assertEqual(
            inst.get_extensions(['video', 'audio', 'image', 'all']),
            sorted(EXTENSIONS)
        )
        self.assertEqual(inst.get_extensions(['foo', 'bar']), [])

    def test_import_start(self):
        tmp = TempDir()
        nope = tmp.join('memory_card')
        inst = self.new()
        self.assertEqual(inst.import_start(nope), 'not_dir_or_file')
        self.assertEqual(inst.import_start('some/relative/path'), 'not_abspath')
        self.assertEqual(inst.import_start(tmp.path), 'started')
        self.assertEqual(inst.import_start(tmp.path), 'already_running')

    def test_handle_msg(self):
        # Test that DMedia._handle_imports is removing process from active
        # imports after it gets the 'finished' status message.
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.import_start(tmp.path), 'started')
        self.assertEqual(inst.import_list(), [tmp.path])
        time.sleep(7)  # dummy_import_files should run for ~6 seconds
        self.assertEqual(inst.import_list(), [])

    def test_import_stop(self):
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.import_stop(tmp.path), 'not_running')
        self.assertEqual(inst.import_start(tmp.path), 'started')
        self.assertEqual(inst.import_stop(tmp.path), 'stopped')
        self.assertEqual(inst.import_stop(tmp.path), 'not_running')

    def test_import_list(self):
        inst = self.new()
        tmp1 = TempDir()
        tmp2 = TempDir()

        # Add them in
        self.assertEqual(inst.import_list(), [])
        self.assertEqual(inst.import_start(tmp1.path), 'started')
        self.assertEqual(inst.import_list(), [tmp1.path])
        self.assertEqual(inst.import_start(tmp2.path), 'started')
        self.assertEqual(inst.import_list(), sorted([tmp1.path, tmp2.path]))

        # Take them out
        self.assertEqual(inst.import_stop(tmp1.path), 'stopped')
        self.assertEqual(inst.import_list(), [tmp2.path])
        self.assertEqual(inst.import_stop(tmp2.path), 'stopped')
        self.assertEqual(inst.import_list(), [])
