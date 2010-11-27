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

    def on_started(self, obj, *args):
        self.signals.append(
            ('started',) + args
        )

    def on_finished(self, obj, *args):
        self.signals.append(
            ('finished',) + args
        )

    def on_progress(self, obj, *args):
        self.signals.append(
            ('progress',) + args
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
        return self.klass(busname=self.busname)

    def test_init(self):
        # Test with no busname
        inst = self.klass()
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._connect is True)
        self.assertTrue(inst._conn, dbus.SessionBus)

        # Test with connect=False
        inst = self.klass(connect=False)
        self.assertEqual(inst._busname, 'org.freedesktop.DMedia')
        self.assertTrue(inst._connect is False)
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
        base = unicode(tmp.path)
        inst = self.klass(self.busname, connect=False)
        c = SignalCapture()
        inst.connect('import_started', c.on_started)
        inst.connect('import_finished', c.on_finished)
        inst.connect('import_progress', c.on_progress)

        inst._connect_signals()
        mainloop = gobject.MainLoop()
        gobject.timeout_add(7000, mainloop.quit)

        self.assertEqual(inst.start_import(tmp.path), 'started')
        mainloop.run()

        self.assertEqual(
            c.signals,
            [
                ('started', base),
                ('progress', base, 0, 4),
                ('progress', base, 1, 4),
                ('progress', base, 2, 4),
                ('progress', base, 3, 4),
                ('progress', base, 4, 4),
                ('finished', base),
            ]
        )

    def test_kill(self):
        inst = self.new()
        self.assertEqual(self.service.poll(), None)
        inst.kill()
        self.assertTrue(inst._Client__proxy is None)
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

    def test_start_import(self):
        tmp = TempDir()
        nope = tmp.join('memory_card')
        inst = self.new()
        self.assertEqual(inst.start_import(nope), 'not_dir_or_file')
        self.assertEqual(inst.start_import('some/relative/path'), 'not_abspath')
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.start_import(tmp.path), 'already_running')

    def test_import_finished(self):
        # Test that DMedia.ImportFinished is removing process from active
        # imports after it gets the ImportFinished signal from the queue:
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.list_imports(), [tmp.path])
        time.sleep(7)  # dummy_import_files should run for ~6 seconds
        self.assertEqual(inst.list_imports(), [])

    def test_stop_import(self):
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.stop_import(tmp.path), 'stopped')
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')

    def test_list_imports(self):
        inst = self.new()
        tmp1 = TempDir()
        tmp2 = TempDir()

        # Add them in
        self.assertEqual(inst.list_imports(), [])
        self.assertEqual(inst.start_import(tmp1.path), 'started')
        self.assertEqual(inst.list_imports(), [tmp1.path])
        self.assertEqual(inst.start_import(tmp2.path), 'started')
        self.assertEqual(inst.list_imports(), sorted([tmp1.path, tmp2.path]))

        # Take them out
        self.assertEqual(inst.stop_import(tmp1.path), 'stopped')
        self.assertEqual(inst.list_imports(), [tmp2.path])
        self.assertEqual(inst.stop_import(tmp2.path), 'stopped')
        self.assertEqual(inst.list_imports(), [])
