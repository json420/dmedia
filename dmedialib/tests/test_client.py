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

import os
from os import path
from subprocess import Popen
import time
import dbus
import gobject
import dmedialib
from dmedialib import client, service
from dmedialib.constants import VIDEO, AUDIO, IMAGE, EXTENSIONS
from .helpers import CouchCase, TempDir, random_bus, prep_import_source
from .helpers import sample_mov_hash, sample_thm_hash


tree = path.dirname(path.dirname(path.abspath(dmedialib.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


class CaptureCallback(object):
    def __init__(self, signal, messages):
        self.signal = signal
        self.messages = messages

    def __call__(self, *args):
        self.messages.append(
            (self.signal,) + args
        )


class SignalCapture(object):
    def __init__(self, obj, *signals):
        self.obj = obj
        self.messages = []
        self.handlers = {}
        for name in signals:
            callback = CaptureCallback(name, self.messages)
            obj.connect(name, callback)
            self.handlers[name] = callback


class test_Client(CouchCase):
    klass = client.Client

    def setUp(self):
        """
        Launch dmedia dbus service using a random bus name.

        This will launch dmedia-service with a random bus name like this:

            dmedia-service --dummy --bus org.test3ISHAWZVSWVN5I5S.DMedia

        How do people usually unit test dbus services?  This works, but not sure
        if there is a better idiom in common use.  --jderose
        """
        super(test_Client, self).setUp()
        self.busname = random_bus()
        cmd = [script, '--no-gui',
            '--couchdir', self.couchdir,
            '--bus', self.busname,
        ]
        self.service = Popen(cmd)
        time.sleep(1)  # Give dmedia-service time to start

    def tearDown(self):
        super(test_Client, self).tearDown()
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
        return
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
        return
        inst = self.new()
        self.assertTrue(inst._Client__proxy is None)
        p = inst._proxy
        self.assertTrue(isinstance(p, dbus.proxies.ProxyObject))
        self.assertTrue(inst._Client__proxy is p)
        self.assertTrue(inst._proxy is p)

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

    def test_import(self):
        inst = self.new()
        signals = SignalCapture(inst,
            'import_started',
        )

        tmp = TempDir()

        # Test with relative path
        self.assertEqual(
            inst.start_import('some/relative/path'),
            'not_abspath'
        )
        self.assertEqual(
            inst.start_import('/media/EOS_DIGITAL/../../etc/ssh'),
            'not_abspath'
        )

        # Test with non-dir
        nope = tmp.join('memory_card')
        self.assertEqual(inst.start_import(nope), 'not_a_dir')
        nope = tmp.touch('memory_card')
        self.assertEqual(inst.start_import(nope), 'not_a_dir')
        os.unlink(nope)

        # Test a real import
        prep_import_source(tmp)
        self.assertEqual(inst.list_imports(), [])
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.start_import(tmp.path), 'already_running')
        self.assertEqual(inst.list_imports(), [tmp.path])



        return
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.stop_import(tmp.path), 'stopped')
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')
