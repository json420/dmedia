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
from .helpers import CouchCase, TempDir, random_bus
from .helpers import sample_mov_hash, sample_thm_hash


tree = path.dirname(path.dirname(path.abspath(dmedialib.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-service')
assert path.isfile(script)


class SignalCapture(object):
    def __init__(self):
        self.signals = []

    def on_batch_started(self, obj, *args):
        self.signals.append(
            ('batch_started',) + args
        )

    def on_batch_finished(self, obj, *args):
        self.signals.append(
            ('batch_finished',) + args
        )

    def on_started(self, obj, *args):
        self.signals.append(
            ('started',) + args
        )

    def on_count(self, obj, *args):
        self.signals.append(
            ('count',) + args
        )

    def on_progress(self, obj, *args):
        self.signals.append(
            ('progress',) + args
        )

    def on_finished(self, obj, *args):
        self.signals.append(
            ('finished',) + args
        )


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

    def test_connect_signals(self):
        return
        tmp = TempDir()
        base = tmp.path
        files = tuple(
            path.join(base, *parts) for parts in [
                ('DCIM', '100EOS5D2', 'MVI_5751.MOV'),
                ('DCIM', '100EOS5D2', 'MVI_5751.THM'),
                ('DCIM', '100EOS5D2', 'MVI_5752.MOV'),
            ]
        )

        inst = self.klass(self.busname, connect=False)
        c = SignalCapture()
        inst.connect('batch_import_started', c.on_batch_started)
        inst.connect('batch_import_finished', c.on_batch_finished)
        inst.connect('import_started', c.on_started)
        inst.connect('import_count', c.on_count)
        inst.connect('import_progress', c.on_progress)
        inst.connect('import_finished', c.on_finished)

        inst._connect_signals()
        mainloop = gobject.MainLoop()

        # DummyImporter should run for ~1 second:
        gobject.timeout_add(2000, mainloop.quit)

        self.assertEqual(inst.start_import(tmp.path), 'started')
        mainloop.run()


        self.assertEqual(len(c.signals), 8)

        self.assertEqual(c.signals[0],
            ('batch_started',)
        )
        self.assertEqual(c.signals[1],
            ('started', base, '4CXJKLJ3MXAVTNWYEPHTETHV')
        )
        self.assertEqual(c.signals[2],
            ('count', base, 3)
        )
        self.assertEqual(c.signals[3],
            ('progress', base, 1, 3,
                dict(src=files[0], action='imported', _id=sample_mov_hash)
            )
        )
        self.assertEqual(c.signals[4],
            ('progress', base, 2, 3,
                dict(src=files[1], action='imported', _id=sample_thm_hash)
            )
        )
        self.assertEqual(c.signals[5],
            ('progress', base, 3, 3,
                dict(src=files[2], action='skipped', _id=sample_mov_hash)
            )
        )
        self.assertEqual(c.signals[6],
            ('batch_finished',
                dict(
                    imported=2,
                    imported_bytes=(20202333 + 27328),
                    skipped=1,
                    skipped_bytes=20202333,
                )
            )
        )
        self.assertEqual(c.signals[7],
            ('finished', base,
                dict(
                    imported=2,
                    imported_bytes=(20202333 + 27328),
                    skipped=1,
                    skipped_bytes=20202333,
                )
            )
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
        return
        tmp = TempDir()
        nope = tmp.join('memory_card')
        inst = self.new()
        self.assertEqual(inst.start_import(nope), 'not_dir_or_file')
        self.assertEqual(inst.start_import('some/relative/path'), 'not_abspath')
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.start_import(tmp.path), 'already_running')

    def test_import_finished(self):
        return
        # Test that DMedia.ImportFinished is removing process from active
        # imports after it gets the ImportFinished signal from the queue:
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.list_imports(), [tmp.path])
        time.sleep(2)  # DummyImporter should run for ~1 second.
        self.assertEqual(inst.list_imports(), [])

    def test_stop_import(self):
        return
        tmp = TempDir()
        inst = self.new()
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')
        self.assertEqual(inst.start_import(tmp.path), 'started')
        self.assertEqual(inst.stop_import(tmp.path), 'stopped')
        self.assertEqual(inst.stop_import(tmp.path), 'not_running')

    def test_list_imports(self):
        return
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
