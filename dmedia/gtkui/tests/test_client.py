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
Unit tests for `dmedia.client` module.
"""

import os
from os import path
from subprocess import Popen
import time
import json

import dbus
from dbus.proxies import ProxyObject
from gi.repository import GObject

import dmedia
from dmedia.constants import VIDEO, AUDIO, IMAGE, EXTENSIONS
from dmedia.gtkui import client, service

from dmedia.tests.helpers import TempDir, random_bus, prep_import_source
from dmedia.tests.helpers import sample_mov, sample_thm
from dmedia.tests.helpers import mov_hash, thm_hash
from dmedia.tests.couch import CouchCase


tree = path.dirname(path.dirname(path.abspath(dmedia.__file__)))
assert path.isfile(path.join(tree, 'setup.py'))
script = path.join(tree, 'dmedia-import-service')
assert path.isfile(script)


class CaptureCallback(object):
    def __init__(self, signal, messages):
        self.signal = signal
        self.messages = messages
        self.callback = None

    def __call__(self, *args):
        self.messages.append(
            (self.signal,) + args
        )
        if callable(self.callback):
            self.callback()


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

            dmedia-service --bus org.test3ISHAWZVSWVN5I5S.DMedia

        How do people usually unit test dbus services?  This works, but not sure
        if there is a better idiom in common use.  --jderose
        """
        super(test_Client, self).setUp()
        self.bus = random_bus()
        cmd = [script, '--no-gui',
            '--bus', self.bus,
            '--env', json.dumps(self.env),
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
        return self.klass(bus=self.bus)

    def test_init(self):
        # Test with no bus
        inst = self.klass()
        self.assertEqual(inst._bus, 'org.freedesktop.DMedia.Import')
        self.assertTrue(isinstance(inst._conn, dbus.SessionBus))
        self.assertTrue(inst._proxy is None)

        # Test with random bus
        inst = self.new()
        self.assertEqual(inst._bus, self.bus)
        self.assertTrue(isinstance(inst._conn, dbus.SessionBus))
        self.assertTrue(inst._proxy is None)

        # Test the proxy property
        p = inst.proxy
        self.assertTrue(isinstance(p, ProxyObject))
        self.assertTrue(p is inst._proxy)
        self.assertTrue(p is inst.proxy)

        # Test version()
        self.assertEqual(inst.version(), dmedia.__version__)

        # Test get_extensions()
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

    def test_connect(self):
        def callback(*args):
            pass

        inst = self.new()
        self.assertEqual(inst._proxy, None)
        inst.connect('import_started', callback)
        self.assertTrue(isinstance(inst._proxy, ProxyObject))
        self.assertTrue(inst._proxy is inst.proxy)

    def test_kill(self):
        inst = self.new()
        self.assertEqual(self.service.poll(), None)
        inst.kill()
        self.assertTrue(inst._proxy is None)
        time.sleep(1)  # Give dmedia-service time to shutdown
        self.assertEqual(self.service.poll(), 0)

    def test_import(self):
        inst = self.new()
        signals = SignalCapture(inst,
            'batch_started',
            'import_started',
            'import_count',
            'import_progress',
            'import_finished',
            'batch_finished',
        )
        mainloop = GObject.MainLoop()
        signals.handlers['batch_finished'].callback = mainloop.quit

        tmp = TempDir()
        base = tmp.path

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
        (src1, src2, dup1) = prep_import_source(tmp)
        mov_size = path.getsize(sample_mov)
        thm_size = path.getsize(sample_thm)
        self.assertEqual(inst.list_imports(), [])
        self.assertEqual(inst.stop_import(base), 'not_running')
        self.assertEqual(inst.start_import(base), 'started')
        self.assertEqual(inst.start_import(base), 'already_running')
        self.assertEqual(inst.list_imports(), [base])

        # mainloop.quit() gets called at 'batch_finished' signal
        mainloop.run()

        self.assertEqual(inst.list_imports(), [])
        self.assertEqual(inst.stop_import(base), 'not_running')

        self.assertEqual(len(signals.messages), 8)
        batch_id = signals.messages[0][2]
        self.assertEqual(
            signals.messages[0],
            ('batch_started', inst, batch_id)
        )
        import_id = signals.messages[1][3]
        self.assertEqual(
            signals.messages[1],
            ('import_started', inst, base, import_id)
        )
        self.assertEqual(
            signals.messages[2],
            ('import_count', inst, base, import_id, 3)
        )
        self.assertEqual(
            signals.messages[3],
            ('import_progress', inst, base, import_id, 1, 3,
                dict(action='imported', src=src1, _id=mov_hash)
            )
        )
        self.assertEqual(
            signals.messages[4],
            ('import_progress', inst, base, import_id, 2, 3,
                dict(action='imported', src=src2, _id=thm_hash)
            )
        )
        self.assertEqual(
            signals.messages[5],
            ('import_progress', inst, base, import_id, 3, 3,
                dict(action='skipped', src=dup1, _id=mov_hash)
            )
        )
        self.assertEqual(
            signals.messages[6],
            ('import_finished', inst, base, import_id,
                dict(
                    imported=2,
                    imported_bytes=(mov_size + thm_size),
                    skipped=1,
                    skipped_bytes=mov_size,
                )
            )
        )
        self.assertEqual(
            signals.messages[7],
            ('batch_finished', inst, batch_id,
                dict(
                    imported=2,
                    imported_bytes=(mov_size + thm_size),
                    skipped=1,
                    skipped_bytes=mov_size,
                )
            )
        )
