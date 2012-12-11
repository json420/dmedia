# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Helpers to perform background task but using the GLib mainloop for communication.
"""

import multiprocessing
import logging
import time

from dmedia.parallel import start_thread, start_process
from dmedia.core import snapshot_worker

from gi.repository import GLib


log = logging.getLogger()


class Snapshots:
    def __init__(self, env, dumpdir, callback):
        self.env = env
        self.dumpdir = dumpdir
        self.callback = callback
        self.in_q = multiprocessing.Queue()
        self.out_q = multiprocessing.Queue()
        self.in_flight = set()
        self.process = None
        self.thread = None

    def start(self):
        assert self.process is None
        assert self.thread is None
        self.process = start_process(snapshot_worker,
            self.env,
            self.dumpdir,
            self.in_q,
            self.out_q,
        )
        self.thread = start_thread(self.listener_thread)

    def shutdown(self):
        self.in_q.put(None)
        self.process.join()
        self.process = None
        self.thread.join()
        self.thread = None

    def listener_thread(self):
        while True:
            item = self.out_q.get()
            if item is None:
                break
            GLib.idle_add(self.on_complete, item)

    def on_complete(self, item):
        (name, success) = item
        self.in_flight.remove(name)
        if not self.in_flight:
            log.info('No in-flight snapshots, shutting down worker...')
            self.shutdown()
        self.callback(name, success)

    def run(self, name):
        if name in self.in_flight:
            log.warning('%r already in-flight', name)
            return False
        self.in_flight.add(name)
        if self.process is None:
            self.start()
        self.in_q.put(name)
        return True
