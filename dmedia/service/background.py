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

from gi.repository import GLib
from microfiber import BulkConflict

from dmedia.parallel import start_thread, start_process
from dmedia.core import snapshot_worker


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


class LazyAccess:
    """
    Lazily update doc['atime'] after a file access.

    The class solves two problems:

    First, we don't want to do any CouchDB writes inside a call to
    Dmedia.Resolve() or Dmedia.ResolveMany() because that has too big a
    performance hit. So we want to update the atime only when the mainloop is
    idle.

    Second, we want to limit the frequency of doc updates.  It's quite common
    (especially in Novacut) for the same file to be resolved many times in a
    short period of time.  This class will only write out the atime updates at
    most once every 30 seconds, writing out only the latest access time in cases
    where the file was resolved multiple times during those 30 seconds.

    So that conflicts are less problematic, all the docs for all access files
    are retrieved just before the flush, using Database.get_many().
    Non-existent docs, if any, are ignored.

    Then the atime of all the docs is set, and they are saved back to CouchDB
    using Database.save_many().  Any conflicts created between the get and save
    are ignored, although logged.
    """
    def __init__(self, db, seconds=15):
        self.db = db
        self.delay = seconds * 1000
        self.buf = {}
        self.timeout_id = None

    def access(self, _id):
        self.buf[_id] = int(time.time())
        if len(self.buf) >= 500:
            log.warning('Doing a synchronous atime flush')
            self.flush()
        elif self.timeout_id is None:
            self.timeout_id = GLib.timeout_add(self.delay, self.on_timeout)

    def on_timeout(self):
        assert self.timeout_id is not None
        self.timeout_id = None
        self.flush()

    def flush(self):
        if self.buf:
            log.info('Flushing atime updates for %d files', len(self.buf))
            ids = sorted(self.buf)
            docs = list(filter(None, self.db.get_many(ids)))
            for doc in docs:
                doc['atime'] = self.buf[doc['_id']]
            try:
                self.db.save_many(docs)
            except BulkConflict as e:
                log.exception('Conflicts in LazyAccess.flush()')
            self.buf.clear()
            return len(docs)

