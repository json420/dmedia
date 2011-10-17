# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
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
Wrap callbacks from dmedia core into signal emitting GObjects.

The "signaling" used the dmedia core is very simple... a given event producer
is optionally passed a callback with a signature like this:

>>> def callback(signal, args):
...     print(signal, args)
...

Where *args* is a tuple (possibly empty) containing the arguments for this
signal.  This is great for keeping the core simple, but at the UI level often we
need an event to have multiple consumers, so we wrap this in a GObject when
needed.
"""

import time
import logging

from gi.repository import GObject
from gi.repository.GObject import TYPE_PYOBJECT

from dmedia.importer import ImportManager
from dmedia.service import dbus


log = logging.getLogger()


class GImportManager(GObject.GObject):
    """
    Wrap signals from `dmedia.importer.ImportManager`
    """

    __gsignals__ = {
        'batch_started': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
        'import_started': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'batch_progress': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'batch_finished': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
    }

    _autoemit = (
        'batch_started',
        'import_started',
        'batch_progress',
    )

    def __init__(self, env):
        super().__init__()
        self.manager = ImportManager(env, self._callback)
        self.udisks = dbus.UDisks()
        self.udisks.monitor()
        self.udisks.connect('card_inserted', self._on_card_inserted)
        self._blocking = False
        self._cards = []

    def _callback(self, signal, args):
        if signal in self._autoemit:
            self.emit(signal, *args)
        elif signal == 'batch_finished':
            self._on_batch_finished(*args)

    def _on_batch_finished(self, batch_id, stats):
        Worker = dbus.Ejector
        self._blocking = True
        self._batch_id = batch_id
        self._stats = stats
        self._workers = dict(
            (obj, Worker(obj, self._on_complete))
            for obj in self._cards
        )
        self._cards = []
        for worker in self._workers.values():
            worker.run()

    def _on_complete(self, worker, obj):
        log.info('Completed: %r', worker)
        w = self._workers.pop(obj)
        assert worker is w
        if len(self._workers) == 0:
            self._blocking = False
            self.emit('batch_finished', self._batch_id, self._stats)

    def _on_card_inserted(self, udisks, obj, parentdir, partition, drive):
        if self._blocking:
            log.warning('Blocking, ignoring card-insert %r', obj)
            return
        log.info('card-insert %r', obj)
        self._cards.append(obj)
        info = dbus.extra_info(partition, drive)
        self.manager.start_import(parentdir, info)


