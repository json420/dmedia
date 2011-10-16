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

from gi.repository import GObject
from gi.repository.GObject import TYPE_PYOBJECT

from dmedia.importer import ImportManager
from dmedia.service import dbus


def extra_info(partition, drive):
    return {
        'partition': {
            'uuid': partition['IdUuid'],
            'bytes': partition['DeviceSize'],
            'filesystem': partition['IdType'],
            'filesystem_version': partition['IdVersion'],
            'label': partition['IdLabel'],
            'number': partition['PartitionNumber'],
        },
        'drive': {
            'serial': drive['DriveSerial'],
            'bytes': drive['DeviceSize'],
            'block_bytes': drive['DeviceBlockSize'],
            'vendor': drive['DriveVendor'],
            'model': drive['DriveModel'],
            'revision': drive['DriveRevision'],
            'rotational': drive['DriveIsRotational'],
            'partition_scheme': drive['PartitionTableScheme'],
            'internal': drive['DeviceIsSystemInternal'],
            'connection': drive['DriveConnectionInterface'],
        },
    }


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
        'batch_finished',
    )

    def __init__(self, env):
        super().__init__()
        self.manager = ImportManager(env, self._callback)
        self.udisks = dbus.UDisks()
        self.udisks.monitor()
        self.udisks.connect('card_inserted', self._on_card_inserted)

    def _callback(self, signal, args):
        if signal in self._autoemit:
            self.emit(signal, *args)

    def _on_card_inserted(self, udisks, obj, parentdir, partition, drive):
        info = extra_info(partition, drive)
        self.manager.start_import(parentdir, info)


