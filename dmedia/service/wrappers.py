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

import weakref

from gi.repository import GObject
from gi.repository.GObject import TYPE_PYOBJECT


class Wrapper(GObject.GObject):
    """
    Wrap signals from a `dmedia.workers.Manager`.
    """

    def callback(self, signal, args):
        self.emit_mainthread(signal, *args)

    def emit_mainthread(self, signal, *args):
        GObject.idle_add(self.emit, signal, *args)



class ImportWrapper(Wrapper):
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
            # (batch_id, stats, copies)
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'batch_finalized': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # (batch_id, stats, copies)
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'error': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
    }


