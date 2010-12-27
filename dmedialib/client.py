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
Convenience wrapper for Python applications talking to dmedia dbus service.
"""

import dbus
import dbus.mainloop.glib
import gobject
from gobject import TYPE_PYOBJECT
from .constants import BUS, INTERFACE, EXTENSIONS


# We need mainloop integration to test signals:
dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)


class Client(gobject.GObject):
    """
    Simple and Pythonic way to control dmedia dbus service.

    For Python applications, this client provides several advantages over
    strait dbus because it:

      1. Lazily starts the dmedia service the first time you call a dbus method
         or connect a signal handler

      2. More Pythonic API, including default argument values where they make
         sense

      3. Can use convenient gobject signals

    Controlling import operations
    =============================

    The dmedia service can have multiple import operations running at once.
    Import jobs are identified by the path of the directory being imported.

    For example, use `Client.list_imports()` to get the list of currently running
    imports:

    >>> from dmedialib.client import Client
    >>> client = Client()  #doctest: +SKIP
    >>> client.list_imports()  #doctest: +SKIP
    []

    Start an import operation using `Client.start_import()`, after which you
    will see it in the list of running imports:

    >>> client.start_import('/media/EOS_DIGITAL')  #doctest: +SKIP
    'started'
    >>> client.list_imports()  #doctest: +SKIP
    ['/media/EOS_DIGITAL']

    If you try to import a path for which an import operation is already in
    progress, `Client.start_import()` will return the status string
    ``'already_running'``:

    >>> client.start_import('/media/EOS_DIGITAL')  #doctest: +SKIP
    'already_running'

    Stop an import operation using `Client.stop_import()`, after which there
    will be no running imports:

    >>> client.stop_import('/media/EOS_DIGITAL')  #doctest: +SKIP
    'stopped'
    >>> client.list_imports()  #doctest: +SKIP
    []

    If you try to stop an import operation that doesn't exist,
    `Client.stop_import()` will return the status string ``'not_running'``:

    >>> client.stop_import('/media/EOS_DIGITAL')  #doctest: +SKIP
    'not_running'

    Finally, you can shutdown the dmedia service with `Client.kill()`:

    >>> client.kill()  #doctest: +SKIP

    """

    __gsignals__ = {
        'batch_started': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
        'batch_finished': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'import_started': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'import_count': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'import_progress': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT,
            TYPE_PYOBJECT]
        ),
        'import_finished': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
    }

    def __init__(self, bus=None):
        super(Client, self).__init__()
        self._bus = (BUS if bus is None else bus)
        self._conn = dbus.SessionBus()
        self._proxy = None

    @property
    def proxy(self):
        """
        Lazily create proxy object so dmedia service starts only when needed.
        """
        if self._proxy is None:
            self._proxy = self._conn.get_object(self._bus, '/')
            self._connect_signals()
        return self._proxy

    def _method(self, name):
        return self.proxy.get_dbus_method(name, dbus_interface=INTERFACE)

    def _connect_signals(self):
        self.proxy.connect_to_signal(
            'BatchStarted', self._on_BatchStarted, INTERFACE
        )
        self.proxy.connect_to_signal(
            'BatchFinished', self._on_BatchFinished, INTERFACE
        )
        self.proxy.connect_to_signal(
            'ImportStarted', self._on_ImportStarted, INTERFACE
        )
        self.proxy.connect_to_signal(
            'ImportCount', self._on_ImportCount, INTERFACE
        )
        self.proxy.connect_to_signal(
            'ImportProgress', self._on_ImportProgress, INTERFACE
        )
        self.proxy.connect_to_signal(
            'ImportFinished', self._on_ImportFinished, INTERFACE
        )

    def _on_BatchStarted(self, batch_id):
        self.emit('batch_started', batch_id)

    def _on_BatchFinished(self, batch_id, stats):
        self.emit('batch_finished', batch_id, stats)

    def _on_ImportStarted(self, base, import_id):
        self.emit('import_started', base, import_id)

    def _on_ImportCount(self, base, import_id, total):
        self.emit('import_count', base, import_id, total)

    def _on_ImportProgress(self, base, import_id, completed, total, info):
        self.emit('import_progress', base, import_id, completed, total, info)

    def _on_ImportFinished(self, base, import_id, stats):
        self.emit('import_finished', base, import_id, stats)

    def connect(self, *args, **kw):
        super(Client, self).connect(*args, **kw)
        if self._proxy is None:
            self.proxy

    def kill(self):
        """
        Shutdown the dmedia daemon.
        """
        self._method('Kill')()
        self._proxy = None

    def version(self):
        """
        Return version number of running dmedia daemon.
        """
        return self._method('Version')()

    def get_extensions(self, types):
        """
        Get a list of extensions based on broad categories in *types*.

        Currently recognized categories include ``'video'``, ``'audio'``,
        ``'images'``, and ``'all'``.  You can safely include categories that
        don't yet exist.

        :param types: A list of general categories, e.g. ``['video', 'audio']``
        """
        return self._method('GetExtensions')(types)

    def start_import(self, base, extract=True):
        """
        Start import of card mounted at *base*.

        If *extract* is ``True`` (the default), metadata will be extracted and
        thumbnails generated.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        :param extract: If ``True``, perform metadata extraction, thumbnail
            generation; default is ``True``.
        """
        return self._method('StartImport')(base, extract)

    def stop_import(self, base):
        """
        Start import of card mounted at *base*.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        """
        return self._method('StopImport')(base)

    def list_imports(self):
        """
        Return list of currently running imports.
        """
        return self._method('ListImports')()
