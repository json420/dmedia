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

      2. More Pythonic API, including default argument values where they make
         since

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
        'import_started': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
        'import_count': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'import_progress': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'import_finished': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
    }

    def __init__(self, busname=None, connect=True):
        super(Client, self).__init__()
        self._busname = (BUS if busname is None else busname)
        self._connect = connect
        self._conn = dbus.SessionBus()
        self.__proxy = None

    @property
    def _proxy(self):
        """
        Lazily create proxy object so dmedia service starts only when needed.
        """
        if self.__proxy is None:
            self.__proxy = self._conn.get_object(self._busname, '/')
            if self._connect:
                self._connect_signals()
        return self.__proxy

    def _method(self, name):
        return self._proxy.get_dbus_method(name, dbus_interface=INTERFACE)

    def _connect_signals(self):
        self._proxy.connect_to_signal(
            'ImportStarted', self._on_ImportStarted, INTERFACE
        )
        self._proxy.connect_to_signal(
            'ImportCount', self._on_ImportCount, INTERFACE
        )
        self._proxy.connect_to_signal(
            'ImportProgress', self._on_ImportProgress, INTERFACE
        )
        self._proxy.connect_to_signal(
            'ImportFinished', self._on_ImportFinished, INTERFACE
        )

    def _on_ImportStarted(self, base):
        self.emit('import_started', base)

    def _on_ImportCount(self, base, total):
        self.emit('import_count', base, total)

    def _on_ImportProgress(self, base, completed, total, info):
        self.emit('import_progress', base, completed, total, info)

    def _on_ImportFinished(self, base, stats):
        self.emit('import_finished', base, stats)

    def kill(self):
        """
        Shutdown the dmedia daemon.
        """
        self._method('Kill')()
        self.__proxy = None

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

    def start_import(self, base):
        """
        Start import of card mounted at *base*.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        """
        return self._method('StartImport')(base)

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
