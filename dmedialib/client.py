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

    For example, use `Client.import_list()` to get the list of currently running
    imports:

    >>> from dmedialib.client import Client
    >>> client = Client()  #doctest: +SKIP
    >>> client.import_list()  #doctest: +SKIP
    []

    Start an import operation using `Client.import_start()`, after which you
    will see it in the list of running imports:

    >>> client.import_start('/media/EOS_DIGITAL')  #doctest: +SKIP
    'started'
    >>> client.import_list()  #doctest: +SKIP
    ['/media/EOS_DIGITAL']

    If you try to import a path for which an import operation is already in
    progress, `Client.import_start()` will return the status string
    ``'already_running'``:

    >>> client.import_start('/media/EOS_DIGITAL')  #doctest: +SKIP
    'already_running'

    Stop an import operation using `Client.import_stop()`, after which there
    will be no running imports:

    >>> client.import_stop('/media/EOS_DIGITAL')  #doctest: +SKIP
    'stopped'
    >>> client.import_list()  #doctest: +SKIP
    []

    If you try to stop an import operation that doesn't exist,
    `Client.import_stop()` will return the status string ``'not_running'``:

    >>> client.import_stop('/media/EOS_DIGITAL')  #doctest: +SKIP
    'not_running'

    Finally, you can shutdown the dmedia service with `Client.kill()`:

    >>> client.kill()  #doctest: +SKIP

    """

    __gsignals__ = {
        'import_progress': (
            gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, [gobject.TYPE_PYOBJECT]
        ),
        'import_status': (
            gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, [gobject.TYPE_PYOBJECT]
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
            'import_status', self._on_import_status, INTERFACE
        )
        self._proxy.connect_to_signal(
            'import_progress', self._on_import_progress, INTERFACE
        )

    def _on_import_status(self, base, status):
        self.emit('import_status',
            {
                'base': unicode(base),
                'status': unicode(status),
            }
        )

    def _on_import_progress(self, base, current, total):
        self.emit('import_progress',
            {
                'base': unicode(base),
                'current': int(current),
                'total': int(total),
            }
        )

    def kill(self):
        """
        Shutdown the dmedia daemon.
        """
        self._method('kill')()
        self.__proxy = None

    def version(self):
        """
        Return version number of running dmedia daemon.
        """
        return self._method('version')()

    def get_extensions(self, types):
        """
        Get a list of extensions based on broad categories in *types*.

        Currently recognized categories include ``'video'``, ``'audio'``,
        ``'images'``, and ``'all'``.  You can safely include categories that
        don't yet exist.

        :param types: A list of general categories, e.g. ``['video', 'audio']``
        """
        return self._method('get_extensions')(types)

    def import_start(self, base, extensions=None):
        """
        Start import of directory or file at *base*, matching *extensions*.

        If *extensions* is ``None`` (the default), the set defined in the
        `EXTENSIONS` constant will be used.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        :param extensions: List (or other iterable) of file extensions to match,
            e.g. ``['mov', 'cr2', 'wav']``
        """
        extensions = (list(EXTENSIONS) if extensions is None else extensions)
        return self._method('import_start')(base, extensions)

    def import_stop(self, base):
        """
        In running, stop the import of directory or file at *base*.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        """
        return self._method('import_stop')(base)

    def import_list(self):
        """
        Return list of currently running imports.
        """
        return self._method('import_list')()
