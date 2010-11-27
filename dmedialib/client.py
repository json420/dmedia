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
                self._connect_connect()
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
