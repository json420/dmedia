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
import gobject
from .constants import BUS, INTERFACE, EXTENSIONS


class Client(gobject.GObject):
    __gsignals__ = {
        'import_progress': (
            gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, [gobject.TYPE_PYOBJECT]
        ),
    }

    def __init__(self, busname=None):
        super(Client, self).__init__()
        self._busname = (BUS if busname is None else busname)
        self._conn = dbus.SessionBus()
        self.__proxy = None

    @property
    def _proxy(self):
        """
        Lazily create proxy object so dmedia service starts only when needed.
        """
        if self.__proxy is None:
            self.__proxy = self._conn.get_object(self._busname, '/')
        return self.__proxy

    def kill(self):
        """
        Shutdown the dmedia daemon.
        """
        self._proxy.kill()

    def version(self):
        """
        Return version number of running dmedia daemon.
        """
        return self._proxy.version()

    def get_extensions(self, types):
        """
        Get a list of extensions based on broad categories in *types*.

        Currently recognized categories include ``'video'``, ``'audio'``,
        ``'images'``, and ``'all'``.  You can safely include categories that
        don't yet exist.

        :param types: A list of general categories, e.g. ``['video', 'audio']``
        """
        return self._proxy.get_extensions(types)

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
        return self._proxy.import_start(base, extensions)

    def import_stop(self, base):
        """
        In running, stop the import of directory or file at *base*.
        """
        return self._proxy.import_stop(base)

    def import_list(self):
        """
        Return list of currently running imports.
        """
        return self._proxy.import_list()
