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
Makes dmedia functionality avaible over D-Bus.
"""

from dmedialib import __version__
from os import path
import time
import multiprocessing
import dbus
import dbus.service
from .constants import BUS, INTERFACE


def import_files(status, progress, base):
    status.put(
        (base, 'start')
    )
    time.sleep(1)  # Scan list of files
    count = 10
    progress.put(
        (base, 0, count)
    )
    for i in xrange(count):
        time.sleep(1)
        progress.put(
            (base, i + 1, count)
        )
    time.sleep(1)
    status.put(
        (base, 'finish')
    )


class DMedia(dbus.service.Object):
    def __init__(self, busname=None, killfunc=None):
        self._busname = (BUS if busname is None else busname)
        self._killfunc = killfunc
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self.__busname = dbus.service.BusName(self._busname, self._conn)
        self.__imports = {}
        self.__import_status = multiprocessing.Queue()
        self.__import_progress = multiprocessing.Queue()

    @dbus.service.method(INTERFACE, in_signature='', out_signature='')
    def kill(self):
        """
        Kill the dmedia service process.
        """
        if callable(self._killfunc):
            self._killfunc()

    @dbus.service.method(INTERFACE, in_signature='', out_signature='s')
    def version(self):
        """
        Return dmedia version.
        """
        return __version__

    @dbus.service.method(INTERFACE, in_signature='s', out_signature='s')
    def import_start(self, base):
        """
        Start import of directory or file at *base*.
        """
        if path.abspath(base) != base:
            return 'not_abspath'
        if not (path.isdir(base) or path.isfile(base)):
            return 'not_dir_or_file'
        if base in self.__imports:
            return 'already_running'
        p = multiprocessing.Process(
            target=import_files,
            args=(self.__import_status, self.__import_progress, base),
        )
        p.daemon = True
        self.__imports[base] = p
        p.start()
        return 'started'

    @dbus.service.method(INTERFACE, in_signature='s', out_signature='s')
    def import_stop(self, base):
        """
        In running, stop the import of directory or file at *base*.
        """
        if base in self.__imports:
            p = self.__imports.pop(base)
            p.terminate()
            return 'stopped'
        return 'not_running'

    @dbus.service.method(INTERFACE, in_signature='', out_signature='as')
    def import_list(self):
        """
        Return list of currently running imports.
        """
        return sorted(self.__imports)
