# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Manish SInha <mail@manishsinha.net>
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
from threading import Thread
import multiprocessing
from Queue import Empty
import dbus
import dbus.service
from .constants import BUS, INTERFACE, EXT_MAP
from .importer import import_files


def dummy_import_files(q, base, extensions):
    # Note the dummy import will take approximately 2 seconds to complete
    q.put(['ImportStarted', base])
    time.sleep(1)  # Scan list of files
    count = 4
    q.put(['ImportProgress', base, 0, count])
    for i in xrange(count):
        q.put(['ImportProgress', base, i + 1, count])
    time.sleep(1)
    q.put(['ImportFinished', base])



class DMedia(dbus.service.Object):
    __signals = frozenset([
        'ImportStarted',
        'ImportFinished',
        'ImportProgress',
    ])

    def __init__(self, busname=None, killfunc=None, dummy=False):
        self._busname = (BUS if busname is None else busname)
        self._killfunc = killfunc
        self._dummy = dummy
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self.__busname = dbus.service.BusName(self._busname, self._conn)
        self.__imports = {}
        self.__running = True
        self.__queue = multiprocessing.Queue()
        self.__thread = Thread(target=self._signal_thread)
        self.__thread.daemon = True
        self.__thread.start()

    def _signal_thread(self):
        while self.__running:
            try:
                msg = self.__queue.get(timeout=1)
                signal = msg[0]
                if signal not in self.__signals:
                    continue
                method = getattr(self, signal, None)
                if callable(method):
                    args = msg[1:]
                    method(*args)
            except Empty:
                pass

    @dbus.service.signal(INTERFACE, signature='s')
    def ImportStarted(self, base):
        pass

    @dbus.service.signal(INTERFACE, signature='s')
    def ImportFinished(self, base):
        p = self.__imports.pop(base, None)
        if p is not None:
            p.join()  # Sanity check to make sure worker is terminating

    @dbus.service.signal(INTERFACE, signature='sii')
    def ImportProgress(self, base, current, total):
        pass

    @dbus.service.method(INTERFACE, in_signature='', out_signature='')
    def Kill(self):
        """
        Kill the dmedia service process.
        """
        self.__running = False
        self.__thread.join()  # Cleanly shutdown _signal_thread
        for p in self.__imports.values():
            p.terminate()
            p.join()
        if callable(self._killfunc):
            self._killfunc()

    @dbus.service.method(INTERFACE, in_signature='', out_signature='s')
    def Version(self):
        """
        Return dmedia version.
        """
        return __version__

    @dbus.service.method(INTERFACE, in_signature='as', out_signature='as')
    def GetExtensions(self, types):
        """
        Get a list of extensions based on broad categories in *types*.

        Currently recognized categories include ``'video'``, ``'audio'``,
        ``'images'``, and ``'all'``.  You can safely include categories that
        don't yet exist.

        :param types: A list of general categories, e.g. ``['video', 'audio']``
        """
        extensions = set()
        for key in types:
            if key in EXT_MAP:
                extensions.update(EXT_MAP[key])
        return sorted(extensions)

    @dbus.service.method(INTERFACE, in_signature='sas', out_signature='s')
    def StartImport(self, base, extensions):
        """
        Start import of directory or file at *base*, matching *extensions*.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        :param extensions: List of file extensions to match, e.g.
            ``['mov', 'cr2', 'wav']``
        """
        if path.abspath(base) != base:
            return 'not_abspath'
        if not (path.isdir(base) or path.isfile(base)):
            return 'not_dir_or_file'
        if base in self.__imports:
            return 'already_running'
        p = multiprocessing.Process(
            target=(dummy_import_files if self._dummy else import_files),
            args=(self.__queue, base, frozenset(extensions)),
        )
        p.daemon = True
        self.__imports[base] = p
        p.start()
        return 'started'

    @dbus.service.method(INTERFACE, in_signature='s', out_signature='s')
    def StopImport(self, base):
        """
        In running, stop the import of directory or file at *base*.
        """
        if base in self.__imports:
            p = self.__imports.pop(base)
            p.terminate()
            p.join()
            return 'stopped'
        return 'not_running'

    @dbus.service.method(INTERFACE, in_signature='', out_signature='as')
    def ListImports(self):
        """
        Return list of currently running imports.
        """
        return sorted(self.__imports)
