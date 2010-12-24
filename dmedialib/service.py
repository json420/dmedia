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
import dbus
import dbus.service
import dbus.mainloop.glib
import gobject
from .constants import BUS, INTERFACE, EXT_MAP
from .util import NotifyManager, import_started, batch_import_finished
from .importer import ImportManager

gobject.threads_init()
dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

try:
    import pynotify
    pynotify.init('dmedia')
except ImportError:
    pynotify = None

try:
    import appindicator
    import gtk
except ImportError:
    appindicator = None


ICON = '/usr/share/pixmaps/dmedia/indicator-rendermenu.svg'
ICON_ATT = '/usr/share/pixmaps/dmedia/indicator-rendermenu-att.svg'


class DMedia(dbus.service.Object):
    __signals = frozenset([
        'ImportStarted',
        'ImportCount',
        'ImportProgress',
        'ImportFinished',
    ])

    def __init__(self, killfunc=None, bus=None, couchdir=None, no_gui=False):
        self._killfunc = killfunc
        self._bus = (BUS if bus is None else bus)
        self._couchdir = couchdir
        self._no_gui = no_gui
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self._busname = dbus.service.BusName(self._bus, self._conn)

        if no_gui or pynotify is None:
            self._notify = None
        else:
            self._notify = NotifyManager()
        if no_gui or appindicator is None:
            self._indicator = None
        else:
            self._indicator = appindicator.Indicator('rendermenu', ICON,
                appindicator.CATEGORY_APPLICATION_STATUS
            )
            self._indicator.set_attention_icon(ICON_ATT)
            self._menu = gtk.Menu()
            self._indicator.set_menu(self._menu)
            self._indicator.set_status(appindicator.STATUS_ACTIVE)

        self._manager = None

    @property
    def manager(self):
        if self._manager is None:
            self._manager = ImportManager(self._on_signal, self._couchdir)
            self._manager.start()
        return self._manager

    def _on_signal(self, signal, args):
        pass


    @dbus.service.signal(INTERFACE, signature='s')
    def BatchImportStarted(self, batch_id):
        """
        Fired at transition from idle to at least one active import.

        For pro file import UX, the RenderMenu should be set to STATUS_ATTENTION
        when this signal is received.
        """
        if self._indicator:
            self._indicator.set_status(appindicator.STATUS_ATTENTION)

    @dbus.service.signal(INTERFACE, signature='sa{sx}')
    def BatchImportFinished(self, batch_id, stats):
        """
        Fired at transition from at least one active import to idle.

        *stats* will be the combined stats of all imports in this batch.

        For pro file import UX, the RenderMenu should be set back to
        STATUS_ACTIVE, and the NotifyOSD with the aggregate import stats should
        be displayed when this signal is received.
        """
        if self._indicator:
            self._indicator.set_status(appindicator.STATUS_ACTIVE)
        if self._notify is None:
            return
        self._batch = []
        (summary, body) = batch_import_finished(stats)
        self._notify.replace(summary, body, 'notification-device-eject')

    @dbus.service.signal(INTERFACE, signature='ss')
    def ImportStarted(self, base, import_id):
        """
        Fired when card is inserted.

        For pro file import UX, the "Searching for new files" NotifyOSD should
        be displayed when this signal is received.  If a previous notification
        is still visible, the two should be merge and the summary conspicuously
        changed to be very clear that both cards were detected.
        """
        if self._notify is None:
            return
        self._batch.append(base)
        (summary, body) = import_started(self._batch)
        # FIXME: use correct icon depending on whether card reader is corrected
        # via FireWire or USB
        self._notify.replace(summary, body, 'notification-device-usb')

    @dbus.service.signal(INTERFACE, signature='ssx')
    def ImportCount(self, base, import_id, total):
        pass

    @dbus.service.signal(INTERFACE, signature='ssiia{ss}')
    def ImportProgress(self, base, import_id, completed, total, info):
        pass

    @dbus.service.signal(INTERFACE, signature='ssa{sx}')
    def ImportFinished(self, base, import_id, stats):
        p = self.__imports.pop(base, None)
        if p is not None:
            p.join()  # Sanity check to make sure worker is terminating

        for key in self.__stats:
            self.__stats[key] += stats[key]
        if len(self.__imports) == 0:
            self.BatchImportFinished(self.__stats)
            self.__stats = None

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

    @dbus.service.method(INTERFACE, in_signature='sb', out_signature='s')
    def StartImport(self, base, extract):
        """
        Start import of card mounted at *base*.

        If *extract* is ``True``, metadata will be extracted and thumbnails
        generated.

        :param base: File-system path from which to import, e.g.
            ``'/media/EOS_DIGITAL'``
        :param extract: If ``True``, perform metadata extraction, thumbnail
            generation
        """
        if path.abspath(base) != base:
            return 'not_abspath'
        if not (path.isdir(base) or path.isfile(base)):
            return 'not_dir_or_file'
        if base in self.__imports:
            return 'already_running'
        p = self._create_worker('import_files', base, extract)
        if len(self.__imports) == 0:
            self.__stats = dict(
                imported=0,
                imported_bytes=0,
                skipped=0,
                skipped_bytes=0,
            )
            if self._db is not None:
                doc = create_batchimport()
                self._batch_id = doc['_id']
                self._db[self._batch_id] = doc
            self.BatchImportStarted()
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
