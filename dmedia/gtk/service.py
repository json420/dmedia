# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>
#   Manish SInha <mail@manishsinha.net>
#   David Green <david4dev@gmail.com>
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
D-Bus service implementing Pro File Import UX.

For details, please see:

  https://wiki.ubuntu.com/AyatanaDmediaLovefest
"""

from os import path
from gettext import gettext as _
import logging
from subprocess import check_call

import dbus
import dbus.service
import dbus.mainloop.glib

from dmedia import __version__
from dmedia.constants import IMPORT_BUS, IMPORT_IFACE, EXT_MAP
from dmedia.importer import ImportManager

from .util import NotifyManager, Timer, import_started, batch_finished


try:
    from gi.repository import Notify
    Notify.init('dmedia')
except ImportError:
    Notify = None

try:
    from gi.repository import AppIndicator3
    from gi.repository import Gtk
except ImportError:
    AppIndicator3 = None

log = logging.getLogger()


ICON = 'indicator-rendermenu'
ICON_ATT = 'indicator-rendermenu-att'


class DMedia(dbus.service.Object):
    __signals = frozenset([
        'BatchStarted',
        'BatchFinished',
        'ImportStarted',
        'ImportCount',
        'ImportProgress',
        'ImportFinished',
    ])

    def __init__(self, env, bus, killfunc):
        assert callable(killfunc)
        self._env = env
        self._bus = bus
        self._killfunc = killfunc
        self._bus = bus
        self._no_gui = env.get('no_gui', False)
        log.info('Starting service on %r', self._bus)
        self._conn = dbus.SessionBus()
        super(DMedia, self).__init__(self._conn, object_path='/')
        self._busname = dbus.service.BusName(self._bus, self._conn)

        if self._no_gui or Notify is None:
            self._notify = None
        else:
            log.info('Using `Notify`')
            self._notify = NotifyManager()

        if self._no_gui or AppIndicator3 is None:
            self._indicator = None
        else:
            log.info('Using `AppIndicator3`')
            self._indicator = AppIndicator3.Indicator.new('rendermenu', ICON,
                AppIndicator3.IndicatorCategory.APPLICATION_STATUS
            )
            self._timer = Timer(2, self._on_timer)
            self._indicator.set_attention_icon(ICON_ATT)
            self._menu = Gtk.Menu()

            self._current = Gtk.MenuItem()
            self._menu.append(self._current)

            sep = Gtk.SeparatorMenuItem()
            self._menu.append(sep)

            futon = Gtk.MenuItem()
            futon.set_label(_('Browse DB in Futon'))
            futon.connect('activate', self._on_futon)
            self._menu.append(futon)

            quit = Gtk.MenuItem()
            quit.set_label(_('Shutdown dmedia'))
            quit.connect('activate', self._on_quit)
            self._menu.append(quit)

            self._menu.show_all()
            self._current.hide()
            self._indicator.set_menu(self._menu)
            self._indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)

        self._manager = None

    @property
    def manager(self):
        if self._manager is None:
            self._manager = ImportManager(self._env, self._on_signal)
        return self._manager

    def _on_signal(self, signal, args):
        if signal in self.__signals:
            method = getattr(self, signal)
            method(*args)

    def _on_timer(self):
        if self._manager is None:
            return
        text = _('File {completed} of {total}').format(
            **self._manager.get_batch_progress()
        )
        self._current.set_label(text)
#        self._indicator.set_menu(self._menu)

    def _on_quit(self, menuitem):
        self.Kill()

    def _on_futon(self, menuitem):
        log.info('Opening dmedia database in Futon..')
        try:
            uri = self.metastore.get_auth_uri() + '/_utils'
            check_call(['/usr/bin/xdg-open', uri])
            log.info('Opened Futon')
        except Exception:
            log.exception('Could not open dmedia database in Futon')

    @dbus.service.signal(IMPORT_IFACE, signature='s')
    def BatchStarted(self, batch_id):
        """
        Fired at transition from idle to at least one active import.

        For pro file import UX, the RenderMenu should be set to STATUS_ATTENTION
        when this signal is received.
        """
        if self._notify:
            self._batch = []
        if self._indicator:
            self._indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)
            self._current.show()
            self._current.set_label(_('Searching for files...'))
            self._indicator.set_menu(self._menu)
            self._timer.start()

    @dbus.service.signal(IMPORT_IFACE, signature='sa{sx}')
    def BatchFinished(self, batch_id, stats):
        """
        Fired at transition from at least one active import to idle.

        *stats* will be the combined stats of all imports in this batch.

        For pro file import UX, the RenderMenu should be set back to
        STATUS_ACTIVE, and the NotifyOSD with the aggregate import stats should
        be displayed when this signal is received.
        """
        if self._indicator:
            self._indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        if self._notify is None:
            return
        (summary, body) = batch_finished(stats)
        self._notify.replace(summary, body, 'notification-device-eject')
        self._timer.stop()
        self._current.hide()

    @dbus.service.signal(IMPORT_IFACE, signature='ss')
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

    @dbus.service.signal(IMPORT_IFACE, signature='ssx')
    def ImportCount(self, base, import_id, total):
        pass

    @dbus.service.signal(IMPORT_IFACE, signature='ssiia{ss}')
    def ImportProgress(self, base, import_id, completed, total, info):
        pass

    @dbus.service.signal(IMPORT_IFACE, signature='ssa{sx}')
    def ImportFinished(self, base, import_id, stats):
        pass

    @dbus.service.method(IMPORT_IFACE, in_signature='', out_signature='')
    def Kill(self):
        """
        Kill the dmedia service process.
        """
        log.info('Killing service...')
        if self._manager is not None:
            self._manager.kill()
        if callable(self._killfunc):
            log.info('Calling killfunc()')
            self._killfunc()

    @dbus.service.method(IMPORT_IFACE, in_signature='', out_signature='s')
    def Version(self):
        """
        Return dmedia version.
        """
        return __version__

    @dbus.service.method(IMPORT_IFACE, in_signature='as', out_signature='as')
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

    @dbus.service.method(IMPORT_IFACE, in_signature='sb', out_signature='s')
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
        base = unicode(base)
        if path.abspath(base) != base:
            return 'not_abspath'
        if not path.isdir(base):
            return 'not_a_dir'
        if self.manager.start_import(base, extract):
            return 'started'
        return 'already_running'

    @dbus.service.method(IMPORT_IFACE, in_signature='s', out_signature='s')
    def StopImport(self, base):
        """
        In running, stop the import of directory or file at *base*.
        """
        if self.manager.kill_job(base):
            return 'stopped'
        return 'not_running'

    @dbus.service.method(IMPORT_IFACE, in_signature='', out_signature='as')
    def ListImports(self):
        """
        Return list of currently running imports.
        """
        if self._manager is None:
            return []
        return self.manager.list_jobs()