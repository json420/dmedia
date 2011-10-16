# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
Some Ubuntu-specific UI bits.
"""

from gi.repository import Gtk, Notify, AppIndicator3, Unity

from dmedia.importer import notify_started, notify_stats


Notify.init('dmedia')
ICON = 'indicator-dmedia'
ICON_ATT = 'indicator-dmedia-att'


class NotifyManager:
    """
    Helper class to make it easier to update notification when still visible.
    """

    def __init__(self, klass=None):
        self._klass = (Notify.Notification if klass is None else klass)
        self._current = None

    def _on_closed(self, notification):
        assert self._current is notification
        self._current = None

    def isvisible(self):
        """
        Return ``True`` in a notification is currently visible.
        """
        return (self._current is not None)

    def notify(self, summary, body=None, icon=None):
        """
        Display a notification with *summary*, *body*, and *icon*.
        """
        assert self._current is None
        self._current = self._klass.new(summary, body, icon)
        self._current.connect('closed', self._on_closed)
        self._current.show()

    def update(self, summary, body=None, icon=None):
        """
        Update current notification to have *summary*, *body*, and *icon*.

        This method will only work if the current notification is still visible.

        To a display new or replace the existing notification regardless whether
        the current notification is visible, use
        `NotifyManager.replace()`.
        """
        assert self._current is not None
        self._current.update(summary, body, icon)
        self._current.show()

    def replace(self, summary, body=None, icon=None):
        """
        Update current notification if visible, otherwise display a new one.
        """
        if self.isvisible():
            self.update(summary, body, icon)
        else:
            self.notify(summary, body, icon)


class UnityImportUX:
    def __init__(self):
        self.launcher = Unity.LauncherEntry.get_for_desktop_id('dmedia.desktop')
        self.notify = NotifyManager()
        self.indicator = AppIndicator3.Indicator.new('dmedia', ICON,
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS
        )
        self.indicator.set_attention_icon(ICON_ATT)
        self.menu = Gtk.Menu()
        close = Gtk.MenuItem()
        close.set_label(_('Close'))
        close.connect('activate', self.on_close)
        self.menu.append(close)
        self.menu.show_all()
        self.indicator.set_menu(self.menu)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        self.manager = ImportManager(env, self.on_callback)
        self.handlers = {
            'batch_started': self.on_batch_started,
            'import_started': self.on_import_started,
            'batch_finished': self.on_batch_finished,
            'batch_progress': self.on_batch_progress,
        }
        self.window = Gtk.Window()
        self.window.connect('destroy', self.on_close)
        self.window.show_all()

    def on_close(self, button):
        Gtk.main_quit()

    def on_callback(self, signal, args):
        print(signal, *args)
        try:
            handler = self.handlers[signal]
            handler(*args)
        except KeyError:
            pass

    def on_batch_started(self, batch_id):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)
        self.launcher.set_property('count', 0)
        self.launcher.set_property('count_visible', True)
        self.launcher.set_property('progress', 0.0)
        self.launcher.set_property('progress_visible', True)
        self.basedirs = []

    def on_import_started(self, basedir, import_id):
        self.basedirs.append(basedir)
        (summary, body) = notify_started(self.basedirs)
        self.notify.replace(summary, body, None)

    def on_batch_progress(self, count, total_count, size, total_size):
        self.launcher.set_property('count', total_count)
        progress = (0.0 if total_size == 0 else size / total_size)
        self.launcher.set_property('progress', progress)

    def on_batch_finished(self, batch_id, stats):
        self.launcher.set_property('count_visible', False)
        self.launcher.set_property('progress_visible', False)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        (summary, body) = notify_stats(stats)
        self.notify.replace(summary, body, 'notification-device-eject')




