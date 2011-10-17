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

from gettext import gettext as _

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
    def __init__(self, gmanager):
        self.gmanager = gmanager
        self.launcher = Unity.LauncherEntry.get_for_desktop_id('dmedia.desktop')
        self.notify = NotifyManager()
        self.indicator = AppIndicator3.Indicator.new('dmedia', ICON,
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS
        )
        self.indicator.set_attention_icon(ICON_ATT)
        self.menu = Gtk.Menu()
        self.close = Gtk.MenuItem()
        self.close.set_label(_('Close'))
        #self.close.connect('activate', self.on_close)
        self.menu.append(self.close)
        self.menu.show_all()
        self.indicator.set_menu(self.menu)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        self.gmanager.connect('batch_started', self.on_batch_started)
        self.gmanager.connect('import_started', self.on_import_started)
        self.gmanager.connect('batch_progress', self.on_batch_progress)
        self.gmanager.connect('batch_finished', self.on_batch_finished)
        self.gmanager.connect('error', self.on_error)

    def on_batch_started(self, gm, batch_id):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)
        self.launcher.set_property('count', 0)
        self.launcher.set_property('count_visible', True)
        self.launcher.set_property('progress', 0.0)
        self.launcher.set_property('progress_visible', True)
        self.basedirs = []

    def on_import_started(self, gm, basedir, import_id, info):
        self.basedirs.append(basedir)
        (summary, body) = notify_started(self.basedirs)
        icons = {
            'usb': 'notification-device-usb',
        }
        icon = icons.get(info['drive']['connection'])
        self.notify.replace(summary, body, icon)

    def on_batch_progress(self, gm, count, total_count, size, total_size):
        self.launcher.set_property('count', total_count)
        progress = (0.0 if total_size == 0 else size / total_size)
        self.launcher.set_property('progress', progress)

    def on_batch_finished(self, gm, batch_id, stats):
        self.launcher.set_property('count_visible', False)
        self.launcher.set_property('progress_visible', False)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        (summary, body) = notify_stats(stats)
        self.notify.replace(summary, body, 'notification-device-eject')

    def on_error(self, gm, exception, message):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        self.launcher.set_property('count_visible', False)
        self.launcher.set_property('progress_visible', False)
        self.launcher.set_property('urgent', True)
        
