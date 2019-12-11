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
from gettext import ngettext

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('Notify', '0.7')
gi.require_version('AppIndicator3', '0.1')
from gi.repository import Gtk, Notify, AppIndicator3

from dmedia.units import bytes10
from dmedia.misc import WeakMethod


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


def card_label(basedir, info):
    if info and info.get('partition'):
        p = info['partition']
        return '{}, {}'.format(bytes10(p['bytes']), p['label'])
    return basedir


# FIXME: We need to consolidate all this UI string logic in a single module
def notify_started(basedirs):
    assert len(basedirs) >= 1
    summary = ngettext(
        'Importing files from {count} card:',
        'Importing files from {count} cards:',
        len(basedirs)
    ).format(count=len(basedirs))
    body = '\n'.join(basedirs)
    return (summary, body)


def notify_stats(lines):
    summary = lines[0]
    lines = lines[1:]
    body = ('\n'.join(lines) if lines else None)
    return (summary, body)


class UnityImportUX:
    def __init__(self, hub):
        self.hub = hub
        self.notify = NotifyManager()
        self.indicator = AppIndicator3.Indicator.new('dmedia', ICON,
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS
        )
        self.indicator.set_attention_icon(ICON_ATT)
        self.menu = Gtk.Menu()
        self.stop = Gtk.MenuItem()
        self.stop.set_label(_('Stop Importer'))
        self.stop.connect('activate', WeakMethod(self, 'on_stop'))
        self.menu.append(self.stop)
        self.menu.show_all()
        self.indicator.set_menu(self.menu)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        hub.connect('batch_started', WeakMethod(self, 'on_batch_started'))
        hub.connect('import_started', WeakMethod(self, 'on_import_started'))
        hub.connect('batch_finalized', WeakMethod(self, 'on_batch_finalized'))
        hub.connect('error', WeakMethod(self, 'on_error'))

    def on_stop(self, menuitem):
        self.hub.send('stop_importer')

    def on_batch_started(self, gm, batch_id):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)
        self.basedirs = []

    def on_import_started(self, gm, basedir, import_id, info):
        self.basedirs.append(card_label(basedir, info))
        (summary, body) = notify_started(self.basedirs)
        icons = {
            'usb': 'notification-device-usb',
        }
        icon = icons['usb']  # FIXME: Need equivalent info from VolumeMonitor
        self.notify.replace(summary, body, icon)

    def on_batch_finalized(self, gm, batch_id, stats, copies, lines):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        (summary, body) = notify_stats(lines)
        self.notify.replace(summary, body, 'notification-device-eject')

    def on_error(self, gm, error):
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)

