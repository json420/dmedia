#!/usr/bin/env python

# Authors:
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
import dmedialib.client
from dmedia_import.pidlock import PidLock
from dmedia_import.common import device_type, get_icon
import pynotify
from gettext import gettext as _
import gtk
from time import sleep


class Notifier(object):
    def __init__(self):
        self.client = dmedialib.client.Client()
        self.client.version()
        self.imports = {
            #base : [completed, total]
        }
        for base in self.client.list_imports():
            self.imports[base] = ['0', '0']
        self.client.connect('import_started', self.on_import_started)
        self.client.connect('import_finished', self.on_import_finished)
        self.client.connect('import_progress', self.on_import_progress)

    def on_import_started(self, *rest):
        base = rest[1]
        notification = pynotify.Notification(_("Searching for new files"), base, get_icon(device_type(base)))
        notification.set_hint_string('append', '')
        notification.show()
        self.imports[base] = ['0', '0']

    def on_import_finished(self, *rest):
        base = rest[1]
        if len(self.imports) == 1: #only notify if all other imports are finished
            notification = pynotify.Notification(_("Added %s new files, %s GB"), _("Skipped %s duplicates, %s GB"), get_icon("notification-device-eject")) #not yet possible to get data for number added, number skipped and sizes
            notification.show()
        del(self.imports[base])

    def on_import_progress(self, *rest):
        base = rest[1]
        completed = rest[2]
        total = rest[3]
        self.imports[base] = [str(completed), str(total)]

    def main(self):
        PidLock('dmedia-notifier').main(gtk.main)

if __name__ == '__main__':
    Notifier().main()
