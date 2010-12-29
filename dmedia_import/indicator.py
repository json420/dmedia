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
import dmedia.client
from dmedia_import.pidlock import PidLock
from dmedia_import.common import device_type, get_icon
import pynotify
from gettext import gettext as _
import gtk
import appindicator


class Indicator(object):
    def __init__(self):
        self.indicator = appindicator.Indicator(
            "indicator-dmedia",
            "/usr/share/pixmaps/dmedia.svg",
            appindicator.CATEGORY_APPLICATION_STATUS
        )
        self.indicator.set_status(appindicator.STATUS_ACTIVE)
        self.client = dmedia.client.Client()
        self.client.version()
        self.imports = {
            #base : [completed, total]
        }
        for base in self.client.list_imports():
            self.imports[base] = ['0', '0']
        self.client.connect('import_started', self.on_import_started)
        self.client.connect('import_finished', self.on_import_finished)
        self.client.connect('import_progress', self.on_import_progress)

    def menu_items(self):
        if len(self.imports) > 0:
            yield(_("Current Imports:"))
            for base in self.imports:
                yield("    " + base + "  (" + '/'.join(self.imports[base]) + ")")
        yield(_("Browse Files"))
        yield(_("Preferences"))


    def update_menu_items(self):
        self.menu = gtk.Menu()
        for text in self.menu_items():
            item = gtk.MenuItem(text)
            item.show()
            self.menu.append(item)
        self.indicator.set_menu(self.menu)

    def on_import_started(self, *rest):
        base = rest[1]
        self.imports[base] = ['0', '0']
        self.update_menu_items()

    def on_import_finished(self, *rest):
        base = rest[1]
        del(self.imports[base])
        self.update_menu_items()

    def on_import_progress(self, *rest):
        base = rest[1]
        completed = rest[2]
        total = rest[3]
        self.imports[base] = [str(completed), str(total)]
        self.update_menu_items()

    def main(self):
        PidLock('indicator-dmedia').main(gtk.main)

if __name__ == '__main__':
    Indicator().main()