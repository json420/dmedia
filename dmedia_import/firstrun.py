#!/usr/bin/env python

# Authors:
#   David Green <david4dev@gmail.com>
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

import gtk
import gconf
from gettext import gettext as _


NO_DIALOG = '/apps/dmedia/dont-show-import-firstrun'
conf = gconf.client_get_default()


class FirstRunGUI(gtk.Window):
    def __init__(self):
        super(FirstRunGUI, self).__init__()

        self.set_title(_('dmedia Media Importer'))
        try:
            self.set_icon_from_file('/usr/share/pixmaps/dmedia.svg')
        except:
            pass

        self.ok_was_pressed = False

        self.add_content()

        self.connect_signals()

    def add_content(self):
        self.container = gtk.VBox()

        self.label = gtk.Label(_("This is the dmedia media importer.\nIt will import media files from the following folders into your dmedia library:\n\n%s\nTo start the import press 'OK'.\n\n"))
        self.label.show()

        self.spacing_label = gtk.Label("        ")

        self.dont_show_again = gtk.CheckButton(label=_("Don't show this again"))
        self.dont_show_again.set_active(True)

        self.ok = gtk.Button()
        self.ok_label = gtk.Label(_("OK"))
        self.ok_label.show()
        self.ok.add(self.ok_label)

        self.hbox = gtk.HBox()
        self.ok.show()
        self.hbox.add(self.ok)
        self.spacing_label.show()
        self.hbox.add(self.spacing_label)
        self.dont_show_again.show()
        self.hbox.add(self.dont_show_again)

        self.container.add(self.label)
        self.label.show()
        self.container.add(self.hbox)
        self.hbox.show()

        self.container.show()
        self.add(self.container)


    def connect_signals(self):
        self.connect("delete_event", self.delete_event)
        self.connect("destroy", self.destroy)
        self.ok.connect("clicked", self.on_ok)


    def destroy(self, widget, data=None):
        gtk.main_quit()


    def delete_event(self, widget, event, data=None):
        return False


    def on_ok(self, widget):
        dont_show_again = self.dont_show_again.get_active()
        val = 0
        if dont_show_again:
            val = 1
        conf.set_bool(NO_DIALOG, val)
        self.ok_was_pressed = True
        self.destroy(widget)

    def go(self, *args):
        self.label.set_text(self.label.get_text() % ("\t" + "\n\t".join(args) + "\n"))
        self.show_all()
        gtk.main()
        return self.ok_was_pressed

    @classmethod
    def run_if_first_run(cls, base, unset=False):
        if unset:
            conf.unset(NO_DIALOG)
        if conf.get_bool(NO_DIALOG):
            return True
        app = cls()
        return app.go(base)
