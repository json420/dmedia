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
import pango
import gconf
from gettext import gettext as _
from xml.sax import saxutils


NO_DIALOG = '/apps/dmedia/dont-show-import-firstrun'
conf = gconf.client_get_default()

PADDING = 5


class EasyBox(object):
    """
    A more convenient box borrowed from TymeLapse.
    """
    def _pack(self, widget, expand=False, padding=PADDING, end=False):
        method = (self.pack_start if not end else self.pack_end)
        method(widget, expand, expand, padding)
        return widget


class VBox(gtk.VBox, EasyBox):
    pass


class HBox(gtk.HBox, EasyBox):
    pass


class Label(gtk.Label):
    """
    A more convenient label borrowed from TymeLapse.
    """
    def __init__(self, text, *tags):
        super(Label, self).__init__()
        self.set_alignment(0, 0.5)
        self.set_padding(5, 5)
        self._text = text
        self._tags = set(tags)
        self._update()
        #self.set_selectable(True)

    def _add_tags(self, *tags):
        self._tags.update(tags)
        self._update()

    def _remove_tags(self, *tags):
        for tag in tags:
            if tag in self._tags:
                self._tags.remove(tag)
        self._update()

    def _set_text(self, text):
        self._text = text
        self._update()

    def _update(self):
        text = ('' if self._text is None else self._text)
        if text and self._tags:
            m = saxutils.escape(text)
            for tag in self._tags:
                m = '<%(tag)s>%(m)s</%(tag)s>' % dict(tag=tag, m=m)
            self.set_markup(m)
        else:
            self.set_text(text)


class FirstRunGUI(gtk.Window):
    """
    Promt use first time dmedia importer is run.

    For example:

    >>> from dmedialib.firstrun import FirstRunGUI
    >>> run = FirstRunGUI.run_if_first_run('/media/EOS_DIGITAL')  #doctest: +SKIP
    """

    def __init__(self):
        super(FirstRunGUI, self).__init__()
        self.set_default_size(400, 200)

        self.set_title(_('dmedia Media Importer'))
        try:
            self.set_icon_from_file('/usr/share/pixmaps/dmedia.svg')
        except:
            pass

        self.ok_was_pressed = False

        self.add_content()

        self.connect_signals()

    def add_content(self):
        vbox = VBox()
        self.add(vbox)

        label1 = Label(_('Welcome to the dmedia importer!'), 'big')
        label1.set_alignment(0.5, 0.5)
        vbox._pack(label1)

        label2 = Label(_('It will import all files in the following folder:'))
        vbox._pack(label2)

        self.folder = Label(None, 'b')
        self.folder.set_ellipsize(pango.ELLIPSIZE_START)
        vbox._pack(self.folder)

        self.dont_show_again = gtk.CheckButton(label=_("Don't show this again"))
        self.dont_show_again.set_active(True)

        self.ok = gtk.Button()
        self.ok_label = gtk.Label(_('OK, start the import'))
        self.ok.add(self.ok_label)

        hbox = HBox()
        hbox._pack(self.ok, expand=True)
        hbox._pack(self.dont_show_again)

        vbox._pack(hbox, end=True)


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

    def go(self, folder):
        self.folder._set_text(folder)
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
