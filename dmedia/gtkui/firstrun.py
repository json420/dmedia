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

import os
from os import path
from xml.sax import saxutils
from gettext import gettext as _

from gi.repository import Gtk, Pango, GConf


NO_DIALOG = '/apps/dmedia/dont-show-import-firstrun'
conf = GConf.Client.get_default()


TITLE = _('DMedia Importer')

ICON_SIZE = Gtk.IconSize.LARGE_TOOLBAR
PADDING = 5


class EasyBox(object):
    """
    A more convenient box borrowed from TymeLapse.
    """
    def _pack(self, widget, expand=False, padding=PADDING, end=False):
        method = (self.pack_start if not end else self.pack_end)
        method(widget, expand, expand, padding)
        return widget


class VBox(Gtk.VBox, EasyBox):
    pass


class HBox(Gtk.HBox, EasyBox):
    pass


class Label(Gtk.Label):
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


class Button(Gtk.Button):
    def __init__(self, stock=None, text=None):
        super(Button, self).__init__()
        hbox = HBox()
        self.add(hbox)
        self._image = Gtk.Image()
        self._label = Label(None)
        hbox._pack(self._image)
        hbox._pack(self._label, expand=True)
        if stock is not None:
            self._set_stock(stock)
        if text is not None:
            self._set_text(text)

    def _set_stock(self, stock, size=ICON_SIZE):
        self._image.set_from_stock(stock, size)

    def _set_text(self, text):
        self._label.set_text(text)


class FolderChooser(Button):
    def __init__(self):
        super(FolderChooser, self).__init__(stock=Gtk.STOCK_OPEN)
        self._label.set_ellipsize(Pango.EllipsizeMode.START)
        self._title = _('Choose folder to import...')
        self.connect('clicked', self._on_clicked)
        self._set_value(os.environ['HOME'])

    def _set_value(self, value):
        self._value = path.abspath(value)
        self._label._set_text(self._value)

    def _on_clicked(self, button):
        dialog = Gtk.FileChooserDialog(
            title=self._title,
            action=Gtk.FileChooserAction.SELECT_FOLDER,
            buttons=(
                Gtk.STOCK_CANCEL, Gtk.ResponseType.CANCEL,
                Gtk.STOCK_OPEN, Gtk.ResponseType.OK,
            ),
        )
        dialog.set_current_folder(self._value)
        response = dialog.run()
        if response == Gtk.ResponseType.OK:
            self._set_value(dialog.get_filename())
        dialog.destroy()


def okay_button():
    return Button(Gtk.STOCK_OK, _('OK, Import Files'))


class ImportDialog(Gtk.Window):
    def __init__(self):
        super(ImportDialog, self).__init__()
        self.set_default_size(425, 200)
        self.set_title(TITLE)
        try:
            self.set_icon_from_file('/usr/share/pixmaps/dmedia.svg')
        except:
            pass
        self.connect('destroy', Gtk.main_quit)

        self._value = None

        hbox = HBox()
        self.add(hbox)

        vbox = VBox()
        hbox._pack(vbox, expand=True)

        vbox._pack(Label(_('Choose Folder:'), 'b'))
        self._folder = FolderChooser()
        vbox._pack(self._folder)

        self._button = okay_button()
        vbox._pack(self._button, end=True)
        self._button.connect('clicked', self._on_clicked)

        self.show_all()

    def run(self):
        Gtk.main()
        return self._value

    def _on_clicked(self, button):
        self._value = self._folder._value
        Gtk.main_quit()


class FirstRunGUI(Gtk.Window):
    """
    Promt use first time dmedia importer is run.

    For example:

    >>> from dmedia.gtkui.firstrun import FirstRunGUI
    >>> run = FirstRunGUI.run_if_first_run('/media/EOS_DIGITAL')  #doctest: +SKIP
    """

    def __init__(self):
        super(FirstRunGUI, self).__init__()
        self.set_default_size(425, 200)

        self.set_title(TITLE)
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
        self.folder.set_ellipsize(Pango.EllipsizeMode.START)
        vbox._pack(self.folder)

        self.dont_show_again = Gtk.CheckButton(label=_("Don't show this again"))
        self.dont_show_again.set_active(True)

        self.ok = okay_button()

        hbox = HBox()
        hbox._pack(self.ok, expand=True)
        hbox._pack(self.dont_show_again)

        vbox._pack(hbox, end=True)


    def connect_signals(self):
        self.connect("delete_event", self.delete_event)
        self.connect("destroy", self.destroy)
        self.ok.connect("clicked", self.on_ok)


    def destroy(self, widget, data=None):
        Gtk.main_quit()


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
        Gtk.main()
        return self.ok_was_pressed

    @classmethod
    def run_if_first_run(cls, base, unset=False):
        if unset:
            conf.unset(NO_DIALOG)
        if conf.get_bool(NO_DIALOG):
            return True
        app = cls()
        return app.go(base)
