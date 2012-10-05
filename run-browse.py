#!/usr/bin/python3

import logging
from gettext import gettext as _

from gi.repository import GObject, Gtk, AppIndicator3
from microfiber import random_id

from dmedia.service.peers import Peer


logging.basicConfig(level=logging.DEBUG)
machine_id = random_id()


class Browser(Peer):
    def add_peer(self, key, ip, port):
        self.indicator = AppIndicator3.Indicator.new(
            'dmedia-peer',
            'indicator-dmedia-att',
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS
        )
        menu = Gtk.Menu()
        accept = Gtk.MenuItem()
        accept.set_label(_('Accept'))
        accept.connect('activate', self.on_accept)
        menu.append(accept)
        menu.show_all()
        self.indicator.set_menu(menu)
        self.indicator.set_status(AppIndicator3.IndicatorStatus.ATTENTION)

    def remove_peer(self, key):
        del self.indicator

    def on_accept(self, button):
        self.publish('_dmedia-accept._tcp', machine_id, 9000)


peer = Browser()
peer.browse('_dmedia-offer._tcp')
mainloop = GObject.MainLoop()
mainloop.run()
