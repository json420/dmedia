#!/usr/bin/python3

import logging
from gettext import gettext as _

from gi.repository import GObject, Gtk, AppIndicator3, Notify
from microfiber import random_id

from dmedia.service.peers import Peer


Notify.init('dmedia-peer')
logging.basicConfig(level=logging.DEBUG)


class Browser(Peer):
    def add_peer(self, key, ip, port):
        self.indicator = AppIndicator3.Indicator.new(
            'dmedia-peer',
            'indicator-novacut',
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
        note = Notify.Notification.new('Novacut Peering Offer', key, None)
        note.show()

    def remove_peer(self, key):
        del self.indicator

    def on_accept(self, button):
        self.publish('_dmedia-accept._tcp', 9000)


peer = Browser(random_id())
peer.browse('_dmedia-offer._tcp')
mainloop = GObject.MainLoop()
mainloop.run()
