#!/usr/bin/python3

import logging
from gettext import gettext as _
import tempfile

from gi.repository import GObject, Gtk, AppIndicator3, Notify
from microfiber import random_id

from dmedia.startup import DmediaCouch
from dmedia.service.peers import Peer
from dmedia.gtk.peering import BaseUI


Notify.init('dmedia-peer')
logging.basicConfig(level=logging.DEBUG)


class UI(BaseUI):
    page = 'server.html'

    signals = {
        'create_secret': [],
        'new_secret': ['secret'],
    }

    def __init__(self):
        super().__init__()
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=True)
        self.couch.load_pki()
        self.avahi = Peer(
            self.couch.pki.user.id,
            self.on_add_peer,
            self.on_remove_peer
        )
        self.avahi.browse('_dmedia-offer._tcp')

    def connect_hub_signals(self, hub):
        hub.connect('create_secret', self.on_create_secret)

    def on_create_secret(self, hub):
        self.secret = random_id(5)
        self.hub.send('new_secret', self.secret)

    def on_add_peer(self, key, url):
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
        note = Notify.Notification.new('Novacut Peering Offer', None, None)
        note.show()

    def on_remove_peer(self, key):
        del self.indicator

    def on_accept(self, button):
        del self.indicator
        self.avahi.publish('_dmedia-accept._tcp', 9000)
        self.window.show_all()


ui = UI()
#ui.run()
Gtk.main()
