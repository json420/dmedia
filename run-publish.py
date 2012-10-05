#!/usr/bin/python3

import logging
import tempfile

from gi.repository import GObject
from microfiber import random_id

from dmedia.startup import DmediaCouch
from dmedia.service.peers import Peer
from dmedia.gtk.peering import BaseUI


logging.basicConfig(level=logging.DEBUG)



class UI(BaseUI):
    page = 'welcome.html'

    signals = {
        'first': [],
        'sync': [],
    }

    def __init__(self):
        super().__init__()
        self.couch = DmediaCouch(tempfile.mkdtemp())
        self.couch.firstrun_init(create_user=False)
        self.couch.load_pki()
        self.avahi = Peer(
            self.couch.pki.machine.id,
            self.on_peer_added,
            self.on_peer_removed,
        )

    def connect_hub_signals(self, hub):
        hub.connect('first', self.on_first)
        hub.connect('sync', self.on_sync)

    def on_peer_added(self, key, url):
        print('on_add')

    def on_peer_removed(self, key):
        print('on_remove')

    def on_first(self, hub):
        print('first')

    def on_sync(self, hub):
        self.avahi.browse('_dmedia-accept._tcp')
        self.avahi.publish('_dmedia-offer._tcp', 8000)
 

ui = UI()
ui.run()


