#!/usr/bin/python3

import logging
import tempfile
import multiprocessing

from gi.repository import GObject
from microfiber import random_id

from dmedia.startup import DmediaCouch
from dmedia.service.peers import Peer
from dmedia.gtk.peering import BaseUI
from dmedia.httpd import run_server, echo_app


logging.basicConfig(level=logging.DEBUG)


def start_server_process(ssl_config):
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=run_server,
        args=(queue, echo_app, '0.0.0.0', ssl_config),
    )
    process.daemon = True
    process.start()
    env = queue.get()
    if isinstance(env, Exception):
        raise env
    return (process, env['port'])



class UI(BaseUI):
    page = 'client.html'

    signals = {
        'first': [],
        'sync': [],
        'enter_secret': [],
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
        self.hub.send('enter_secret')

    def on_peer_removed(self, key):
        print('on_remove')

    def on_first(self, hub):
        print('first')

    def on_sync(self, hub):
        ssl_config = {
            'key_file': self.couch.pki.machine.key_file,
            'cert_file': self.couch.pki.machine.cert_file,
        }
        print(ssl_config)
        (self.httpd, self.port) = start_server_process(ssl_config)
        self.avahi.browse('_dmedia-accept._tcp')
        self.avahi.publish('_dmedia-offer._tcp', self.port)
 

ui = UI()
ui.run()


