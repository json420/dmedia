#!/usr/bin/python3

import logging

from gi.repository import GObject
from microfiber import random_id

from dmedia.service.peers import Peer


logging.basicConfig(level=logging.DEBUG)
machine_id = random_id()


class Publish(Peer):
    def add_peer(self, key, ip, port):
        print('add_peer({!r}, {!r}, {!r})'.format(key, ip, port))

    def remove_peer(self, key):
        print('remove_peer({!r})'.format(key))


peer = Publish()
peer.browse('_dmedia-accept._tcp')
peer.publish('_dmedia-offer._tcp', machine_id, 8000)
mainloop = GObject.MainLoop()
mainloop.run()
