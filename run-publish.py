#!/usr/bin/python3

import logging

from gi.repository import GObject
from microfiber import random_id

from dmedia.service.peers import Peer
from dmedia.gtk.peering import BaseUI


logging.basicConfig(level=logging.DEBUG)


class Publish(Peer):
    def add_peer(self, key, ip, port):
        print('add_peer({!r}, {!r}, {!r})'.format(key, ip, port))

    def remove_peer(self, key):
        print('remove_peer({!r})'.format(key))


peer = Publish(random_id())
peer.browse('_dmedia-accept._tcp')
peer.publish('_dmedia-offer._tcp', 8000)

ui = BaseUI()

