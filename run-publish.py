#!/usr/bin/python3

import logging

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject
from microfiber import random_id

from dmedia.service.peers import Peer
from dmedia.peering import TempPKI


log = logging.getLogger()
GObject.threads_init()
DBusGMainLoop(set_as_default=True)
logging.basicConfig(level=logging.DEBUG)

pki = TempPKI()
cert_id = pki.create(random_id())

peer = Peer(cert_id)
peer.run()
peer.publish(5000)
mainloop = GObject.MainLoop()
mainloop.run()
