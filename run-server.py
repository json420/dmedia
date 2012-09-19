#!/usr/bin/python3

import logging

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject


log = logging.getLogger()
GObject.threads_init()
DBusGMainLoop(set_as_default=True)


class Publish:
    """
    Broadcast a peering offer.
    """

    service = '_dmedia-peer._tcp'

    def __init__(self, _id, port):
        self.group = None
        self.id = _id
        self.port = port

    def __del__(self):
        self.free()

    def run(self):
        system = dbus.SystemBus()
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        self.group = system.get_object(
            'org.freedesktop.Avahi',
            self.avahi.EntryGroupNew(
                dbus_interface='org.freedesktop.Avahi.Server'
            )
        )
        log.info(
            'Avahi(%s): advertising %s on port %s', self.service, self.id, self.port
        )
        self.group.AddService(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            0,  # Flags
            self.id,
            self.service,
            '',  # Domain, default to .local
            '',  # Host, default to localhost
            self.port,  # Port
            b'',  # TXT record
            dbus_interface='org.freedesktop.Avahi.EntryGroup'
        )
        self.group.Commit(dbus_interface='org.freedesktop.Avahi.EntryGroup')

    def free(self):
        if self.group is not None:
            log.info(
                'Avahi(%s): freeing %s on port %s', self.service, self.id, self.port
            )
            self.group.Reset(dbus_interface='org.freedesktop.Avahi.EntryGroup')
            self.group = None
            del self.avahi


logging.basicConfig(level=logging.DEBUG)
avahi = Publish('user', 5000)
avahi.run()
mainloop = GObject.MainLoop()
mainloop.run()
