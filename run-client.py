#!/usr/bin/python3

import logging

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GObject


log = logging.getLogger()
GObject.threads_init()
DBusGMainLoop(set_as_default=True)


class Browser:
    """
    Discover Dmedia peering offers.
    """

    service = '_dmedia-peer._tcp'

    def run(self):
        system = dbus.SystemBus()
        self.avahi = system.get_object('org.freedesktop.Avahi', '/')
        browser_path = self.avahi.ServiceBrowserNew(
            -1,  # Interface
            0,  # Protocol -1 = both, 0 = ipv4, 1 = ipv6
            self.service,
            'local',
            0,  # Flags
            dbus_interface='org.freedesktop.Avahi.Server'
        )
        self.browser = system.get_object('org.freedesktop.Avahi', browser_path)
        self.browser.connect_to_signal('ItemNew', self.on_ItemNew)
        self.browser.connect_to_signal('ItemRemove', self.on_ItemRemove)

    def on_ItemNew(self, interface, protocol, key, _type, domain, flags):
        self.avahi.ResolveService(
            interface, protocol, key, _type, domain, -1, 0,
            dbus_interface='org.freedesktop.Avahi.Server',
            reply_handler=self.on_reply,
            error_handler=self.on_error,
        )

    def on_reply(self, *args):
        key = args[2]
        (ip, port) = args[7:9]
        url = 'http://{}:{}/'.format(ip, port)
        log.info('Avahi(%s): new peer %s at %s', self.service, key, url)
        self.add_peer(key, url)

    def on_error(self, exception):
        log.error('%s: error calling ResolveService(): %r', self.service, exception)

    def on_ItemRemove(self, interface, protocol, key, _type, domain, flags):
        log.info('Avahi(%s): peer removed: %s', self.service, key)
        self.remove_peer(key)

    def add_peer(self, key, url):
        pass

    def remove_peer(self, key):
        pass



logging.basicConfig(level=logging.DEBUG)
avahi = Browser()
avahi.run()
mainloop = GObject.MainLoop()
mainloop.run()
