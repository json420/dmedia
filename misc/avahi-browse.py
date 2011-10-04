#!/usr/bin/python3

from gi.repository import Gio, GObject

GObject.threads_init()


class DBus:
    def __init__(self, conn):
        self.conn = conn

    def get(self, bus, path, iface=None):
        if iface is None:
            iface = bus
        return Gio.DBusProxy.new_sync(
            self.conn, 0, None, bus, path, iface, None
        )

    def get_async(self, callback, bus, path, iface=None):
        if iface is None:
            iface = bus
        Gio.DBusProxy.new(
            self.conn, 0, None, bus, path, iface, None, callback, None
        )


system = DBus(Gio.bus_get_sync(Gio.BusType.SYSTEM, None))


avahi = system.get(
    'org.freedesktop.Avahi',
    '/',
    'org.freedesktop.Avahi.Server'
)

browser = system.get(
    'org.freedesktop.Avahi',
    avahi.ServiceBrowserNew('(iissu)', -1, -1, '_dmedia._tcp', 'local', 0),
    'org.freedesktop.Avahi.ServiceBrowser',
)


def on_g_signal(proxy, sender, signal, params):
    params = params.unpack()
    print(signal, params)
    if signal == 'ItemNew':
        (interface, protocol, name, _type, domain, flags) = params
        (ip, port) = avahi.ResolveService('(iisssiu)',
            interface, protocol, name, _type, domain, -1, 0
        )[7:9]
        print(ip, port)


browser.connect('g-signal', on_g_signal)
    
mainloop = GObject.MainLoop()
mainloop.run()

