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
group = system.get(
    'org.freedesktop.Avahi',
    avahi.EntryGroupNew(),
    'org.freedesktop.Avahi.EntryGroup'
)


def on_g_signal(*args):
    print(args)

group.connect('g-signal', on_g_signal)


group.AddService('(iiussssqaay)',
    -1,  # Interface
    -1,  # Protocol
    0,  # Flags
    'dmedia file transfer',
    '_dmedia._tcp',
    '',  # Domain, default to .local
    '',  # Host, default to localhost
    8000,  # Port
    None,  # TXT record
)
group.Commit()
#group.Reset()  # Remove
    
mainloop = GObject.MainLoop()
mainloop.run()

