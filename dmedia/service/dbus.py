# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# `dmedia` is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along
# with `dmedia`.  If not, see <http://www.gnu.org/licenses/>.

"""
Simple dbus helper to make PyGI a bit nicer.
"""

from gi.repository import GObject, Gio
from gi.repository.GObject import TYPE_PYOBJECT


PROPS = 'org.freedesktop.DBus.Properties'


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


session = DBus(Gio.bus_get_sync(Gio.BusType.SESSION, None))
system = DBus(Gio.bus_get_sync(Gio.BusType.SYSTEM, None))


class UDisks(GObject.GObject):

    __gsignals__ = {
        'DeviceAdded': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
        'DeviceRemoved': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
        'DeviceChanged': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
    }
    _autoemit = ('DeviceAdded', 'DeviceRemoved', 'DeviceChanged')

    def __init__(self):
        super().__init__()
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            '/org/freedesktop/UDisks'
        )
        self.proxy.connect('g-signal', self.on_g_signal)

    def on_g_signal(self, proxy, sender, signal, params):
        if signal in self._autoemit:
            args = params.unpack()
            self.emit(signal, *args)

    def EnumerateDevices(self):
        return self.proxy.EnumerateDevices()

    def FindDeviceByMajorMinor(self, major, minor):
        return self.proxy.FindDeviceByMajorMinor('(xx)', major, minor)


class Device:
    bus = 'org.freedesktop.UDisks'
    iface = 'org.freedesktop.UDisks.Device'

    def __init__(self, path):
        self.path = path
        self.proxy = system.get(self.bus, path, self.iface)
        self.propsproxy = system.get(self.bus, path, PROPS)
        self.proxy.connect('g-signal', self.on_g_signal)

    def GetProps(self):
        return self.propsproxy.GetAll('(s)', self.iface)

    def FilesystemMount(self, fstype, options):
        return self.proxy.FilesystemMount('(sas)', fstype, options)

    def FilesystemUnmount(self, options):
        return self.proxy.FilesystemUnmount('(as)', options)

    def on_g_signal(self, proxy, sender, signal, params):
        print(signal, pramas.unpack())
        
