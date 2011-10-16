# dmedia: distributed media library
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
#
# Authors:
#   Jason Gerard DeRose <jderose@novacut.com>

"""
Simple dbus helper to make PyGI a bit nicer.
"""

from os import path

from filestore import DOTNAME
from gi.repository import GObject, Gio
from gi.repository.GObject import TYPE_PYOBJECT


PROPS = 'org.freedesktop.DBus.Properties'


class DBus:
    def __init__(self, conn):
        self.conn = conn

    def get(self, bus, obj, iface=None):
        if iface is None:
            iface = bus
        return Gio.DBusProxy.new_sync(
            self.conn, 0, None, bus, obj, iface, None
        )

    def get_async(self, callback, bus, obj, iface=None):
        if iface is None:
            iface = bus
        Gio.DBusProxy.new(
            self.conn, 0, None, bus, obj, iface, None, callback, None
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
        'card-inserted': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store-added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
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

    def on_DeviceChanged(self, udisks, obj):
        try:
            partition = get_device_props(obj)
        except Exception:
            return
        if partition['DeviceIsPartition'] and partition['DeviceIsMounted']:
            if len(partition['DeviceMountPaths']) != 1:
                return
            parentdir = partition['DeviceMountPaths'][0]
            drive = get_device_props(partition['PartitionSlave'])
            if path.isdir(path.join(parentdir, DOTNAME)):
                print('store-added', parentdir)
                self.emit('store-added', parentdir, obj, partition, drive)
            elif not partition['DeviceIsSystemInternal']:
                print('card-inserted', parentdir)
                self.emit('card-inserted', parentdir, obj, partition, drive)

    def monitor(self):
        self.connect('DeviceChanged', self.on_DeviceChanged)

    def EnumerateDevices(self):
        return self.proxy.EnumerateDevices()

    def FindDeviceByMajorMinor(self, major, minor):
        return self.proxy.FindDeviceByMajorMinor('(xx)', major, minor)


def get_device_props(obj):
    device = system.get('org.freedesktop.UDisks', obj, PROPS)
    return device.GetAll('(s)', 'org.freedesktop.UDisks.Device')


class Device:
    bus = 'org.freedesktop.UDisks'
    iface = 'org.freedesktop.UDisks.Device'

    def __init__(self, obj):
        self.obj = obj
        self.proxy = system.get(self.bus, obj, self.iface)
        self.propsproxy = system.get(self.bus, obj, PROPS)
        self.props = self.propsproxy.GetAll('(s)', self.iface)

    def __getitem__(self, key):
        return self.props[key]

    def FilesystemUnmount(self, options):
        return self.proxy.FilesystemUnmount('(as)', options)
        
    def DriveEject(self):
        return self.proxy.DriveEject('(as)', [])
        
    def DriveDetach(self):
        return self.proxy.DriveDetach('(as)', [])

    def FilesystemCreate(self, fstype, options):
        return self.proxy.FilesystemCreate('(sas)', fstype, options)

