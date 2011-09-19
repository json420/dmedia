#!/usr/bin/python3

import os
import json

from gi.repository import Gio


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


BUS = 'org.freedesktop.UDisks'
PROPS = 'org.freedesktop.DBus.Properties'


UDisks = system.get(BUS, '/org/freedesktop/UDisks')

d = {}
print('\nEnumerateDevices:')
for path in UDisks.EnumerateDevices():
    print(path)
    device = system.get(BUS, path, PROPS)
    props = device.GetAll('(s)', 'org.freedesktop.UDisks.Device')
    del props['DriveAtaSmartBlob']
    d[path] = props

dst = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'udisks-report.json'
)
dst_fp = open(dst, 'w')
json.dump(d, dst_fp, sort_keys=True, indent=4)
print('\nUDisks info dumped to:')
print(dst)
 



#class Device(object):
#    def __init__(self, device, conn=None):
#        self._conn = (dbus.SystemBus() if conn is None else conn)
#        self._proxy = self._conn.get_object('org.freedesktop.UDisks', device)
#        self.device = device
#        self._get = self._proxy.get_dbus_method('Get',
#            dbus_interface='org.freedesktop.DBus.Properties'
#        )

#    def __getitem__(self, key):
#        return self._get('org.freedesktop.UDisks.Device', key)



#fs = Device('/org/freedesktop/UDisks/devices/sdf1')
#drive = Device(fs['PartitionSlave'])

#doc = {
#    'device': {
#        'type': 'drive',
#        'drives': [
#            {
#                'serial': drive['DriveSerial'],
#                'bytes': drive['DeviceSize'],
#                'block_bytes': drive['DeviceBlockSize'],
#                'vendor': drive['DriveVendor'],
#                'model': drive['DriveModel'],
#                'revision': drive['DriveRevision'],
#                'rotational': drive['DriveIsRotational'],
#                'partition_scheme': drive['PartitionTableScheme'],
#            },
#        ],
#        'uuid': fs['IdUuid'],
#        'bytes': fs['DeviceSize'],
#        'filesystem': fs['IdType'],
#        'label': fs['IdLabel'],
#        'partition': fs['PartitionNumber'],
#    },
#}

#print json.dumps(doc, sort_keys=True, indent=4)
