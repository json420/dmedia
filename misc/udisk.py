import json

import dbus


class Device(object):
    def __init__(self, device, conn=None):
        self._conn = (dbus.SystemBus() if conn is None else conn)
        self._proxy = self._conn.get_object('org.freedesktop.UDisks', device)
        self.device = device
        self._get = self._proxy.get_dbus_method('Get',
            dbus_interface='org.freedesktop.DBus.Properties'
        )

    def __getitem__(self, key):
        return self._get('org.freedesktop.UDisks.Device', key)



fs = Device('/org/freedesktop/UDisks/devices/sdf1')
drive = Device(fs['PartitionSlave'])

doc = {
    'device': {
        'type': 'drive',
        'drives': [
            {
                'serial': drive['DriveSerial'],
                'bytes': drive['DeviceSize'],
                'block_bytes': drive['DeviceBlockSize'],
                'vendor': drive['DriveVendor'],
                'model': drive['DriveModel'],
                'revision': drive['DriveRevision'],
                'rotational': drive['DriveIsRotational'],
                'partition_scheme': drive['PartitionTableScheme'],
            },
        ],
        'uuid': fs['IdUuid'],
        'bytes': fs['DeviceSize'],
        'filesystem': fs['IdType'],
        'label': fs['IdLabel'],
        'partition': fs['PartitionNumber'],
    },
}

print json.dumps(doc, sort_keys=True, indent=4)
