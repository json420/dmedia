import weakref
import time
import json

from gi.repository import GObject, Gio

GObject.threads_init()
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


system = DBus(Gio.bus_get_sync(Gio.BusType.SYSTEM, None))


def get_device_props(obj):
    device = system.get('org.freedesktop.UDisks', obj, PROPS)
    #return device.Get('(ss)', 'org.freedesktop.UDisks.Device', 'DeviceIsPartition')
    return device.GetAll('(s)', 'org.freedesktop.UDisks.Device')


class Props:
    def __init__(self, obj):
        self.obj = obj
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            obj,
            'org.freedesktop.DBus.Properties'
        )
        self.ispartition = self.get('DeviceIsPartition')
        if self.ispartition:
            self.drive = self.get('PartitionSlave')

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.obj)

    def get(self, key):
        return self.proxy.Get('(ss)', 'org.freedesktop.UDisks.Device', key)

    def get_all(self):
        return self.proxy.GetAll('(s)', 'org.freedesktop.UDisks.Device')


class WeakMethod:
    def __init__(self, inst, method):
        self.proxy = weakref.proxy(inst)
        self.method = method

    def __call__(self, *args):
        return getattr(self.proxy, self.method)(*args)


def partition_info(partition):
    return {
        'uuid': partition['IdUuid'],
        'bytes': partition['DeviceSize'],
        'filesystem': partition['IdType'],
        'filesystem_version': partition['IdVersion'],
        'label': partition['IdLabel'],
        'number': partition['PartitionNumber'],
    }


def drive_info(drive):
    return {
        'serial': drive['DriveSerial'],
        'wwn': drive['DriveWwn'],
        'bytes': drive['DeviceSize'],
        'block_bytes': drive['DeviceBlockSize'],
        'vendor': drive['DriveVendor'],
        'model': drive['DriveModel'],
        'revision': drive['DriveRevision'],
        'partition_scheme': drive['PartitionTableScheme'],
        'internal': drive['DeviceIsSystemInternal'],
        'connection': drive['DriveConnectionInterface'],
        'connection_rate': drive['DriveConnectionSpeed'],

        # These seem consitently worthless, never correct:
        #'rotational': drive['DriveIsRotational'],
        #'rotation_rate': drive['DriveRotationRate'],
    }


class UDisks:
    def __init__(self):
        self.devices = {}
        self.props = {}
        self.drives = {}
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            '/org/freedesktop/UDisks'
        )

    def monitor(self):
        self.proxy.connect('g-signal', WeakMethod(self, 'on_g_signal'))
        for obj in self.proxy.EnumerateDevices():
            self.add_device(obj)
            self.change_device(obj)

    def on_g_signal(self, proxy, sender, signal, params):
        if signal == 'DeviceAdded':
            self.add_device(params.unpack()[0])
        elif signal == 'DeviceChanged':
            self.change_device(params.unpack()[0])
        elif signal == 'DeviceRemoved':
            self.remove_device(params.unpack()[0])

    def get_props(self, obj):
        if obj not in self.props:
            self.props[obj] = Props(obj)
        return self.props[obj]

    def add_device(self, obj):
        print('add', obj)
        self.get_props(obj)

    def change_device(self, obj):
        print('change', obj)
        props = self.props[obj]
        if not props.ispartition:
            return
        if props.get('DeviceIsMounted'):
            d = self.drives.get(props.drive, {})
            self.drives[props.drive] = d
            d[props.drive] = {}     
        else:
            try:
                del self.drives[props.drive][obj]
                del self.drives[props.drive]
            except KeyError:
                pass
        #print(json.dumps(self.drives, sort_keys=True, indent=4))

    def remove_device(self, obj):
        print('remove', obj)
        try:
            del self.props[obj]
        except KeyError:
            pass
 
start = time.time()
udisks = UDisks()
udisks.monitor()
print(time.time() - start)

mainloop = GObject.MainLoop()
mainloop.run()

