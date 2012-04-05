import weakref
import time
import json
import os
from os import path
from collections import namedtuple
from gettext import gettext as _

from gi.repository import GObject, Gio
from filestore import DOTNAME
from microfiber import Database, dmedia_env

from dmedia.units import bytes10

GObject.threads_init()
PROPS = 'org.freedesktop.DBus.Properties'


def major_minor(parentdir):
    st_dev = os.stat(parentdir).st_dev
    return (os.major(st_dev), os.minor(st_dev))


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


class Props:
    __slots__ = ('obj', 'proxy', 'cache')

    def __init__(self, obj):
        self.obj = obj
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            obj,
            'org.freedesktop.DBus.Properties'
        )
        self.cache = {}

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.obj)

    def __getitem__(self, key):
        try:
            return self.cache[key]
        except KeyError:
            value = self.proxy.Get('(ss)', 'org.freedesktop.UDisks.Device', key)
            self.cache[key] = value
            return value

    @property
    def ispartition(self):
        return self['DeviceIsPartition']

    def get_all(self):
        return self.proxy.GetAll('(s)', 'org.freedesktop.UDisks.Device')

    def reset(self):
        self.cache.clear()


class WeakMethod:
    def __init__(self, inst, method):
        self.proxy = weakref.proxy(inst)
        self.method = method

    def __call__(self, *args):
        return getattr(self.proxy, self.method)(*args)


def partition_info(props, mount, stores):
    return {
        'mount': mount,
        'stores': stores,
        'drive': props['PartitionSlave'],
    }

    return {
        'stores': {},
        'mounts': partition['DeviceMountPaths'],
        'uuid': partition['IdUuid'],
        'bytes': partition['DeviceSize'],
        'size': bytes10(partition['DeviceSize']),
        'filesystem': partition['IdType'],
        'filesystem_version': partition['IdVersion'],
        'label': partition['IdLabel'],
        'number': partition['PartitionNumber'],
    }


def drive_text(drive):
    if drive['DeviceIsSystemInternal']:
        template = _('{size} Drive')
    else:
        template = _('{size} Removable Drive')
    return template.format(size=bytes10(drive['DeviceSize']))



def drive_info(drive):
    return {
        'partitions': {},
        #'serial': drive['DriveSerial'],
        #'bytes': drive['DeviceSize'],
        #'size': bytes10(drive['DeviceSize']),
        #'vendor': drive['DriveVendor'],
        'model': drive['DriveModel'],
        #'revision': drive['DriveRevision'],
        #'partition_scheme': drive['PartitionTableScheme'],
        #'removable': not drive['DeviceIsSystemInternal'],
        #'connection': drive['DriveConnectionInterface'],
        #'connection_rate': drive['DriveConnectionSpeed'],
        'text': drive_text(drive),
    }


Store = namedtuple('Store', 'parentdir partition')


def usable_mount(mounts):
    for mount in mounts:
        if mount.startswith('/media/') or mount[:4] in ('/srv', '/mnt'):
            return mount


def get_filestore_id(parentdir):
    store = path.join(parentdir, '.dmedia', 'store.json') 
    try:
        return json.load(open(store, 'r'))['_id']
    except Exception:
        pass


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
        user = path.abspath(os.environ['HOME'])
        home = path.dirname(user)
        self.home = Store(home, self.find(home))
        try:
            self.user = Store(user, self.find(user))
        except Exception:
            self.user = Store(user, self.find(home))
        self.standard = (self.home.partition, self.user.partition)
        self.proxy.connect('g-signal', WeakMethod(self, 'on_g_signal'))
        for obj in self.proxy.EnumerateDevices():
            self.change_device(obj)

    def find(self, parentdir):
        (major, minor) = major_minor(parentdir)
        return self.proxy.FindDeviceByMajorMinor('(xx)', major, minor) 

    def on_g_signal(self, proxy, sender, signal, params):
        if signal == 'DeviceChanged':
            self.change_device(params.unpack()[0])
        elif signal == 'DeviceRemoved':
            self.remove_device(params.unpack()[0])

    def get_props(self, obj):
        if obj not in self.props:
            self.props[obj] = Props(obj)
        return self.props[obj]

    def get_drive(self, obj):
        try:
            return self.drives[obj]
        except KeyError:
            props = self.get_props(obj)
            info = drive_info(props)
            self.drives[obj] = info
            return info

    def change_device(self, obj):
        props = self.get_props(obj)
        props.reset()
        if not props.ispartition:
            return
        if props['DeviceIsMounted']:
            if obj in self.standard:
                mount = props['DeviceMountPaths'][0]
                stores = {}
                if obj == self.home.partition:
                    stores[self.home.parentdir] = get_filestore_id(self.home.parentdir)
                if obj == self.user.partition:
                    stores[self.user.parentdir] = get_filestore_id(self.user.parentdir)
            else:
                mount = usable_mount(props['DeviceMountPaths'])
                if mount is None:
                    return
                stores = {mount: get_filestore_id(mount)}
            part = partition_info(props, mount, stores)
            drive = self.get_drive(part['drive'])
            drive['partitions'][obj] = part

    def change_device_old(self, obj):
        return
        if props['DeviceIsMounted']:
            if obj not in self.standard:
                parentdir = usable_mount(props['DeviceMountPaths'])
                if not parentdir:
                    return
                stores = {
                    parentdir: {},
                }
            else:
                stores = {}
                if obj == self.home.partition:
                    stores[self.home.parentdir] = {}
                if obj == self.user.partition:
                    stores[self.user.parentdir] = {}
            if props.drive not in self.drives:
                self.drives[props.drive] = drive_info(
                    self.get_props(props.drive)
                )
            d = self.drives[props.drive]
            d['partitions'][obj] = partition_info(props)
            d['partitions'][obj]['stores'].update(stores)
            for key in stores:
                if path.isdir(path.join(key, DOTNAME)):
                    stores[key]['dmedia'] = True
        else:
            try:
                del self.drives[props.drive]['partitions'][obj]
                if not self.drives[props.drive]['partitions']:
                    del self.drives[props.drive]
            except KeyError:
                pass

    def remove_device(self, obj):
        print('remove', obj)
        try:
            del self.props[obj]
        except KeyError:
            pass
            
    def json(self):
        return json.dumps(self.drives, sort_keys=True, indent=4)

start = time.time()
udisks = UDisks()
udisks.monitor()

print(udisks.json())

#for dkey in sorted(udisks.drives):
#    print(dkey)
#    drive = udisks.drives[dkey]
#    for pkey in drive['partitions']:
#        print('    {}'.format(pkey))
#        partition = drive['partitions'][pkey]
        
       
print('')
print(time.time() - start)

#for p in ('/', '/tmp', '/home', '/home/jderose'):
#    print(p, major_minor(p))


#mainloop = GObject.MainLoop()
#mainloop.run()

