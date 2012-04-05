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
TYPE_PYOBJECT = GObject.TYPE_PYOBJECT
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


class Device:
    __slots__ = ('obj', 'proxy', 'cache', 'ispartition', 'drive')

    def __init__(self, obj):
        self.obj = obj
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            obj,
            'org.freedesktop.DBus.Properties'
        )
        self.cache = {}
        self.ispartition = self['DeviceIsPartition']
        if self.ispartition:
            self.drive = self['PartitionSlave']
        else:
            self.drive = None

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
    def ismounted(self):
        return self['DeviceIsMounted']

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


def partition_info(d, mount=None):
    return {
        'drive': d.drive,
        'mount': mount,
        'info': {
            'label': d['IdLabel'],
            'uuid': d['IdUuid'],
            'bytes': d['DeviceSize'],
            'size': bytes10(d['DeviceSize']),
            'filesystem': d['IdType'],
            'filesystem_version': d['IdVersion'],
            'number': d['PartitionNumber'],
        },
    }


def drive_text(d):
    if d['DeviceIsSystemInternal']:
        template = _('{size} Drive')
    else:
        template = _('{size} Removable Drive')
    return template.format(size=bytes10(d['DeviceSize']))


def drive_info(d):
    return {
        'partitions': {},
        'info': {
            'serial': d['DriveSerial'],
            'bytes': d['DeviceSize'],
            'size': bytes10(d['DeviceSize']),
            'model': d['DriveModel'],
            'removable': not d['DeviceIsSystemInternal'],
            'connection': d['DriveConnectionInterface'],
            'text': drive_text(d),
        }
    }


Special = namedtuple('Store', 'parentdir partition')


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


class UDisks(GObject.GObject):
    __gsignals__ = {
        'card_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'card_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
    }

    def __init__(self):
        super().__init__()
        self.devices = {}
        self.drives = {}
        self.cards = {}
        self.stores = {}
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            '/org/freedesktop/UDisks'
        )

    def monitor(self):
        user = path.abspath(os.environ['HOME'])
        home = path.dirname(user)

        home_p = self.find(home)
        try:
            user_p = self.find(user)
        except Exception:
            user_p = home_p
        self.special = {
            home: home_p,
            user: user_p,
        }
        self.proxy.connect('g-signal', self.on_g_signal)
        for obj in self.proxy.EnumerateDevices():
            self.change_device(obj)

    def find(self, parentdir):
        """
        Return DBus object path of partition containing *parentdir*.
        """
        (major, minor) = major_minor(parentdir)
        return self.proxy.FindDeviceByMajorMinor('(xx)', major, minor)

    def on_g_signal(self, proxy, sender, signal, params):
        if signal == 'DeviceChanged':
            self.change_device(params.unpack()[0])
        elif signal == 'DeviceRemoved':
            self.remove_device(params.unpack()[0])

    def get_device(self, obj):
        if obj not in self.devices:
            self.devices[obj] = Device(obj)
        return self.devices[obj]

    def get_drive(self, obj):
        if obj not in self.drives:
            d = self.get_device(obj)
            self.drives[obj] = drive_info(d)
        return self.drives[obj]

    def change_device(self, obj):
        d = self.get_device(obj)
        if not d.ispartition:
            return
        d.reset()
        if d.ismounted:
            mount = usable_mount(d['DeviceMountPaths'])
            if mount is None:
                return
            part = partition_info(d, mount)
            drive = self.get_drive(part['drive'])
            drive['partitions'][obj] = part
            store_id = get_filestore_id(mount)
            if store_id:
                self.add_store(obj, mount, store_id)
            elif drive['info']['removable']:
                self.add_card(obj, mount)
        else:
            try:
                del self.drives[d.drive]['partitions'][obj]
                if not self.drives[d.drive]['partitions']:
                    del self.drives[d.drive]
            except KeyError:
                pass
            self.remove_store(obj)
            self.remove_card(obj)

    def add_card(self, obj, mount):
        if obj in self.cards:
            return
        self.cards[obj] = mount
        self.emit('card_added', obj, mount)

    def remove_card(self, obj):
        try:
            mount = self.cards.pop(obj)
            self.emit('card_removed', obj, mount)
        except KeyError:
            pass

    def add_store(self, obj, mount, store_id):
        if obj in self.stores:
            return
        self.stores[obj] = {'parentdir': mount, 'id': store_id}
        self.emit('store_added', obj, mount, store_id)

    def remove_store(self, obj):
        try:
            d = self.stores.pop(obj)
            self.emit('store_removed', obj, d['parentdir'], d['id'])
        except KeyError:
            pass

    def remove_device(self, obj):
        try:
            del self.devices[obj]
        except KeyError:
            pass

    def get_parentdir_info(self, parentdir):
        obj = self.find(parentdir)
        d = self.get_device(obj)
        return {
            'parentdir': parentdir,
            'partition': partition_info(d)['info'],
            'drive': self.get_drive(d.drive)['info'],
        }
  
    def json(self):
        d = {
            'drives': self.drives,
            'stores': self.stores,
            'cards': self.cards,
            'special': self.special,
        }
        return json.dumps(d, sort_keys=True, indent=4)
  
def on_signal(u, *args):
    print(args[-1], args[:-1])

start = time.time()
udisks = UDisks()
    
for signal in ('store_added', 'store_removed', 'card_added', 'card_removed'):
    udisks.connect(signal, on_signal, signal)

udisks.monitor()
print(udisks.json())

        
       
print('')
print(time.time() - start)

#for p in ('/', '/tmp', '/home', '/home/jderose'):
#    print(p, major_minor(p))





mainloop = GObject.MainLoop()
mainloop.run()

