# dmedia: distributed media library
# Copyright (C) 2011 Novacut Inc
#
# This file is part of `dmedia`.
#
# `dmedia` is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
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
Attempts to tame the UDisks beast.
"""

import json
import os
from os import path
from gettext import gettext as _

from gi.repository import GObject
from filestore import DOTNAME

from dmedia.units import bytes10
from dmedia.service.dbus import system


TYPE_PYOBJECT = GObject.TYPE_PYOBJECT


def major_minor(parentdir):
    st_dev = os.stat(parentdir).st_dev
    return (os.major(st_dev), os.minor(st_dev))


def usable_mount(mounts):
    for mount in mounts:
        if mount.startswith('/media/') or mount[:4] in ('/srv', '/mnt'):
            return mount

  
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


def get_filestore_id(parentdir):
    store = path.join(parentdir, DOTNAME, 'store.json') 
    try:
        return json.load(open(store, 'r'))['_id']
    except Exception:
        pass



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

