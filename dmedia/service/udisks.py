# dmedia: distributed media library
# Copyright (C) 2012 Novacut Inc
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
import logging

import dbus
from gi.repository import GObject
from filestore import DOTNAME

from dmedia.units import bytes10


log = logging.getLogger()
TYPE_PYOBJECT = GObject.TYPE_PYOBJECT
system = dbus.SystemBus()


def major_minor(parentdir):
    st_dev = os.stat(parentdir).st_dev
    return (os.major(st_dev), os.minor(st_dev))


def usable_mount(mounts):
    """
    Return mount point at which Dmedia will look for file-stores.

    For example:
    
    >>> usable_mount(['/', '/tmp']) is None
    True
    >>> usable_mount(['/', '/tmp', '/media/foo'])
    '/media/foo'

    """
    for mount in mounts:
        if mount.startswith('/media/') or mount.startswith('/srv/'):
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
        'partitions': [],
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
        self.proxy = system.get_object('org.freedesktop.UDisks', obj)
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
            value = self.proxy.Get('org.freedesktop.UDisks.Device', key,
                dbus_interface='org.freedesktop.DBus.Properties'
            )
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
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
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
        self.partitions = {}
        self.cards = {}
        self.stores = {}
        self.proxy = system.get_object(
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
        self.proxy.connect_to_signal('DeviceChanged', self.on_DeviceChanged)
        self.proxy.connect_to_signal('DeviceRemoved', self.on_DeviceRemoved)
        for obj in self.proxy.EnumerateDevices():
            self.change_device(obj)

    def on_DeviceChanged(self, obj):
        self.change_device(obj)

    def on_DeviceRemoved(self, obj):
        self.remove_device(obj)

    def find(self, parentdir):
        """
        Return DBus object path of partition containing *parentdir*.
        """
        (major, minor) = major_minor(parentdir)
        return self.proxy.FindDeviceByMajorMinor(major, minor,
            dbus_interface='org.freedesktop.UDisks'
        )

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
            self.partitions[obj] = part
            drive = self.get_drive(d.drive)
            partitions = set(drive['partitions'])
            partitions.add(obj)
            drive['partitions'] = sorted(partitions)
            store_id = get_filestore_id(mount)
            if store_id:
                self.add_store(obj, mount, store_id)
            elif drive['info']['removable']:
                self.add_card(obj, mount, part, drive)
        else:
            try:
                partitions = set(self.drives[d.drive]['partitions'])
                partitions.remove(obj)
                if len(partitions) == 0:
                    del self.drives[d.drive]
                else:
                    self.drives[d.drive]['partitions'] = sorted(partitions)
            except KeyError:
                pass
            try:
                del self.partitions[obj]
            except KeyError:
                pass
            self.remove_store(obj)
            self.remove_card(obj)

    def add_card(self, obj, mount, part, drive):
        if obj in self.cards:
            return
        info = {
            'mount': mount,
            'partition': part['info'],
            'drive': drive['info'],
        }
        self.cards[obj] = info
        log.info('card_added %r %r', obj, mount)
        self.emit('card_added', obj, mount, info)

    def remove_card(self, obj):
        try:
            d = self.cards.pop(obj)
            log.info('card_removed %r %r', obj, d['mount'])
            self.emit('card_removed', obj, d['mount'])
        except KeyError:
            pass

    def add_store(self, obj, mount, store_id):
        if obj in self.stores:
            return
        self.stores[obj] = {'parentdir': mount, 'id': store_id}
        log.info('store_added %r %r %r', obj, mount, store_id)
        self.emit('store_added', obj, mount, store_id)

    def remove_store(self, obj):
        try:
            d = self.stores.pop(obj)
            log.info('store_removed %r %r %r', obj, d['parentdir'], d['id'])
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

    def get_info(self):
        d = {
            'drives': self.drives,
            'partitions': self.partitions,
            'stores': self.stores,
            'cards': self.cards,
            #'special': self.special,
        }
        return d

