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

import os
from os import path
import logging
import weakref

from filestore import DOTNAME
from gi.repository import GObject, Gio
from gi.repository.GObject import TYPE_PYOBJECT


log = logging.getLogger()
PROPS = 'org.freedesktop.DBus.Properties'
ECRYPTFS = '/home/.ecryptfs/'


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


def read_ecryptfs_link(parentdir):
    private = path.join(parentdir, '.Private')
    if path.islink(private):
        link = os.readlink(private)
        if link.startswith(ECRYPTFS):
            return link


def extra_info(partition, drive):
    return {
        'partition': {
            'uuid': partition['IdUuid'],
            'bytes': partition['DeviceSize'],
            'filesystem': partition['IdType'],
            'filesystem_version': partition['IdVersion'],
            'label': partition['IdLabel'],
            'number': partition['PartitionNumber'],
        },
        'drive': {
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
        },
    }


class WeakRefCallback:
    def __init__(self, inst):
        self.proxy = weakref.proxy(inst)

    def __call__(self, *args):
        return self.proxy._on_g_signal(*args)


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
        'card_inserted': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'card_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
    }

    _autoemit = ('DeviceAdded', 'DeviceRemoved', 'DeviceChanged')

    def __init__(self):
        super().__init__()
        self.proxy = system.get(
            'org.freedesktop.UDisks',
            '/org/freedesktop/UDisks'
        )
        self.proxy.connect('g-signal', WeakRefCallback(self))
        self._monitoring = False
        
    def __del__(self):
        print('del')

    def _on_g_signal(self, proxy, sender, signal, params):
        if signal in self._autoemit:
            args = params.unpack()
            self.emit(signal, *args)
            if not self._monitoring:
                return
            if signal == 'DeviceChanged':
                self._on_DeviceChanged(*args)
            elif signal == 'DeviceRemoved':
                self._on_DeviceRemoved(*args)

    def _on_DeviceChanged(self, udisks, obj):
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
                self._stores[obj] = parentdir
                self.emit('store_added', obj, parentdir, partition, drive)
            elif not partition['DeviceIsSystemInternal']:
                self._cards[obj] = parentdir
                self.emit('card_inserted', obj, parentdir, partition, drive)

    def _on_DeviceRemoved(self, udisks, obj):
        try:
            parentdir = self._stores.pop(obj)
            self.emit('store_removed', obj, parentdir)
        except KeyError:
            try:
                parentdir = self._cards.pop(obj)
                self.emit('card_removed', obj, parentdir)
            except KeyError:
                pass  

    def monitor(self):
        self._cards = {}
        self._stores = {}
        self._monitoring = True

    def EnumerateDevices(self):
        return self.proxy.EnumerateDevices()

    def FindDeviceByMajorMinor(self, major, minor):
        return self.proxy.FindDeviceByMajorMinor('(xx)', major, minor)

    def get_parentdir_info(self, parentdir):
        info = {
            'parentdir': parentdir,
            'ecryptfs': False,
            'ecryptfs_link_target': None,
        }
        try:
            st_dev = os.stat(parentdir).st_dev
            major = os.major(st_dev)
            minor = os.minor(st_dev)
            obj = self.FindDeviceByMajorMinor(major, minor)
            partition = Partition(obj)
            drive = partition.get_drive()
            info.update(extra_info(partition, drive))
        except Exception:
            link = read_ecryptfs_link(parentdir)
            if link:
                info['ecryptfs'] = True
                info['ecryptfs_link_target'] = link
                try:
                    st_dev = os.stat(link).st_dev
                    major = os.major(st_dev)
                    minor = os.minor(st_dev)
                    obj = self.FindDeviceByMajorMinor(major, minor)
                    partition = Partition(obj)
                    drive = partition.get_drive()
                    info.update(extra_info(partition, drive))
                except Exception:
                    pass
        return info


def get_device_props(obj):
    device = system.get('org.freedesktop.UDisks', obj, PROPS)
    return device.GetAll('(s)', 'org.freedesktop.UDisks.Device')


class Device:
    bus = 'org.freedesktop.UDisks'
    iface = 'org.freedesktop.UDisks.Device'

    def __init__(self, obj):
        self.obj = obj
        self.proxy = system.get(self.bus, obj, self.iface)
        self.refresh()

    def refresh(self):
        self.__props = get_device_props(self.obj)

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.obj)

    def __getitem__(self, key):
        return self.__props[key]


class Partition(Device):
    def __init__(self, obj):
        super().__init__(obj)
        assert self['DeviceIsPartition']

    def get_drive(self):
        return Drive(self['PartitionSlave'])

    def FilesystemUnmount(self):
        log.info('Unmounting %r', self)
        return self.proxy.FilesystemUnmount('(as)', [])

    def FilesystemCreate(self, fstype=None, options=None):
        if fstype is None:
            fstype = self['IdType']
        if options is None:
            options = ['label={}'.format(self['IdLabel'])]
        log.info('Formating %r as %r with %r', self, fstype, options)
        return self.proxy.FilesystemCreate('(sas)', fstype, options)


class Drive(Device):

    def DriveEject(self):
        log.info('Ejecting %r', self)
        return self.proxy.DriveEject('(as)', [])

    def DriveDetach(self):
        """
        Worthless, don't use this!
        """
        log.info('Detaching %r', self)
        return self.proxy.DriveDetach('(as)', [])
        
        
        
class DeviceWorker:
    def __init__(self, obj, callback):
        self.obj = obj
        self.callback = callback
        self.partition = Partition(obj)
        self.drive = self.partition.get_drive()
        weak = WeakRefCallback(self)
        self.partition.proxy.connect('g-signal', weak)
        self.drive.proxy.connect('g-signal', weak)
        self.next = None

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.obj)

    def _on_g_signal(self, proxy, sender, signal, params):
        if signal == 'JobChanged':
            job_in_progress = params.unpack()[0]
            if not (job_in_progress or self.next is None):
                self.next()


class Ejector(DeviceWorker):

    def run(self):
        self.unmount()

    def unmount(self):
        self.next = self.eject
        self.partition.FilesystemUnmount()

    def wait1(self):
        # FIXME: This wait is to work around a UDisks bug: when we get the
        # signal that the format is complete, we can't actually eject yet
        self.next = None
        GObject.timeout_add(1000, self.eject)

    def eject(self):
        self.next = self.wait2
        self.drive.DriveEject()
        return False  # Do not repeat timeout call

    def wait2(self):
        # FIXME: This wait is a UX hack so the 'batch_finished' notification is
        # shown just a touch after the cards disappear from the Launcher.
        # Ideally, we don't want these cards in the Launcher in the first place
        # during a dmedia import as they aren't actionable in the expected way.
        # Plus, we want to mount the cards read-only... which will probably take
        # some changes in Nautilus.
        self.next = None
        GObject.timeout_add(1000, self.finish)

    def finish(self):
        self.callback(self, self.obj)
        return False  # Do not repeat timeout call


class Formatter(DeviceWorker):

    def run(self):
        self.unmount()

    def unmount(self):
        self.next = self.format
        self.partition.FilesystemUnmount()

    def format(self):
        self.next = self.wait1
        self.partition.FilesystemCreate()

    def wait1(self):
        # FIXME: This wait is to work around a UDisks bug: when we get the
        # signal that the format is complete, we can't actually eject yet
        self.next = None
        GObject.timeout_add(1000, self.eject)

    def eject(self):
        self.next = self.wait2
        self.drive.DriveEject()
        return False  # Do not repeat timeout call

    def wait2(self):
        # FIXME: This wait is a UX hack so the 'batch_finished' notification is
        # shown just a touch after the cards disappear from the Launcher.
        # Ideally, we don't want these cards in the Launcher in the first place
        # during a dmedia import as the aren't actionable in the expected way.
        # Plus, we want to mount the cards read-only... which will probably take
        # some changes in Nautilus.
        self.next = None
        GObject.timeout_add(1000, self.finish)

    def finish(self):
        self.callback(self, self.obj)
        return False  # Do not repeat timeout call
