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

import sys
import json
import os
from os import path
from gettext import gettext as _
import logging
import time

import dbus
from gi.repository import GObject
from filestore import DOTNAME

from dmedia.units import bytes10
from dmedia.misc import WeakMethod


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
        if mount.startswith('/run/media/') or mount.startswith('/media/') or mount.startswith('/srv/'):
            return str(mount)


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
        self.obj = str(obj)
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


empty_options = dbus.Array(signature='s')

class Drive(Device):

    def DriveEject(self):
        log.info('Ejecting %r', self)
        return self.proxy.DriveEject(empty_options)

    def DriveDetach(self):
        """
        Worthless, don't use this!
        """
        log.info('Detaching %r', self)
        return self.proxy.DriveDetach(empty_options)


class Partition(Device):
    def __init__(self, obj):
        super().__init__(obj)
        assert self.ispartition

    def get_drive(self):
        return Drive(self.drive)

    def FilesystemUnmount(self):
        log.info('Unmounting %r', self)
        return self.proxy.FilesystemUnmount(empty_options)

    def FilesystemCreate(self, fstype=None, options=None):
        if fstype is None:
            fstype = str(self['IdType'])
        if options is None:
            options = ['label={}'.format(self['IdLabel'])]
        log.info('Formating %r as %r with %r', self, fstype, options)
        return self.proxy.FilesystemCreate(fstype, options)


class DeviceWorker:
    def __init__(self, obj, callback):
        self.obj = str(obj)
        self.callback = callback
        self.partition = Partition(obj)
        self.drive = self.partition.get_drive()
        callback = WeakMethod(self, 'on_JobChanged')
        self.partition.proxy.connect_to_signal('JobChanged', callback)
        self.drive.proxy.connect_to_signal('JobChanged', callback)
        self.next = None

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.obj)

    def on_JobChanged(self, *args):
        job_in_progress = args[0]
        if not (job_in_progress or self.next is None):
            next = self.next
            self.next = None
            # FIXME: This wait is to work around UDisks issues: when we get the
            # JobChanged signal, it seems that often UDisks isn't actually ready
            # for the next step
            GObject.timeout_add(500, self.on_timeout, next)

    def on_timeout(self, next):
        next()
        return False  # Do not repeat timeout call

    def run(self):
        # This should fix LP:1067142 and also prevents the UI from blocking
        # too long:
        GObject.idle_add(self.unmount)

    def _unmount(self):
        try:
            self.partition.FilesystemUnmount()
        except Exception:
            log.exception('Error unmounting %r', self)
            self.finish()

    def eject(self):
        self.next = self.finish
        try:
            self.drive.DriveEject()
        except Exception:
            log.exception('Error ejecting %r', self)
            self.finish()

    def finish(self):
        self.next = None
        GObject.idle_add(self.callback, self, self.obj)


class Ejector(DeviceWorker):
    def unmount(self):
        self.next = self.eject
        self._unmount()


class Formatter(DeviceWorker):
    def unmount(self):
        self.next = self.format
        self._unmount()

    def format(self):
        self.next = self.eject
        try:
            self.partition.FilesystemCreate()
        except Exception as e:
            log.exception('Error formatting %r', self)
            self.finish()


class UDisks(GObject.GObject):
    __gsignals__ = {
        'store_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # obj, mount, store_id, info
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'store_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # obj, mount, store_id
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'card_added': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # obj, mount, info
            [TYPE_PYOBJECT, TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'card_removed': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            # obj, mount
            [TYPE_PYOBJECT, TYPE_PYOBJECT]
        ),
        'init_done': (GObject.SIGNAL_RUN_LAST, GObject.TYPE_NONE,
            [TYPE_PYOBJECT]
        ),
    }

    def __init__(self):
        super().__init__()
        self.devices = {}
        self.drives = {}
        self.partitions = {}
        self.stores = {}
        self.cards = {}
        self.info = {
            'drives': self.drives,
            'partitions': self.partitions,
            'stores': self.stores,
            'cards': self.cards,
        }

    def monitor(self):
        start = time.time()
        log.info('Starting UDisks device enumeration...')
        self.proxy = system.get_object(
            'org.freedesktop.UDisks',
            '/org/freedesktop/UDisks'
        )
        self.proxy.connect_to_signal('DeviceChanged', self.on_DeviceChanged)
        self.proxy.connect_to_signal('DeviceRemoved', self.on_DeviceRemoved)
        try:
            devices = self.proxy.EnumerateDevices()
        except Exception as e:
            log.exception('Exception calling UDisks.EnumerateDevices()')
            devices = []  
        for obj in devices:
            try:
                self.change_device(obj)
            except Exception as e:
                log.exception('Exception calling change_device(%r)', obj)
        log.info('Finished UDisks device enumeration in %r', time.time() - start)

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
                part['store_id'] = store_id
                self.add_store(obj, mount, store_id, part, drive)
            elif drive['info']['removable']:
                part['card'] = True
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

    def add_store(self, obj, mount, store_id, part, drive):
        if obj in self.stores:
            return
        info = {
            'parentdir': mount,
            'id': store_id,
            'partition': part['info'],
            'drive': drive['info'],
        }
        self.stores[obj] = info
        #log.info('store_added %r %r %r', obj, mount, store_id)
        self.emit('store_added', obj, mount, store_id, info)

    def remove_store(self, obj):
        try:
            d = self.stores.pop(obj)
            #log.info('store_removed %r %r %r', obj, d['parentdir'], d['id'])
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

